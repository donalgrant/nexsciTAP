"""The pre-loaded forking server (TAP.forkserver) against the SQLite fixtures.

It must give the same answers as the CGI fixture, since each request runs
the same entry point; these tests cover what is specific to the server:
handing the connection to the request, async jobs outliving their request,
and the files it serves from the work directory.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
import requests
from _helpers import row_count_from_ipac
from conftest import FIXTURES, STUBS, TESTS_DIR, _free_port


def _serve(fixture_root: Path, name: str, web_extra: str = "", workdir: Path | None = None):
    """Start a fork server with its own configuration and (unless given one) work directory."""
    port = _free_port()
    workdir = workdir or fixture_root / f"{name}-workdir"
    (workdir / "TAP").mkdir(parents=True, exist_ok=True)

    text = (FIXTURES / "TAP.conf.template").read_text().format(
        TEST_WORKDIR=str(workdir),
        TEST_HTTP_HOST="127.0.0.1",
        TEST_HTTP_PORT=str(port),
        TEST_DB_PATH=str(fixture_root / "test_data.db"),
        TEST_TAP_SCHEMA=str(fixture_root / "tap_schema.db"),
    )
    if web_extra:
        assert "[WEB]" in text
        text = text.replace("[WEB]", "[WEB]\n" + web_extra, 1)

    conf = fixture_root / f"TAP-{name}.conf"
    conf.write_text(text)

    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [str(TESTS_DIR.parent), str(STUBS)] + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else []))

    proc = subprocess.Popen(
        [sys.executable, "-m", "TAP.forkserver", "--conf", str(conf), "--port", str(port)],
        env=env, cwd=str(fixture_root))

    base = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            requests.get(base + "/nothing", timeout=1)
            break
        except requests.ConnectionError:
            assert proc.poll() is None, "fork server exited during start-up"
            time.sleep(0.1)
    return proc, base, workdir


@pytest.fixture(scope="module")
def fork_server(fixture_root: Path):
    proc, base, workdir = _serve(fixture_root, "fork")
    try:
        yield base, workdir
    finally:
        proc.terminate()
        proc.wait(timeout=10)


@pytest.fixture(scope="module")
def limited_server(fixture_root: Path):
    proc, base, _ = _serve(fixture_root, "limited", "MAXREC_LIMIT=4")
    try:
        yield base
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def test_maxrec_limit_caps_every_query(limited_server):
    """MAXREC_LIMIT applies with no MAXREC, a larger one, and not a smaller one."""
    url = limited_server + "/TAP/sync"
    query = "select l1filename from data_l1"   # 10 rows

    for extra, rows in (({}, 4), ({"maxrec": "1000"}, 4), ({"maxrec": "2"}, 2)):
        r = requests.post(url, data={"query": query, "format": "ipac", **extra})
        assert r.status_code == 200, r.text[:500]
        assert row_count_from_ipac(r.text) == rows, extra

    vot = requests.post(url, data={"query": query, "format": "votable"})
    assert vot.text.count("<TR>") == 4
    assert 'value="OVERFLOW"' in vot.text


def test_sync_query(fork_server):
    base, _ = fork_server
    for _ in range(3):
        r = requests.post(base + "/TAP/sync",
                          data={"query": "select l0filename, program from data_l0", "format": "ipac"})
        assert r.status_code == 200, r.text[:500]
        assert row_count_from_ipac(r.text) == 6


def test_requests_do_not_share_state(fork_server):
    """A failed request leaves nothing behind for the next one."""
    base, _ = fork_server
    bad = requests.post(base + "/TAP/sync", data={"query": "select nothing from nowhere", "format": "ipac"})
    assert bad.status_code != 200

    good = requests.post(base + "/TAP/sync", data={"query": "select l1filename from data_l1", "format": "ipac"})
    assert good.status_code == 200, good.text[:500]
    assert row_count_from_ipac(good.text) == 10


def test_async_job_outlives_its_request(fork_server):
    base, _ = fork_server
    r = requests.post(base + "/TAP/async",
                      data={"query": "select l0filename from data_l0", "format": "ipac", "phase": "RUN"},
                      allow_redirects=False)
    assert r.status_code == 303, r.text[:500]
    job = r.headers["Location"]

    phase = ""
    for _ in range(100):
        phase = requests.get(job + "/phase").text.strip()
        if phase in ("COMPLETED", "ERROR", "ABORTED"):
            break
        time.sleep(0.1)
    assert phase == "COMPLETED"

    result = requests.get(job + "/results/result")
    assert result.status_code == 200, result.text[:500]
    assert row_count_from_ipac(result.text) == 6


def test_large_response_to_slow_reader_is_complete(fork_server):
    """The whole response arrives even when the client is slow to read it.

    The service writes to the connection with blocking writes; a socket left
    in non-blocking mode cuts the response off once its buffer fills.
    """
    import socket
    from urllib.parse import urlencode, urlsplit

    base, _ = fork_server
    query = ("select a.l0filename, b.l0filename, c.l0filename, d.l0filename, e.l0filename "
             "from data_l0 a, data_l0 b, data_l0 c, data_l0 d, data_l0 e")
    body = urlencode({"query": query, "format": "csv"}).encode()
    host, port = urlsplit(base).hostname, urlsplit(base).port

    with socket.create_connection((host, port)) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        s.sendall(b"POST /TAP/sync HTTP/1.0\r\nContent-Type: application/x-www-form-urlencoded\r\n"
                  b"Content-Length: %d\r\n\r\n" % len(body) + body)
        time.sleep(1.5)
        data = b""
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            data += chunk

    head, _, text = data.partition(b"\r\n\r\n")
    assert head.startswith(b"HTTP/1.1 200"), head[:200]
    assert len(text) > 500_000
    assert len(text.strip().splitlines()) == 6 ** 5 + 1


def test_vosi_capabilities(fork_server):
    base, _ = fork_server
    r = requests.get(base + "/TAP/capabilities")
    assert r.status_code == 200
    assert "<vosi:capabilities" in r.text


def test_work_files_served_but_uploads_private(fork_server):
    base, workdir = fork_server
    job = workdir / "TAP" / "tap_static"
    job.mkdir()
    (job / "result.csv").write_text("a,b\n1,2\n")
    (job / "upload_mine.vot").write_text("<VOTABLE/>")

    ok = requests.get(base + "/workspace/TAP/tap_static/result.csv")
    assert ok.status_code == 200 and ok.text == "a,b\n1,2\n"
    assert requests.head(base + "/workspace/TAP/tap_static/result.csv").headers["Content-Length"] == "8"

    assert requests.get(base + "/workspace/TAP/tap_static/upload_mine.vot").status_code == 404
    assert requests.get(base + "/workspace/../TAP.conf").status_code == 404
    assert requests.get(base + "/elsewhere").status_code == 404


# ---- two servers sharing one work area --------------------------------

@pytest.fixture(scope="module")
def second_server(fixture_root: Path, fork_server):
    """Another server on the first one's work directory, as with a shared file system."""
    _, workdir = fork_server
    proc, base, _ = _serve(fixture_root, "second", workdir=workdir)
    try:
        yield base
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def _elsewhere(job_url: str, base: str) -> str:
    return base + job_url[job_url.index("/TAP/"):]


def test_job_submitted_to_one_server_is_served_by_the_other(fork_server, second_server):
    base, _ = fork_server
    r = requests.post(base + "/TAP/async",
                      data={"query": "select l0filename from data_l0", "format": "ipac", "phase": "RUN"},
                      allow_redirects=False)
    assert r.status_code == 303, r.text[:500]
    job = _elsewhere(r.headers["Location"], second_server)

    phase = ""
    for _ in range(100):
        phase = requests.get(job + "/phase").text.strip()
        if phase in ("COMPLETED", "ERROR", "ABORTED"):
            break
        time.sleep(0.1)
    assert phase == "COMPLETED"

    result = requests.get(job + "/results/result")
    assert result.status_code == 200, result.text[:500]
    assert row_count_from_ipac(result.text) == 6


def test_job_whose_server_is_gone_is_reported_as_failed(fork_server, second_server):
    """A job left EXECUTING by a server that stopped must not look alive for ever."""
    base, workdir = fork_server
    r = requests.post(base + "/TAP/async",
                      data={"query": "select l0filename from data_l0", "format": "ipac", "phase": "RUN"},
                      allow_redirects=False)
    job = r.headers["Location"]
    for _ in range(100):
        if requests.get(job + "/phase").text.strip() == "COMPLETED":
            break
        time.sleep(0.1)

    # Put the job back as its server would have left it on dying mid-query.
    jobdir = workdir / "TAP" / job.rsplit("/", 1)[1]
    status = jobdir / "status.xml"
    status.write_text(status.read_text().replace("<uws:phase>COMPLETED</uws:phase>",
                                                 "<uws:phase>EXECUTING</uws:phase>"))
    (jobdir / "worker").write_text(f"a-host-that-is-gone 4242 {time.time() - 3600:.0f}\n")

    other = _elsewhere(job, second_server)
    assert requests.get(other + "/phase").text.strip() == "ERROR"
    assert "stopped before the job finished" in requests.get(other).text
