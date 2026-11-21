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


# ---- one client cannot take every place ---------------------------------

@pytest.fixture(scope="module")
def fair_server(fixture_root: Path):
    proc, base, _ = _serve(fixture_root, "fair", "CLIENT_LIMIT=2\nCLIENT_HEADER=X-Client\nUNLIMITED_CLIENTS=portal\nSHARED_CLIENTS=firefly\nSHARED_CLIENT_LIMIT=3")
    try:
        yield base
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def _stalled_request(base: str, client: str):
    """A request that has arrived but not finished: its body never comes."""
    import socket
    host, port = base.replace("http://", "").split(":")
    s = socket.create_connection((host, int(port)))
    s.sendall(f"POST /TAP/sync HTTP/1.1\r\nHost: {host}\r\nX-Client: {client}\r\n"
              "Content-Type: application/x-www-form-urlencoded\r\nContent-Length: 50\r\n\r\n".encode())
    return s


def test_client_limit(fair_server):
    base = fair_server
    ask = {"query": "select l0filename from data_l0", "format": "ipac"}
    held = [_stalled_request(base, "alice"), _stalled_request(base, "alice")]
    try:
        time.sleep(0.5)

        r = requests.post(base + "/TAP/sync", data=ask, headers={"X-Client": "alice"})
        assert r.status_code == 429 and r.headers["Retry-After"]
        assert "alice" in r.text

        # Someone else is not affected, and neither is an exempt client.
        assert requests.post(base + "/TAP/sync", data=ask, headers={"X-Client": "bob"}).status_code == 200
        more = [_stalled_request(base, "portal") for _ in range(3)]
        time.sleep(0.3)
        assert requests.post(base + "/TAP/sync", data=ask, headers={"X-Client": "portal"}).status_code == 200
        for s in more:
            s.close()

        # A shared client (a portal's address, many users) has its own limit:
        # more than one client's two, and not unlimited.
        more = [_stalled_request(base, "firefly") for _ in range(3)]
        time.sleep(0.3)
        r = requests.post(base + "/TAP/sync", data=ask, headers={"X-Client": "firefly"})
        assert r.status_code == 429 and "portal" in r.text and "3 at once" in r.text
        for s in more:
            s.close()
    finally:
        for s in held:
            s.close()

    # The places come back when the requests end.
    for _ in range(50):
        r = requests.post(base + "/TAP/sync", data=ask, headers={"X-Client": "alice"})
        if r.status_code == 200:
            break
        time.sleep(0.1)
    assert r.status_code == 200


# ---- one client cannot download without end ------------------------------

@pytest.fixture(scope="module")
def capped_server(fixture_root: Path):
    proc, base, workdir = _serve(fixture_root, "capped", "CLIENT_HEADER=X-Client\nUNLIMITED_CLIENTS=portal\n"
                                 "SHARED_CLIENTS=firefly\nVOLUME_LIMIT=12000\nSHARED_VOLUME_LIMIT=30KB")
    try:
        yield base, workdir
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def test_volume_limit(capped_server):
    """A client that has been sent its day's volume is refused until the day has passed."""
    base, workdir = capped_server
    (workdir / "TAP" / "big.txt").write_text("x" * 5000)
    get = lambda who: requests.get(base + "/workspace/TAP/big.txt", headers={"X-Client": who})

    # 5,000 bytes a time against 12,000: the third takes carol over, the fourth is refused.
    assert [get("carol").status_code for _ in range(4)] == [200, 200, 200, 429]
    r = get("carol")
    assert r.status_code == 429 and int(r.headers["Retry-After"]) > 3600
    assert "carol" in r.text and "12 KB in a day" in r.text and "15 KB" in r.text

    # Refused for queries too, not only for files; a HEAD sends nothing and is let by.
    ask = {"query": "select l0filename from data_l0", "format": "ipac"}
    assert requests.post(base + "/TAP/sync", data=ask, headers={"X-Client": "carol"}).status_code == 429
    assert requests.head(base + "/workspace/TAP/big.txt", headers={"X-Client": "carol"}).status_code == 200

    # Someone else is not affected; a portal has its own, larger share; an exempt client has none.
    assert requests.post(base + "/TAP/sync", data=ask, headers={"X-Client": "bob"}).status_code == 200
    assert [get("firefly").status_code for _ in range(7)] == [200] * 6 + [429]
    assert "this portal" in get("firefly").text
    assert [get("portal").status_code for _ in range(8)] == [200] * 8
    assert not (workdir / "TAP" / ".volume" / "portal").exists()

    # The count is not served.
    hour = next((workdir / "TAP" / ".volume" / "carol").iterdir()).name
    assert requests.get(base + f"/workspace/TAP/.volume/carol/{hour}").status_code == 404


def test_what_a_query_sent_is_counted(capped_server):
    """An answer from the service itself counts, as a file does."""
    base, workdir = capped_server
    sizes = {}
    for fmt in ("votable", "ipac", "csv"):
        r = requests.post(base + "/TAP/sync", data={"query": "select * from data_l1", "format": fmt}, headers={"X-Client": "erin"})
        assert r.status_code == 200
        sizes[fmt] = len(r.content)
        if len(r.content) >= 4096:
            break
    print(sizes)
    assert len(r.content) >= 4096, sizes
    counted = sum(int(line) for f in (workdir / "TAP" / ".volume" / "erin").iterdir() for line in f.read_text().split())
    assert abs(counted - len(r.content)) < 1000        # the headers are counted with it


def test_register_of_running_jobs_is_not_served(fork_server):
    base, workdir = fork_server
    hidden = workdir / "TAP" / ".running"
    hidden.mkdir(exist_ok=True)
    (hidden / "tap_x").write_text("10.1.2.3\n")
    assert requests.get(base + "/workspace/TAP/.running/tap_x").status_code == 404


# ---- text longer than the result writer's old fixed buffers ---------------

LONG_NAME = "a_column_name_" + "x" * 300          # names were copied into 40 bytes
LONG_VALUE = "v" * 5000                           # string values into 1024


@pytest.mark.parametrize("fmt", ["csv", "ipac", "votable", "json", "tsv"])
def test_long_column_names_and_values_do_not_corrupt_memory(fork_server, fmt):
    """
    A result column is named by its expression unless the query gives an
    alias, so the client chooses its length; the writer wrote past its
    buffers and the process died (seen as 502, or a lost async job).
    """
    base, _ = fork_server
    r = requests.post(base + "/TAP/sync", data={
        "query": f"select l0filename as {LONG_NAME}, '{LONG_VALUE}' as longtext from data_l0",
        "format": fmt})
    if fmt == "ipac":
        # Fixed-width output has no room for the value and says so; with
        # the name alone it is written.
        assert r.status_code == 400 and "overflows column width" in r.text
        r = requests.post(base + "/TAP/sync", data={
            "query": f"select l0filename as {LONG_NAME} from data_l0", "format": fmt})
        assert r.status_code == 200 and LONG_NAME in r.text
        return

    assert r.status_code == 200, r.text[:300]
    assert LONG_NAME in r.text
    assert r.text.count(LONG_VALUE) == 6


def test_sigterm_stops_the_server_promptly(fixture_root: Path):
    """A container's first process must act on SIGTERM itself."""
    proc, _, _ = _serve(fixture_root, "term")
    proc.terminate()
    assert proc.wait(timeout=5) == 0


def test_csv_doubles_quotes_inside_a_value(fork_server):
    """A value holding a quote must come back as that value from any CSV reader."""
    import csv
    import io
    base, _ = fork_server
    r = requests.post(base + "/TAP/sync", data={
        "query": "select 'J 20mag/sq.\" isophotal, with a comma' as descr, l0filename from data_l0", "format": "csv"})
    assert r.status_code == 200, r.text[:300]
    rows = list(csv.DictReader(io.StringIO(r.text)))
    assert len(rows) == 6
    assert all(row["descr"] == 'J 20mag/sq." isophotal, with a comma' for row in rows)
    assert all(row["l0filename"] for row in rows)


def _records(log: Path, word: str):
    import json
    return [json.loads(line.split(" ", 1)[1]) for line in log.read_text().splitlines()
            if line.startswith(word + " ")]


def test_every_request_leaves_a_record(fixture_root: Path):
    """What an administrator counts queries from: one TAPQ line a request, one TAPV line at start."""
    log = fixture_root / "record-server.log"
    port = _free_port()
    workdir = fixture_root / "record-workdir"
    (workdir / "TAP").mkdir(parents=True, exist_ok=True)
    conf = fixture_root / "TAP-record.conf"
    conf.write_text((FIXTURES / "TAP.conf.template").read_text().format(
        TEST_WORKDIR=str(workdir), TEST_HTTP_HOST="127.0.0.1", TEST_HTTP_PORT=str(port),
        TEST_DB_PATH=str(fixture_root / "test_data.db"), TEST_TAP_SCHEMA=str(fixture_root / "tap_schema.db")))

    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [str(TESTS_DIR.parent), str(STUBS)] + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else []))

    with open(log, "w") as err:
        proc = subprocess.Popen(
            [sys.executable, "-m", "TAP.forkserver", "--conf", str(conf), "--port", str(port)],
            env=env, cwd=str(fixture_root), stderr=err)
    base = f"http://127.0.0.1:{port}"

    try:
        for _ in range(100):
            try:
                requests.get(base + "/nothing", timeout=1)
                break
            except requests.ConnectionError:
                assert proc.poll() is None
                time.sleep(0.1)

        ok = requests.post(base + "/TAP/sync", data={"QUERY": "select l0filename\n from   data_l0", "format": "csv"})
        assert ok.status_code == 200
        bad = requests.post(base + "/TAP/sync", data={"query": "select nothing from nowhere", "format": "csv"})
        assert bad.status_code != 200

        r = requests.post(base + "/TAP/async",
                          data={"query": "select l1filename from data_l1", "format": "ipac", "phase": "RUN"},
                          allow_redirects=False)
        job = r.headers["Location"]
        for _ in range(100):
            if requests.get(job + "/phase").text.strip() in ("COMPLETED", "ERROR", "ABORTED"):
                break
            time.sleep(0.1)
        time.sleep(0.5)
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    # Looking at a job must not fill the log with a parser's advice.
    assert "XMLParsedAsHTMLWarning" not in log.read_text()

    versions = _records(log, "TAPV")
    assert len(versions) == 1 and versions[0]["python"]

    recs = _records(log, "TAPQ")
    sync = [r for r in recs if r["path"] == "/sync"]
    assert len(sync) == 2

    good, failed = sync
    assert good["phase"] == "COMPLETED" and good["rows"] == 6 and good["fmt"] == "csv"
    assert good["q"] == "select l0filename from data_l0"          # white space folded to one line
    assert good["client"] == "127.0.0.1" and good["ms"] >= 0 and good["job"] is False
    assert failed["phase"] == "ERROR" and failed["err"] and "rows" not in failed

    done = [r for r in recs if r["job"]]
    assert len(done) == 1 and done[0]["phase"] == "COMPLETED" and done[0]["rows"] == 10
    assert done[0]["id"] == job.rsplit("/", 1)[1]
    assert all(r["id"] == done[0]["id"] for r in recs if "/async/" in r["path"])

    # Looking at a job is a request too, but not a query.
    looks = [r for r in recs if r["path"].endswith("/phase")]
    assert looks and all("q" not in r and "phase" not in r for r in looks)
