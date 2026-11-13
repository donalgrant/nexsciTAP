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


@pytest.fixture(scope="module")
def fork_server(fixture_root: Path):
    port = _free_port()
    workdir = fixture_root / "fork-workdir"
    (workdir / "TAP").mkdir(parents=True)

    conf = fixture_root / "TAP-fork.conf"
    conf.write_text((FIXTURES / "TAP.conf.template").read_text().format(
        TEST_WORKDIR=str(workdir),
        TEST_HTTP_HOST="127.0.0.1",
        TEST_HTTP_PORT=str(port),
        TEST_DB_PATH=str(fixture_root / "test_data.db"),
        TEST_TAP_SCHEMA=str(fixture_root / "tap_schema.db"),
    ))

    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [str(TESTS_DIR.parent), str(STUBS)] + ([env["PYTHONPATH"]] if env.get("PYTHONPATH") else []))

    proc = subprocess.Popen(
        [sys.executable, "-m", "TAP.forkserver", "--conf", str(conf), "--port", str(port)],
        env=env, cwd=str(fixture_root))

    base = f"http://127.0.0.1:{port}"
    try:
        for _ in range(100):
            try:
                requests.get(base + "/nothing", timeout=1)
                break
            except requests.ConnectionError:
                assert proc.poll() is None, "fork server exited during start-up"
                time.sleep(0.1)
        yield base, workdir
    finally:
        proc.terminate()
        proc.wait(timeout=10)


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
