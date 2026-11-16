"""TAP.jobworker: async jobs when several servers share the work area."""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

from TAP import jobworker

STATUS = """<?xml version="1.0" encoding="UTF-8"?>
<uws:job xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0">
    <uws:jobId>tap_x</uws:jobId>
    <uws:runId>4242</uws:runId>
    <uws:phase>{phase}</uws:phase>
</uws:job>
"""

WORKER = """
import sys, time
from TAP import jobworker
jobworker.start(sys.argv[1], beat=0.1)
time.sleep(30)
"""


def _worker(jobdir: Path) -> subprocess.Popen:
    (jobdir / "status.xml").write_text(STATUS.format(phase="EXECUTING"))
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).parent.parent))
    proc = subprocess.Popen([sys.executable, "-c", WORKER, str(jobdir)], env=env)
    for _ in range(100):
        if (jobdir / "worker").exists():
            break
        time.sleep(0.05)
    return proc


def test_worker_records_itself_and_shows_it_is_alive(tmp_path):
    proc = _worker(tmp_path)
    try:
        host, pid, _ = jobworker.owner(tmp_path)
        assert (host, pid) == (jobworker._host(), proc.pid)

        first = (tmp_path / "worker").read_text()
        time.sleep(1.5)
        assert (tmp_path / "worker").read_text() != first   # written again
        assert jobworker.owner(tmp_path)[2] < 5
    finally:
        proc.kill()
        proc.wait()


def test_stop_on_the_owning_host_kills_the_worker(tmp_path):
    proc = _worker(tmp_path)
    jobworker.stop(tmp_path, 0)                        # the pid comes from the worker file
    assert proc.wait(timeout=5) != 0
    assert not (tmp_path / "abort").exists()


def test_stop_from_another_host_does_not_kill_by_number(tmp_path, monkeypatch):
    """The process number means nothing on another host; the worker is asked to stop instead."""
    proc = _worker(tmp_path)
    try:
        monkeypatch.setattr(jobworker, "_host", lambda: "some-other-host")
        bystander = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
        try:
            jobworker.stop(tmp_path, bystander.pid)    # the number recorded in the status document
            assert (tmp_path / "abort").exists()
            assert proc.wait(timeout=5) == 1           # the worker saw the request and left
            assert bystander.poll() is None            # and nothing else was touched
        finally:
            bystander.kill()
            bystander.wait()
    finally:
        proc.kill()
        proc.wait()


def test_worker_exits_when_its_job_is_deleted(tmp_path):
    jobdir = tmp_path / "job"
    jobdir.mkdir()
    proc = _worker(jobdir)
    import shutil
    shutil.rmtree(jobdir)
    assert proc.wait(timeout=5) == 1


def test_worker_exits_when_its_status_says_aborted(tmp_path):
    """The signal that works across machines: the status document itself."""
    proc = _worker(tmp_path)
    (tmp_path / "status.xml").write_text(STATUS.format(phase="ABORTED"))
    assert proc.wait(timeout=5) == 1


def test_job_without_a_worker_file_is_stopped_by_number(tmp_path):
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    jobworker.stop(tmp_path, proc.pid)
    assert proc.wait(timeout=5) != 0


def test_mark_lost(tmp_path):
    status = tmp_path / "status.xml"
    status.write_text(STATUS.format(phase="EXECUTING"))

    # No worker file: nothing known, nothing changed.
    assert not jobworker.mark_lost(tmp_path, status)

    (tmp_path / "worker").write_text(f"gone-host 4242 {time.time():.0f}\n")
    assert not jobworker.mark_lost(tmp_path, status)   # heard from just now
    assert "EXECUTING" in status.read_text()

    # The file's own modification time is not what counts (it is cached on
    # network file systems); the time written in it is.
    long_ago = time.time() - 3600
    os.utime(tmp_path / "worker", (long_ago, long_ago))
    assert not jobworker.mark_lost(tmp_path, status)

    (tmp_path / "worker").write_text(f"gone-host 4242 {long_ago:.0f}\n")
    assert jobworker.mark_lost(tmp_path, status)
    text = status.read_text()
    assert "<uws:phase>ERROR</uws:phase>" in text
    assert "<uws:errorSummary" in text and text.rstrip().endswith("</uws:job>")

    # A finished job is left alone however old its worker file is.
    status.write_text(STATUS.format(phase="COMPLETED"))
    assert not jobworker.mark_lost(tmp_path, status)
    assert "COMPLETED" in status.read_text()
