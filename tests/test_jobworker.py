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


# ---- how many jobs at once -----------------------------------------------

def _running(tapdir: Path, jobid: str, client: str, age: float = 0):
    """A job as its worker leaves it while running."""
    (tapdir / jobid).mkdir()
    (tapdir / jobid / "worker").write_text(f"some-host 1 {time.time() - age:.0f}\n")
    (tapdir / ".running").mkdir(exist_ok=True)
    (tapdir / ".running" / jobid).write_text(client + "\n")


def test_admit_holds_each_client_and_the_service_to_their_limits(tmp_path, monkeypatch):
    monkeypatch.setattr(jobworker, "_registered", None)
    _running(tmp_path, "tap_a1", "alice")
    _running(tmp_path, "tap_a2", "alice")
    _running(tmp_path, "tap_b1", "bob")

    said = jobworker.admit(str(tmp_path), "tap_new", "alice", total=10, per_client=2)
    assert said and "2 jobs running" in said
    assert not (tmp_path / ".running" / "tap_new").exists()

    # Another client still gets in, and is then counted.
    assert jobworker.admit(str(tmp_path), "tap_c1", "carol", total=10, per_client=2) is None
    assert (tmp_path / ".running" / "tap_c1").read_text().strip() == "carol"
    (tmp_path / "tap_c1").mkdir()
    (tmp_path / "tap_c1" / "worker").write_text(f"some-host 2 {time.time():.0f}\n")

    said = jobworker.admit(str(tmp_path), "tap_d1", "dave", total=4, per_client=2)
    assert said and "running 4 jobs" in said

    # An exempt client is held to the overall limit only.
    assert jobworker.admit(str(tmp_path), "tap_a3", "alice", total=10, per_client=2, exempt=("alice",)) is None
    jobworker.finished()
    assert not (tmp_path / ".running" / "tap_a3").exists()


def test_admit_does_not_count_jobs_that_ended_without_saying_so(tmp_path, monkeypatch):
    """A killed worker, or one whose server went away, must not hold its place for ever."""
    monkeypatch.setattr(jobworker, "_registered", None)
    _running(tmp_path, "tap_dead1", "alice", age=3600)
    _running(tmp_path, "tap_dead2", "alice", age=3600)
    (tmp_path / ".running" / "tap_deleted").write_text("alice\n")     # its directory is gone

    assert jobworker.admit(str(tmp_path), "tap_new", "alice", total=2, per_client=2) is None
    assert sorted(p.name for p in (tmp_path / ".running").iterdir()) == ["tap_new"]
    jobworker.finished()


def test_no_limits_no_register(tmp_path):
    assert jobworker.admit(str(tmp_path), "tap_x", "alice") is None
    assert not (tmp_path / ".running").exists()


def test_stopped_job_gives_up_its_place_at_once(tmp_path):
    """A killed worker cannot unregister itself; the one stopping it does."""
    jobdir = tmp_path / "tap_j"
    jobdir.mkdir()
    proc = _worker(jobdir)
    (tmp_path / ".running").mkdir()
    (tmp_path / ".running" / "tap_j").write_text("alice\n")
    jobworker.stop(jobdir, 0)
    proc.wait(timeout=5)
    assert not (tmp_path / ".running" / "tap_j").exists()


def test_jobs_arriving_together_do_not_all_go_in(tmp_path, monkeypatch):
    """Twelve at the same moment against a limit of five: five run, never six."""
    import threading
    monkeypatch.setattr(jobworker, "owner", lambda path: ("host", 1, 0))
    monkeypatch.setattr(jobworker.atexit, "register", lambda f: None)
    for round_ in range(20):
        tapdir = tmp_path / f"r{round_}"
        tapdir.mkdir()
        go, said = threading.Barrier(12), {}

        def one(n):
            go.wait()
            said[n] = jobworker.admit(str(tapdir), f"tap_{n:02d}", "127.0.0.1", total=8, per_client=1,
                                      shared=("127.0.0.1",), shared_limit=5)

        threads = [threading.Thread(target=one, args=(n,)) for n in range(12)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        assert sum(v is None for v in said.values()) == 5
        assert len(list((tapdir / ".running").iterdir())) == 5


def test_shared_client_has_its_own_limit(tmp_path, monkeypatch):
    """A portal's address stands for many users: its own limit, not one client's and not none."""
    monkeypatch.setattr(jobworker, "owner", lambda path: ("host", 1, 0))
    monkeypatch.setattr(jobworker.atexit, "register", lambda f: None)
    kw = dict(total=8, per_client=1, shared=("127.0.0.1",), shared_limit=3)

    for n in range(3):          # more than one client may have, up to the portal's own limit
        assert jobworker.admit(str(tmp_path), f"tap_p{n}", "127.0.0.1", **kw) is None
    refused = jobworker.admit(str(tmp_path), "tap_p3", "127.0.0.1", **kw)
    assert refused and "portal" in refused

    # Someone else still has a place: the portal could not take all eight.
    assert jobworker.admit(str(tmp_path), "tap_q0", "alice", **kw) is None
    refused = jobworker.admit(str(tmp_path), "tap_q1", "alice", **kw)
    assert refused and "You have 1 jobs" in refused

    # No shared limit set: the portal is held to the overall limit only.
    kw["shared_limit"] = 0
    assert jobworker.admit(str(tmp_path), "tap_p4", "127.0.0.1", **kw) is None
