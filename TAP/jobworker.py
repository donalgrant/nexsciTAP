# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

"""
Async jobs when the work area is shared by more than one server.

A job's status document names the process running it (uws:runId), and the
service stopped a job by killing that process number.  That only means
something on the machine the job runs on.  When several servers share one
work area (a network file system), the request to abort or delete a job can
reach a different server, where the same number is some unrelated process,
or nothing.  And when a server goes away, the jobs it was running stay
"EXECUTING" in the shared area for ever.

So each job's directory also holds a small file, "worker", written by the
process that runs the job:

    <host name> <process id> <time of this writing>

  * The worker rewrites it every few seconds for as long as it runs.  A
    worker file whose time is long past belongs to a job whose server is
    gone; reading the job's status then records it as failed (mark_lost).

    The time is in the file, not taken from the file's modification time,
    because of how network file systems behave: another machine sees a
    file's attributes up to a minute late (they are cached), but opening a
    file always checks it afresh.  For the same reason nothing here
    replaces a file by renaming a new one over it; a machine that had the
    old one open, or cached, would go on using it.

  * stop() kills the process only on the host that owns it.  Anywhere else
    it leaves the worker to stop itself: every few seconds the worker reads
    its job's status document, and exits if the job has been marked ABORTED
    or the document is gone (the job was deleted).  stop() also leaves a
    file "abort" in the job's directory, for the moment between the request
    to stop and the status being written; a new file can take a while to
    show on another machine, which is why the status document, an existing
    file opened afresh each time, is the signal relied on.

Jobs without a worker file (started by an older version, or not yet
started) are handled as before: by process number, on this host.

The work area also holds a register of the jobs running now, one small
file per job under "<workdir>/TAP/.running", naming the client that
submitted it.  admit() counts them to hold a client, and the service as a
whole, to a number of jobs at once.  Being files in the shared area, the
count covers every server; it can be a little out of date between
machines, so the limits are close rather than exact.
"""

import atexit
import fcntl
import html
import logging
import os
import re
import signal
import socket
import threading
import time

WORKER = 'worker'
ABORT = 'abort'
STATUS = 'status.xml'
ABORTED = '<uws:phase>ABORTED</uws:phase>'

BEAT = 3        # seconds between a worker's signs of life
STALE = 90      # a worker silent this long is taken to be gone

RUNNING = '.running'

_registered = None

LOST_MESSAGE = 'The server running this job stopped before the job finished.  Submit it again.'


def _host():
    return socket.gethostname()


def _beat(path):
    with open(path, 'w') as f:
        f.write(f'{_host()} {os.getpid()} {time.time():.0f}\n')


def start(jobdir, beat=BEAT):
    """Called in the process that runs the job, once the job is published."""

    path = os.path.join(jobdir, WORKER)

    try:
        _beat(path)
    except OSError as e:
        logging.error(f'Could not write {path}: {e}')
        return

    def watch():
        while True:
            time.sleep(beat)
            try:
                if aborted(jobdir):
                    # Asked to stop from another server, or the job was deleted.
                    logging.warning(f'{jobdir}: aborted or deleted; worker {os.getpid()} stopping')
                    finished()
                    os._exit(1)
                _beat(path)
            except Exception as e:
                # Keep going: one failed sign of life is not the end of the job.
                logging.error(f'{jobdir}: worker {os.getpid()} could not report: {e!r}')

    threading.Thread(target=watch, name='job-watch', daemon=True).start()


def aborted(jobdir):
    """Has the job been aborted or deleted, here or from another server?"""

    if os.path.exists(os.path.join(jobdir, ABORT)):
        return True
    try:
        with open(os.path.join(jobdir, STATUS)) as f:
            return ABORTED in f.read()
    except FileNotFoundError:
        return True
    except OSError:
        return False


def owner(jobdir):
    """
    (host, pid, seconds since last sign of life), or None if not recorded.
    None too for a file caught half written; the next look will see it whole.
    """

    try:
        with open(os.path.join(jobdir, WORKER)) as f:
            host, pid, when = f.read().split()
        return host, int(pid), time.time() - float(when)
    except (OSError, ValueError):
        return None


def stop(jobdir, pid):
    """Stop the process running the job in jobdir, wherever it is."""

    own = owner(jobdir)

    if own is not None and own[0] != _host():
        try:
            with open(os.path.join(jobdir, ABORT), 'w'):
                pass
        except OSError as e:
            logging.error(f'Could not ask the worker on {own[0]} to stop: {e}')
        return

    if own is not None:
        pid = own[1]

    if pid > 0 and pid != os.getpid():
        os.kill(pid, signal.SIGKILL)

    # The worker cannot say it is no longer running; say it for it.
    try:
        os.remove(os.path.join(os.path.dirname(jobdir), RUNNING, os.path.basename(jobdir)))
    except OSError:
        pass


def mark_lost(jobdir, statuspath, stale=STALE):
    """
    If the job is recorded as executing but its worker has been silent for
    `stale` seconds, rewrite its status document as an error.  Returns True
    if it did.
    """

    own = owner(jobdir)
    if own is None or own[2] < stale:
        return False

    # In place, under the lock the service's own status writes take.
    try:
        with open(statuspath, 'r+') as f:
            fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            text = f.read()

            if '<uws:phase>EXECUTING</uws:phase>' not in text:
                return False

            # The worker may have spoken while we were getting here.
            own = owner(jobdir)
            if own is None or own[2] < stale:
                return False

            text = text.replace('<uws:phase>EXECUTING</uws:phase>', '<uws:phase>ERROR</uws:phase>', 1)
            summary = ('    <uws:errorSummary type="transient" hasDetail="true">\n'
                       f'        <uws:message>{html.escape(LOST_MESSAGE)}</uws:message>\n'
                       '    </uws:errorSummary>\n')
            text = re.sub(r'</uws:job>\s*$', summary + '</uws:job>\n', text, count=1)

            f.seek(0)
            f.truncate()
            f.write(text)

    except OSError as e:
        logging.error(f'Could not record the lost job in {statuspath}: {e}')
        return False

    return True


class _OneAtATime:
    """
    Held while a job counts the running jobs and takes its place, so that no
    two jobs do that at once: a lock on a file beside the register, which
    works across the tasks that share the work area.  If the lock cannot be
    had in a few seconds the job goes on without it, and says so in the log;
    the places taken by time (see admit) then keep the count nearly exact.
    """

    def __init__(self, register, wait=5.0):
        self.path, self.wait, self.f = register + '.lock', wait, None

    def __enter__(self):
        try:
            import fcntl
            f = open(self.path, 'a')
            until = time.time() + self.wait
            while True:
                try:
                    fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self.f = f
                    break
                except OSError:
                    if time.time() > until:
                        f.close()
                        logging.error(f'No lock on {self.path} in {self.wait} s; counting the jobs without it')
                        break
                    time.sleep(0.01)
        except Exception as e:
            logging.error(f'Could not lock {self.path}: {e}')
        return self

    def __exit__(self, *a):
        if self.f:
            try:
                self.f.close()          # closing gives the lock back
            except OSError:
                pass


def admit(tapdir, jobid, client, total=0, per_client=0, exempt=(), shared=(), shared_limit=0):
    """
    Counts and takes the place with nothing else doing the same meanwhile.
    """
    if total <= 0 and (shared_limit if client in shared and client not in exempt else per_client) <= 0:
        return None                 # no limit applies: nothing to count, no register
    register = os.path.join(tapdir, RUNNING)
    try:
        os.makedirs(register, exist_ok=True)
    except OSError:
        pass
    with _OneAtATime(register):
        return _admit(tapdir, jobid, client, total, per_client, exempt, shared, shared_limit)


def _admit(tapdir, jobid, client, total=0, per_client=0, exempt=(), shared=(), shared_limit=0):
    """
    Called by a job's worker before it runs the query.  Returns None and
    registers the job as running, or the message to fail the job with.

    total       most jobs running at once in this work area   (0: no limit)
    per_client  most jobs one client may have running at once  (0: no limit)
    exempt      clients the per-client limit does not apply to
    shared      clients that stand for many users (a portal on the same host)
    shared_limit  most jobs such a client may have running at once (0: no limit)
    """

    global _registered

    is_shared = client in shared and client not in exempt
    if is_shared:
        per_client = shared_limit

    if total <= 0 and per_client <= 0:
        return None

    register = os.path.join(tapdir, RUNNING)
    entry = os.path.join(register, jobid)

    # The job takes its place first and counts afterward, and counts only
    # the jobs that took theirs before it.  Counting first let jobs that
    # arrived at the same moment each see room and all go in: seven sent at
    # once through a portal limited to five, six ran (21 November 2026).
    # Of jobs arriving together, the earlier places now win, and exactly
    # as many as the limit allows.
    try:
        os.makedirs(register, exist_ok=True)
        with open(entry, 'w') as f:
            f.write(client + '\n')
        place = (os.stat(entry).st_mtime_ns, jobid)
        names = os.listdir(register)
    except OSError as e:
        logging.error(f'Could not use {register}: {e}')
        return None                 # a limit that cannot be checked does not stop the service

    everyone = mine = 0

    for name in names:
        if name == jobid:
            continue
        other = os.path.join(register, name)
        own = owner(os.path.join(tapdir, name))
        if own is None or own[2] >= STALE:
            # Finished without saying so (killed), or its server is gone.
            try:
                os.remove(other)
            except OSError:
                pass
            continue
        try:
            if (os.stat(other).st_mtime_ns, name) > place:
                continue            # it came after this job: it is the one to give way
            with open(other) as f:
                whose = f.read().strip()
        except OSError:
            continue
        everyone += 1
        mine += whose == client

    said = None

    if per_client > 0 and is_shared and mine >= per_client:
        said = (f'This portal has {mine} jobs running for its users, the most this service runs for it at once.  '
                'Submit this one again when one of them has finished.')

    elif per_client > 0 and client not in exempt and mine >= per_client:
        said = (f'You have {mine} jobs running, the most this service runs for one client at once.  '
                'Submit this one again when one of them has finished.')

    elif total > 0 and everyone >= total:
        said = (f'The service is running {everyone} jobs, the most it runs at once.  '
                'Submit this one again shortly.')

    if said:
        try:
            os.remove(entry)
        except OSError:
            pass
        return said

    _registered = entry
    atexit.register(finished)
    return None


def finished():
    """The job this process registered as running is over."""

    global _registered

    if _registered:
        try:
            os.remove(_registered)
        except OSError:
            pass
        _registered = None
