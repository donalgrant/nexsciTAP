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
"""

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
            if aborted(jobdir):
                # Asked to stop from another server, or the job was deleted.
                os._exit(1)
            try:
                _beat(path)
            except OSError:
                pass

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
