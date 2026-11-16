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

    <host name> <process id>

  * The worker touches it every few seconds for as long as it runs.  A
    worker file that has not been touched for a while belongs to a job
    whose server is gone; reading the job's status then records it as
    failed (mark_lost).

  * stop() kills the process only on the host that owns it.  Anywhere else
    it leaves a file "abort" in the job's directory, which the worker
    notices within a few seconds and exits.  The worker also exits when its
    directory disappears (the job was deleted).

Jobs without a worker file (started by an older version, or not yet
started) are handled as before: by process number, on this host.
"""

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

BEAT = 3        # seconds between a worker's signs of life
STALE = 60      # a worker silent this long is taken to be gone

LOST_MESSAGE = 'The server running this job stopped before the job finished.  Submit it again.'


def _host():
    return socket.gethostname()


def start(jobdir, beat=BEAT):
    """Called in the process that runs the job, once the job is published."""

    path = os.path.join(jobdir, WORKER)
    abort = os.path.join(jobdir, ABORT)

    try:
        with open(path, 'w') as f:
            f.write(f'{_host()} {os.getpid()}\n')
    except OSError as e:
        logging.error(f'Could not write {path}: {e}')
        return

    def watch():
        while True:
            time.sleep(beat)
            if os.path.exists(abort) or not os.path.isdir(jobdir):
                # Asked to stop from another server, or the job was deleted.
                os._exit(1)
            try:
                os.utime(path, None)
            except OSError:
                pass

    threading.Thread(target=watch, name='job-watch', daemon=True).start()


def aborted(jobdir):
    return os.path.exists(os.path.join(jobdir, ABORT))


def owner(jobdir):
    """(host, pid, seconds since last sign of life), or None if not recorded."""

    path = os.path.join(jobdir, WORKER)
    try:
        with open(path) as f:
            host, pid = f.read().split()
        return host, int(pid), time.time() - os.stat(path).st_mtime
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

    try:
        with open(statuspath) as f:
            text = f.read()
    except OSError:
        return False

    if '<uws:phase>EXECUTING</uws:phase>' not in text:
        return False

    text = text.replace('<uws:phase>EXECUTING</uws:phase>', '<uws:phase>ERROR</uws:phase>', 1)
    summary = ('    <uws:errorSummary type="transient" hasDetail="true">\n'
               f'        <uws:message>{html.escape(LOST_MESSAGE)}</uws:message>\n'
               '    </uws:errorSummary>\n')
    text = re.sub(r'</uws:job>\s*$', summary + '</uws:job>\n', text, count=1)

    tmp = f'{statuspath}.{os.getpid()}.tmp'
    try:
        with open(tmp, 'w') as f:
            f.write(text)
        os.replace(tmp, statuspath)
    except OSError as e:
        logging.error(f'Could not record the lost job in {statuspath}: {e}')
        return False

    return True
