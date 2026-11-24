"""
How much a client has been sent, and a cap on it.

The service can be told to send one client no more than so many bytes in a
day (VOLUME_LIMIT in the configuration; SHARED_VOLUME_LIMIT for a client
that stands for many users).  The count is kept in the work area, which
every task of the service shares, so the cap holds across tasks and across
restarts:

    <workdir>/TAP/.volume/<client>/<hour>

One file for each hour in which the client was sent something, holding one
number a line, appended by the request that sent it.  The day is the last 24
of those hours and the one in progress.  Files older than two days are
removed by whoever next reads the client's count.

Answers shorter than SMALL bytes are not written down: nearly every request
is one (a job's phase, the availability check), and a write to the shared
work area for each would cost more than it tells.
"""

import os
import re
import time

HOUR = 3600
HOURS = 24
SMALL = 4096
FOLDER = os.path.join('TAP', '.volume')
EVERYONE = '-everyone-'     # the count for all clients together; no address is spelled so

UNITS = {'': 1, 'B': 1, 'KB': 1000, 'MB': 1000 ** 2, 'GB': 1000 ** 3, 'TB': 1000 ** 4}


def parse_size(text):
    """'20GB', '500 MB', '1.5gb' or a plain number of bytes; 0 for nothing or 'none'."""
    text = str(text if text is not None else '').strip().upper()
    if text in ('', '0', 'NONE'):
        return 0
    m = re.fullmatch(r'(\d+(?:\.\d+)?)\s*([KMGT]?B?)', text)
    if not m:
        raise ValueError(f'not a size: {text!r} (write it like 20GB or 500MB)')
    unit = m.group(2) if m.group(2) in UNITS else m.group(2) + 'B'
    return int(float(m.group(1)) * UNITS[unit])


def show_size(n):
    for unit in ('TB', 'GB', 'MB', 'KB'):
        if n >= UNITS[unit]:
            return ('%.1f' % (n / UNITS[unit])).rstrip('0').rstrip('.') + ' ' + unit
    return f'{int(n)} bytes'


def _folder(workdir, client):
    return os.path.join(workdir, FOLDER, re.sub(r'[^0-9A-Za-z._-]', '_', client) or '_')


def add(workdir, client, sent, now=None):
    """Write down that the client was sent this many bytes."""
    if sent < SMALL:
        return
    now = time.time() if now is None else now
    folder = _folder(workdir, client)
    os.makedirs(folder, exist_ok=True)
    # One short line, appended: requests ending at the same moment do not
    # lose each other's.
    fd = os.open(os.path.join(folder, str(int(now // HOUR))), os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(fd, f'{int(sent)}\n'.encode())
    finally:
        os.close(fd)


def used(workdir, client, now=None):
    """
    (bytes sent to the client in the last day, seconds until the oldest hour
    of it no longer counts).  (0, 0) for a client that has been sent nothing.
    """
    now = time.time() if now is None else now
    folder = _folder(workdir, client)
    try:
        names = os.listdir(folder)
    except OSError:
        return 0, 0

    this = int(now // HOUR)
    total, oldest = 0, None
    for name in names:
        try:
            hour = int(name)
        except ValueError:
            continue
        path = os.path.join(folder, name)
        if hour < this - 2 * HOURS:
            try:
                os.remove(path)
            except OSError:
                pass
            continue
        if hour < this - HOURS:
            continue
        try:
            with open(path) as f:
                got = sum(int(line) for line in f if line.strip().isdigit())
        except OSError:
            continue
        if got:
            total += got
            oldest = hour if oldest is None else min(oldest, hour)

    wait = 0 if oldest is None else max(1, int((oldest + HOURS + 1) * HOUR - now))
    return total, wait


class Counted:
    """A text stream that counts what is written to it."""

    def __init__(self, stream):
        self._stream = stream
        self.sent = 0

    def write(self, text):
        # Characters, which for the tables this service writes are bytes
        # but for the rare name or description that is not ASCII.
        self.sent += len(text)
        return self._stream.write(text)

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def __getattr__(self, name):
        return getattr(self._stream, name)
