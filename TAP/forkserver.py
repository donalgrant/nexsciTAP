# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

"""
Serve nexsciTAP from a pre-loaded process that forks once per request.

    python -m TAP.forkserver --conf TAP.conf [--host 127.0.0.1] [--port 8080]
                             [--user www-data] [--max-children 16]

Under a web server's CGI interface every request starts a new Python
interpreter and imports the whole service again (astropy alone takes several
tenths of a second), which dominates the response time of small queries.

This server imports everything once.  Each request is then handled in a child
made with fork(), which starts with the modules already loaded and otherwise
runs exactly the code a CGI request runs (TAP.tap.Tap):  the child gets the
CGI environment variables, the request body on standard input, and the client
connection as standard output, so the service's own HTTP response ("nph"
output) goes straight to the client.

The parent never opens a database connection and never starts a thread, so
fork() is safe.  A child serves one request and exits, so nothing a request
does can leak into the next one.

The TAP endpoints appear under /TAP, and files in TAP_WORKDIR are served under
TAP_WORKURL, except uploaded tables (upload_*.vot) and the service's own
bookkeeping files, which stay private.

CLIENT_LIMIT in the configuration's [WEB] section is the most requests one
client may have in progress at once; one more is refused with status 429 and
a Retry-After header, so that a single client cannot take every place and
shut the others out.  A client is the address the connection comes from, or,
behind a reverse proxy, the value of the header named by CLIENT_HEADER, which
the proxy must set itself (never pass on what the client sent).  Addresses in
UNLIMITED_CLIENTS are exempt.

TOTAL_LIMIT is the most requests in progress from all clients together; one
more is refused the same way.  It should be below --max-children, beyond which
requests wait unanswered, health checks among them.  The clients named in
TOTAL_EXEMPT (default "health", the name a container's own health check
gives itself) are neither counted nor refused.

STOP_ABANDONED=on stops a query whose client has closed the connection
before the answer was ready (it gave up, or a proxy in front timed out).
The server must then be behind a proxy that keeps the connection open while
it waits, as nginx does;  a client that closes its sending side as soon as
it has sent the request would be taken for gone.

There is no TLS and no authentication:  run it behind a reverse proxy.
"""

import argparse
import fnmatch
import grp
import http.server
import json
import mimetypes
import os
import pwd
import re
import shutil
import signal
import socket
import socketserver
import struct
import sys
import tempfile
import time
from urllib.parse import unquote, urlsplit
from xml.sax.saxutils import escape as _xml_escape

PRIVATE_FILES = ('upload_*.vot', 'worker', 'abort')

# Imported on demand by the service; loading them here keeps that cost out of
# the requests.  Missing optional packages are skipped.
PRELOAD = ('duckdb', 'numpy', 'pyarrow', 'astropy.units', 'astropy.io.votable',
           'astropy_healpix', 'lxml.etree', 'ipaddress', 'http.client', 'ssl')

_tap = None
_cgi = None
_volume = None


def preload():
    """Import the service and its lazy dependencies in the parent."""

    global _tap, _cgi, _volume

    # TAP.tap parses a CGI form when it is imported.  Give it an empty one
    # so the import neither reads standard input nor the command line.
    os.environ['REQUEST_METHOD'] = 'GET'
    os.environ['QUERY_STRING'] = ''

    import cgi

    from TAP import tap, volume

    _tap, _cgi, _volume = tap, cgi, volume

    for name in PRELOAD:
        try:
            __import__(name)
        except ImportError:
            pass

    # The service reads its own job status documents, which are XML, with
    # BeautifulSoup's HTML parser, and its tag look-ups rely on what that
    # parser does to the names.  BeautifulSoup says so in seven lines of
    # warning each time anyone looks at a job, which buries the server's log.
    try:
        import warnings

        from bs4 import XMLParsedAsHTMLWarning
        warnings.filterwarnings('ignore', category=XMLParsedAsHTMLWarning)
    except ImportError:
        pass


# A job's own record:  /async/<job>, and its phase, error, results list and the like.
# Not the list of jobs, and not a result file (those are served from the work area).
JOB_STATUS = re.compile(r'/async/[A-Za-z0-9_.-]+(/(phase|error|quote|executionduration|destruction|owner|parameters|results))?/?$')


FORM_MOST = 2 * 1024 * 1024         # bytes of a request body that is not a multipart upload
FORM_FIELDS = 100                   # fields in one request
HARD_LIMIT = int(os.environ.get('TAP_HARD_LIMIT', '3600') or 0)     # seconds one request's process may live; 0: no limit
SOCKET_WAIT = 120                   # seconds a send or receive may make no progress
# Bytes of data one request's process may hold (TAP_PROCESS_MEMORY, written like 2GB; unset or 0: no limit).
PROCESS_MEMORY = 0


class Handler(http.server.BaseHTTPRequestHandler):

    def setup(self):
        # A client that stops reading its answer, or stops sending its
        # request, would otherwise hold one of the server's processes for as
        # long as it liked.  Set on the socket itself, so that it also holds
        # for the copy of it that is standard output.
        try:
            wait = struct.pack('ll', SOCKET_WAIT, 0)
            self.request.setsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO, wait)
            self.request.setsockopt(socket.SOL_SOCKET, socket.SO_RCVTIMEO, wait)
        except OSError:
            pass
        super().setup()

    conf = ''
    workdir = ''
    workurl = '/workspace'

    # Seconds a request may take to arrive; large uploads over slow links.
    timeout = 900

    client_header = ''
    client_limit = 0
    unlimited_clients = ()
    shared_clients = ()
    shared_client_limit = 0
    volume_limit = 0
    shared_volume_limit = 0
    total_limit = 0
    total_exempt = ('health',)
    total_volume_limit = 0
    dldir = ''
    stop_abandoned = False
    rundir = ''
    _place = None
    _why = ''

    def _client(self):
        """Who is asking: the proxy's word for it if configured, else the peer address."""
        named = self.headers.get(self.client_header, '') if self.client_header else ''
        # (Not '..' or the like:  the name becomes a directory's.)
        named = re.sub(r'[^0-9A-Za-z.:_-]', '', named.split(',')[-1])[:64].strip('.')
        return named or self.client_address[0]

    def _limit(self, client):
        """Requests this client may have in progress at once (0: no limit)."""
        if client in self.unlimited_clients:
            return 0
        if client in self.shared_clients:
            # An address many users share (a portal beside this service):
            # its own limit, for all of them together.
            return self.shared_client_limit
        return self.client_limit

    def _admit(self, client):
        """Take a place for this request, unless the client has its share already."""
        limit = self._limit(client)
        total = 0 if (client in self.total_exempt or client in self.unlimited_clients) else self.total_limit
        if limit <= 0 and total <= 0:
            return True

        # One file per request in progress, named by its process.
        places = os.path.join(self.rundir, client.replace(':', '_'))
        os.makedirs(places, exist_ok=True)
        mine = os.path.join(places, str(os.getpid()))
        with open(mine, 'w'):
            pass

        def held_in(folder):
            n = 0
            try:
                names = os.listdir(folder)
            except OSError:
                return 0
            for name in names:
                try:
                    os.kill(int(name), 0)
                    n += 1
                except (OSError, ValueError):
                    try:                    # that request is over
                        os.remove(os.path.join(folder, name))
                    except OSError:
                        pass
            return n

        if limit > 0 and held_in(places) > limit:
            os.remove(mine)
            Handler._why = 'client'
            return False

        if total > 0:
            everyone = sum(held_in(os.path.join(self.rundir, d)) for d in os.listdir(self.rundir)
                           if d.replace('_', ':') not in self.total_exempt and d not in self.total_exempt)
            if everyone > total:
                os.remove(mine)
                Handler._why = 'total'
                return False

        Handler._place = mine
        return True

    def _admit_download(self, client):
        """
        Take a place for a file fetched from the work area, unless the client
        has as many downloads in progress as it may have requests.  A
        download is held for as long as its reader cares to take, so without
        this one client could hold every process the server has (found in
        review, 23 November 2026).  Counted apart from the client's requests:
        a client fetching results is not thereby refused its next query.
        """
        limit = self._limit(client)
        if limit <= 0 or not self.dldir:
            return True
        places = os.path.join(self.dldir, client.replace(':', '_'))
        os.makedirs(places, exist_ok=True)
        mine = os.path.join(places, str(os.getpid()))
        with open(mine, 'w'):
            pass
        held = 0
        for name in os.listdir(places):
            try:
                os.kill(int(name), 0)
                held += 1
            except (OSError, ValueError):
                try:
                    os.remove(os.path.join(places, name))
                except OSError:
                    pass
        if held > limit:
            os.remove(mine)
            return False
        Handler._place = mine
        return True

    def _volume_limit(self, client):
        """Bytes this client may be sent in a day (0: no limit)."""
        if client in self.unlimited_clients:
            return 0
        if client in self.shared_clients:
            return self.shared_volume_limit
        return self.volume_limit

    def _over_volume(self, client):
        """True, with the refusal sent, if the client has had its day's volume."""
        if client in self.unlimited_clients or client in self.total_exempt:
            return False
        # Everyone together first:  the caps by client bound what one
        # address is sent, and any number of addresses is any volume.
        if self.total_volume_limit > 0:
            try:
                had, wait = _volume.used(self.workdir, _volume.EVERYONE)
            except Exception as e:                  # a count that cannot be read refuses nobody
                sys.stderr.write(f'volume not read: {e!r}\n')
                had, wait = 0, 0
            if had >= self.total_volume_limit:
                soon = f'{wait // 3600 + 1} hours' if wait >= 5400 else f'{max(1, wait // 60)} minutes'
                body = (f'This service sends at most {_volume.show_size(self.total_volume_limit)} in a day to all '
                        f'its users together, and has sent {_volume.show_size(had)} in the last day.  '
                        f'Try again in about {soon}.\n')
                self._refuse(body, wait, status=503)
                return True
        limit = self._volume_limit(client)
        if limit <= 0:
            return False
        try:
            had, wait = _volume.used(self.workdir, client)
        except Exception as e:                      # a count that cannot be read refuses nobody
            sys.stderr.write(f'volume not read: {e!r}\n')
            return False
        if had < limit:
            return False
        who = 'this portal, for all its users,' if client in self.shared_clients else client
        soon = f'{wait // 3600 + 1} hours' if wait >= 5400 else f'{max(1, wait // 60)} minutes'
        body = (f'This service sends one client at most {_volume.show_size(limit)} in a day, and {who} '
                f'has been sent {_volume.show_size(had)} in the last day.  Try again in about {soon}.\n').encode()
        self._refuse(body, wait)
        return True

    def _refuse(self, message, wait, status=429):
        """Answer 429 (or the status given) with the message as a DALI error document.

        TAP clients read an error from a VOTable whose QUERY_STATUS is ERROR
        and show its text;  given plain text they show only the status, or
        fail on the parse.  The message stays readable to a person with curl.
        """
        if isinstance(message, bytes):
            message = message.decode()
        body = ('<?xml version="1.0" encoding="UTF-8"?>\n'
                '<VOTABLE version="1.3" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">\n'
                '<RESOURCE type="results">\n'
                '<INFO name="QUERY_STATUS" value="ERROR">\n'
                + _xml_escape(message.strip()) +
                '\n</INFO>\n</RESOURCE>\n</VOTABLE>\n').encode()
        self.send_response(status)
        self.send_header('Retry-After', str(wait))
        self.send_header('Content-Type', 'application/x-votable+xml')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _sent(self, client, sent):
        """Write down what the client was sent, where a cap on it is in force."""
        if not sent or client in self.unlimited_clients or client in self.total_exempt:
            return
        try:
            if self._volume_limit(client) > 0:
                _volume.add(self.workdir, client, sent)
            if self.total_volume_limit > 0:
                _volume.add(self.workdir, _volume.EVERYONE, sent)
        except Exception as e:                      # a count, never a reason to fail
            sys.stderr.write(f'volume not written: {e!r}\n')

    def _release(self):
        if self._place:
            try:
                os.remove(self._place)
            except OSError:
                pass

    def do_GET(self):
        self._dispatch()

    def do_POST(self):
        self._dispatch()

    def do_DELETE(self):
        self._dispatch()

    def do_HEAD(self):
        path = urlsplit(self.path).path
        if path.startswith(self.workurl + '/'):
            return self._static(unquote(path[len(self.workurl) + 1:]), body=False)
        self.send_error(405)

    def log_message(self, fmt, *args):
        sys.stderr.write('%s - - [%s] %s\n' % (self.address_string(),
                         self.log_date_time_string(), fmt % args))
        sys.stderr.flush()

    def _dispatch(self):
        parts = urlsplit(self.path)
        path = parts.path

        if path.startswith(self.workurl + '/'):
            return self._static(unquote(path[len(self.workurl) + 1:]))

        if path == '/TAP' or path.startswith('/TAP/'):
            return self._tap(path[len('/TAP'):], parts.query)

        self.send_error(404)

    def _static(self, rel, body=True):
        root = os.path.realpath(self.workdir)
        full = os.path.realpath(os.path.join(root, rel))
        name = os.path.basename(full)

        hidden = any(part.startswith('.') for part in full[len(root):].split(os.sep))

        if (not full.startswith(root + os.sep) or not os.path.isfile(full) or hidden
                or any(fnmatch.fnmatch(name, p) for p in PRIVATE_FILES)):
            return self.send_error(404)

        ctype = mimetypes.guess_type(full)[0] or 'application/octet-stream'
        if full.endswith('.xml'):
            ctype = 'text/xml'

        # A result fetched from the work area counts toward the client's
        # volume for the day, as one returned by a sync query does.
        client = self._client()
        if body and self._over_volume(client):
            return
        if body and not self._admit_download(client):
            who = ('through this portal' if client in self.shared_clients else f'by {client}')
            return self._refuse(f'Too many files being fetched {who} at once (this service sends '
                                f'{self._limit(client)} at a time).  Try again shortly.\n', 5)

        try:
            with open(full, 'rb') as f:
                size = os.fstat(f.fileno()).st_size
                self.send_response(200)
                self.send_header('Content-Type', ctype)
                self.send_header('Content-Length', str(size))
                self.end_headers()
                if body:
                    # What is counted is what went out, also when the client
                    # hangs up part-way:  a download cut short and started
                    # again used to count for nothing.
                    sent = 0
                    try:
                        while True:
                            chunk = f.read(1 << 20)
                            if not chunk:
                                break
                            self.wfile.write(chunk)
                            sent += len(chunk)
                    finally:
                        self._sent(client, sent)
        finally:
            self._release()

    def _tap(self, path_info, query):
        """Run the CGI entry point in this (already forked) process."""

        try:
            length = int(self.headers.get('Content-Length', '0') or '0')
        except ValueError:
            return self.send_error(400)

        if length < 0:
            return self.send_error(400)

        # A form that is not an upload is a query and a few settings.  Read
        # as a form, a body costs some fifty times its size in memory, so
        # tens of megabytes of "a=1&a=1&..." were gigabytes (found in
        # review, 23 November 2026).
        if length > FORM_MOST and 'multipart/form-data' not in (self.headers.get('Content-Type') or '').lower():
            # What is nearly sent already is read and dropped, so that the
            # client hears the answer and not a closed connection.
            left = min(length, 4 * FORM_MOST)
            while left > 0:
                chunk = self.rfile.read(min(left, 1 << 16))
                if not chunk:
                    break
                left -= len(chunk)
            self.close_connection = True
            return self.send_error(413, 'A request that is not an upload may be at most %d bytes' % FORM_MOST)

        # However it goes, this request's process does not outlive this:  an
        # upload is read before the query's own time limit starts, and
        # reading a hostile one could run for ever, holding its place.
        if HARD_LIMIT > 0:
            signal.alarm(HARD_LIMIT)

        # And it has a size.  The engine's memory limit does not count a
        # single huge value, nor what Python holds of a result or an upload:
        # one request could take several GB and have the kernel kill
        # whatever else was running in the container (found in review, 23
        # November 2026).  With this the request itself fails.
        if PROCESS_MEMORY > 0:
            try:
                import resource
                resource.setrlimit(resource.RLIMIT_DATA, (PROCESS_MEMORY, PROCESS_MEMORY))
            except (ImportError, ValueError, OSError) as e:
                sys.stderr.write(f'no memory limit set for the request: {e!r}\n')

        client = self._client()
        # Asking how a job is doing takes no place.  It does no work, and a
        # client that is refused the answer takes its job for failed:  Firefly
        # showed "Error getting phase" for a search that was in fact running
        # (21 November 2026).  The firewall in front still bounds how often.
        if not (self.command in ('GET', 'HEAD') and JOB_STATUS.match(path_info or '')) and not self._admit(client):
            if self._why == 'total':
                body = (f'This service is handling {self.total_limit} requests, the most it takes at once.  '
                        f'Try again shortly.\n').encode()
            elif client in self.shared_clients:
                body = (f'Too many requests in progress through this portal '
                        f'(this service handles {self._limit(client)} at once for all its users).  Try again shortly.\n').encode()
            else:
                body = (f'Too many requests in progress from {client} '
                        f'(this service handles {self._limit(client)} at once for one client).  Try again shortly.\n').encode()
            self._refuse(body, 5)
            return

        if self._over_volume(client):
            self._release()
            return

        # The request body becomes standard input.  It is spooled to an
        # unnamed file because part of it is already in this handler's
        # read buffer, and uploads can be hundreds of megabytes.
        body = tempfile.TemporaryFile(dir=self.workdir)
        left = length
        while left > 0:
            chunk = self.rfile.read(min(left, 1 << 20))
            if not chunk:
                break
            body.write(chunk)
            left -= len(chunk)
        body.flush()
        body.seek(0)

        self.log_request('-')

        for key in [k for k in os.environ if k.startswith('HTTP_')]:
            del os.environ[key]

        os.environ.update({
            'GATEWAY_INTERFACE': 'CGI/1.1',
            'SERVER_PROTOCOL': 'HTTP/1.1',
            'SERVER_SOFTWARE': 'nexsciTAP-forkserver/1.0',
            'REQUEST_METHOD': self.command,
            'QUERY_STRING': query,
            'PATH_INFO': path_info,
            'SCRIPT_NAME': '/TAP',
            'REMOTE_ADDR': self.client_address[0],
            'TAP_CLIENT': client,
            'TAP_ASYNC_STDERR': 'keep',
            'CONTENT_TYPE': self.headers.get('Content-Type', ''),
            'CONTENT_LENGTH': str(length),
            'HTTP_HOST': self.headers.get('Host', ''),
            'TAP_CONF': self.conf,
        })

        # Standard input and output now belong to the request.  Every other
        # reference to the client connection and to the listening socket is
        # dropped, so that an async job's background process, which points
        # its standard streams at /dev/null, holds neither of them open.
        #
        # The handler's read timeout put the socket in non-blocking mode,
        # which is a property of the connection, not of this file object.
        # The service writes with plain blocking writes, and would otherwise
        # fail part way through any response larger than the socket buffer
        # whenever the client reads more slowly than it writes.
        sys.stdout.flush()
        self.connection.setblocking(True)
        os.dup2(body.fileno(), 0)
        os.dup2(self.connection.fileno(), 1)
        body.close()

        # Everything the service answers with goes through sys.stdout;
        # count it, for the record of the request and the client's volume.
        counted = sys.stdout = _volume.Counted(sys.stdout)

        self.close_connection = True
        self.rfile = self.wfile = _Closed()
        os.close(self.connection.detach())
        self.server.socket.close()

        os.chdir(self.workdir)

        # Whoever asked may give up, or a proxy in front may time out, while
        # the query runs on.  Watch the connection, and stop the query if
        # the other end closes it.  The whole request has been read by now,
        # so anything "readable" on the connection is its end.
        gone = []
        # (Sync requests only:  an async request answers at once, and forks,
        # which a process with a second thread should not do.)
        if self.stop_abandoned and (path_info or '').startswith('/sync'):
            _watch_for_hangup(1, gone)

        _tap.Tap.pid = os.getpid()
        _tap.Tap.debugfname = '/tmp/tap_' + str(os.getpid()) + '.debug'
        try:
            _tap.Tap.form = _cgi.FieldStorage(max_num_fields=FORM_FIELDS)
        except ValueError:
            self._release()
            # The connection is this process's standard output by now.
            said = 'Too many fields in the request (at most %d).\n' % FORM_FIELDS
            sys.stdout.write('HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\n'
                             'Content-Length: %d\r\nConnection: close\r\n\r\n%s' % (len(said), said))
            sys.stdout.flush()
            return

        started = time.time()
        request_pid = os.getpid()
        seen = []

        class Request(_tap.Tap):
            # The service does its work in the constructor and usually leaves
            # it by SystemExit, so this is the only hold on the object.
            def __new__(cls, *args, **kwargs):
                seen.append(super().__new__(cls))
                return seen[0]

            # The message of a failed query is not kept on the object.
            def __printError__(self, fmt, errmsg, **kwargs):
                self.said = errmsg
                return super().__printError__(fmt, errmsg, **kwargs)

            def __writeAsyncError__(self, errmsg, *args, **kwargs):
                self.said = errmsg
                return super().__writeAsyncError__(errmsg, *args, **kwargs)

        status = 0
        try:
            Request()
        except SystemExit:
            pass
        except BaseException:
            status = 1
            if gone:                    # nobody to tell; the failed write is expected
                sys.stderr.write('request abandoned by its client; stopped\n')
            else:
                import traceback
                traceback.print_exc()

        # The query as the client wrote it; the service keeps only its translation.
        asked = ''
        try:
            form = _tap.Tap.form
            for key in form.keys():
                if key.lower() == 'query':
                    asked = form[key].value
        except Exception:
            pass

        try:
            log_query(self.command, path_info, client, started, status,
                      seen[0] if seen else None, os.getpid() != request_pid, asked,
                      counted.sent if os.getpid() == request_pid else None)
        except Exception as e:                      # a record, never a reason to fail
            sys.stderr.write(f'query record failed: {e!r}\n')

        # Leave here: the socket server's own clean-up expects a connection
        # this process no longer holds.
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except (OSError, ValueError):
            pass

        if os.getpid() == request_pid:
            self._sent(client, counted.sent)
            _forget_sync(seen[0] if seen else None, self.workdir)

        self._release()
        _tap.jobworker.finished()

        os._exit(status)


def _watch_for_hangup(fd, gone):
    """Start a thread that stops this process's queries when the peer of socket `fd` closes.

    Returns an event that ends the watch when set, or None if `fd` is not a socket.
    """
    import select
    import socket
    import stat
    import threading

    try:
        if not stat.S_ISSOCK(os.fstat(fd).st_mode):
            return
        sock = socket.socket(fileno=os.dup(fd))
    except OSError:
        return

    mine = os.getpid()
    stop = threading.Event()

    def watch():
        try:
            while os.getpid() == mine and not stop.is_set():
                readable, _, _ = select.select([sock], [], [], 1.0)
                if not readable:
                    continue
                try:
                    data = sock.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT)
                except BlockingIOError:
                    continue
                except OSError:
                    data = b''
                if data:
                    return              # more arrived:  not what was expected; leave the request alone
                gone.append(True)
                from TAP import duckdbsupport
                # The query may not have started yet;  keep at it until the process ends.
                while not stop.is_set():
                    duckdbsupport.abandon()
                    time.sleep(0.5)
        except Exception as e:          # a watch, never a reason to fail
            sys.stderr.write(f'connection watch ended: {e!r}\n')

    threading.Thread(target=watch, daemon=True).start()
    return stop


def _forget_sync(tap, workdir):
    """
    Remove what a sync query left in the work area:  its answer has gone to
    the client, and nothing can ask for it again.  It used to stay for the
    retention period, a result file for every query, on a file system that
    bills by the gigabyte (found in review, 23 November 2026).
    TAP_KEEP_SYNC=1 keeps them, to look at.
    """
    if tap is None or os.environ.get('TAP_KEEP_SYNC', '') not in ('', '0'):
        return
    try:
        if getattr(tap, 'tapcontext', '') != 'sync':
            return
        mine = os.path.realpath(getattr(tap, 'userWorkdir', '') or '')
        home = os.path.realpath(os.path.join(workdir, 'TAP'))
        if os.path.dirname(mine) == home and os.path.basename(mine).startswith('tap_') and os.path.isdir(mine):
            shutil.rmtree(mine, ignore_errors=True)
    except Exception as e:                          # tidying, never a reason to fail
        sys.stderr.write(f'sync work area not removed: {e!r}\n')


QUERY_RECORD = 'TAPQ'
VERSION_RECORD = 'TAPV'
QUERY_TEXT_LIMIT = 4000


def log_query(method, path_info, client, started, status, tap, is_job, asked='', sent=None):
    """
    One line on standard error for each request the service handled, as JSON
    after the word TAPQ, for whoever counts queries from the log:

        t       when the request arrived (seconds since 1970)
        ms      how long this process spent on it
        client  who asked (see Handler._client)
        m, path the request
        job     true for the line an async job's own process writes when the
                query is over; the request that submitted or started the job
                has a line of its own
        phase   COMPLETED or ERROR for a query that ran; absent otherwise
        rows    rows returned, for a query that completed
        id      the job, for a request about an async job
        fmt, q  the output format and the query as the client sent it (ADQL).
                For a job started by a later request, which carries no
                query, q is the service's translation of it
        err     the service's message, for a query that failed
        bytes   what the request was answered with, headers included; absent
                for the line an async job's own process writes
    """

    rec = {'t': round(started, 3), 'ms': int((time.time() - started) * 1000),
           'client': client, 'm': method, 'path': path_info or '/', 'job': bool(is_job)}

    if sent is not None:
        rec['bytes'] = int(sent)

    if status:
        rec['phase'] = 'ERROR'
        rec['err'] = 'internal error'

    if tap is not None:
        query = asked or getattr(tap, 'query', '') or ''
        phase = getattr(tap, 'phase', '') or ''

        if query:
            rec['q'] = ' '.join(str(query).split())[:QUERY_TEXT_LIMIT]
            rec['fmt'] = getattr(tap, 'format', '') or ''

        # Which job, for a request about one: what an administrator needs
        # to find it again or to stop it.
        jobid = getattr(tap, 'id', '') or getattr(tap, 'workspace', '') or ''     # a new job has no id yet, only its place
        if jobid and (path_info or '').startswith('/async'):
            rec['id'] = str(jobid)

        said = getattr(tap, 'said', None)

        if said is not None:
            rec['phase'] = 'ERROR'
            rec['err'] = ' '.join(str(said).split())[:500]
        elif phase == 'COMPLETED' and (query or is_job):
            rec['phase'] = phase
            rec['rows'] = int(getattr(tap, 'ntot', 0) or 0)

    sys.stderr.write(QUERY_RECORD + ' ' + json.dumps(rec, separators=(',', ':')) + '\n')
    sys.stderr.flush()


def log_versions():
    """One line at start-up naming what this server is made of (TAPV + JSON)."""

    import platform

    rec = {'python': platform.python_version()}

    try:
        from importlib import metadata
        rec['nexsciTAP'] = metadata.version('nexsciTAP')
    except Exception:
        # Run from a source tree: the version is in setup.py beside the package.
        try:
            setup = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'setup.py')
            with open(setup) as f:
                rec['nexsciTAP'] = re.search(r"version\s*=\s*['\"]([^'\"]+)", f.read()).group(1)
        except Exception:
            pass

    for name in ('duckdb', 'pyarrow', 'astropy'):
        try:
            rec[name] = __import__(name).__version__
        except Exception:
            pass

    if os.environ.get('TAP_COMMIT'):
        rec['commit'] = os.environ['TAP_COMMIT']

    sys.stderr.write(VERSION_RECORD + ' ' + json.dumps(rec, separators=(',', ':')) + '\n')
    sys.stderr.flush()


class _Closed:
    """Stand-in for the handler's socket files once the socket is handed over."""

    closed = True

    def flush(self):
        pass

    def close(self):
        pass


class Server(socketserver.ForkingMixIn, http.server.HTTPServer):

    allow_reuse_address = True
    request_queue_size = 64
    max_children = 16

    # Sent by this process itself, without a child, when every child is busy.
    FULL = ('<?xml version="1.0" encoding="UTF-8"?>\n'
            '<VOTABLE version="1.3" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">\n'
            '<RESOURCE type="results">\n<INFO name="QUERY_STATUS" value="ERROR">\n'
            'This service is as busy as it can be.  Try again shortly.\n'
            '</INFO>\n</RESOURCE>\n</VOTABLE>\n').encode()

    def collect_children(self, *, blocking=False):
        # The library's own waits, when every child is busy, for one to
        # end, and answers nobody meanwhile:  not the health check, and not
        # with a refusal.  Sixteen requests that were slow to finish stopped
        # the server so (found in review, 23 November 2026).  This only
        # gathers the children that have ended.
        if not self.active_children:
            return
        for pid in list(self.active_children):
            try:
                done, _ = os.waitpid(pid, 0 if blocking else os.WNOHANG)
            except ChildProcessError:
                done = pid
            except OSError:
                done = 0
            if done:
                self.active_children.discard(pid)

    def process_request(self, request, client_address):
        self.collect_children()
        if self.active_children and len(self.active_children) >= self.max_children:
            # No child to give it to.  Said from here, briefly, so that the
            # client hears why and this process goes straight on.
            try:
                request.settimeout(0.5)
                try:
                    request.recv(65536)         # the request, so that closing does not reset the answer
                except OSError:
                    pass
                request.sendall(b'HTTP/1.1 503 Service Unavailable\r\nRetry-After: 5\r\n'
                                b'Content-Type: application/x-votable+xml\r\nConnection: close\r\n'
                                b'Content-Length: ' + str(len(self.FULL)).encode() + b'\r\n\r\n' + self.FULL)
            except OSError:
                pass
            sys.stderr.write('every process busy: a request refused (503)\n')
            self.shutdown_request(request)
            return
        super().process_request(request, client_address)

    def service_actions(self):
        self.collect_children()

        # Background workers of async jobs outlive the request that started
        # them and are inherited by this process when it is a container's
        # first process.  Collect them so they do not pile up as zombies.
        try:
            while os.waitpid(-1, os.WNOHANG)[0]:
                pass
        except ChildProcessError:
            pass


def drop_privileges(user):
    pw = pwd.getpwnam(user)
    os.setgroups([g.gr_gid for g in grp.getgrall() if user in g.gr_mem])
    os.setgid(pw.pw_gid)
    os.setuid(pw.pw_uid)
    os.environ['HOME'] = os.environ.get('TAP_HOME', pw.pw_dir)
    os.environ['USER'] = user


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--conf', required=True, help='nexsciTAP configuration file')
    p.add_argument('--port', type=int, help='default: HTTP_PORT from the configuration')
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--user', help='switch to this user after binding the port')
    p.add_argument('--max-children', type=int, default=16,
                   help='requests handled at once; further ones wait (default 16)')
    args = p.parse_args()

    from configobj import ConfigObj

    web = ConfigObj(args.conf)['WEB']

    Handler.conf = os.path.abspath(args.conf)
    Handler.workdir = os.path.abspath(web['TAP_WORKDIR'])
    Handler.workurl = web.get('TAP_WORKURL', '/workspace')

    Handler.client_header = str(web.get('CLIENT_HEADER', '')).strip()
    Handler.client_limit = int(str(web.get('CLIENT_LIMIT', '') or 0).strip() or 0)
    unlimited = web.get('UNLIMITED_CLIENTS', '')
    if isinstance(unlimited, str):
        unlimited = unlimited.split(',')
    Handler.unlimited_clients = tuple(u.strip() for u in unlimited if u.strip())
    shared = web.get('SHARED_CLIENTS', '')
    if isinstance(shared, str):
        shared = shared.split(',')
    Handler.shared_clients = tuple(u.strip() for u in shared if u.strip())
    Handler.shared_client_limit = int(str(web.get('SHARED_CLIENT_LIMIT', '') or 0).strip() or 0)

    # Bytes one client may be sent in a day, written like 20GB (TAP/volume.py).
    from TAP.volume import parse_size
    Handler.volume_limit = parse_size(web.get('VOLUME_LIMIT', ''))
    global PROCESS_MEMORY
    PROCESS_MEMORY = parse_size(os.environ.get('TAP_PROCESS_MEMORY', ''))
    Handler.shared_volume_limit = parse_size(web.get('SHARED_VOLUME_LIMIT', ''))

    Handler.total_volume_limit = parse_size(web.get('TOTAL_VOLUME_LIMIT', ''))
    Handler.total_limit = int(str(web.get('TOTAL_LIMIT', '') or 0).strip() or 0)
    exempt = web.get('TOTAL_EXEMPT', 'health')
    if isinstance(exempt, str):
        exempt = exempt.split(',')
    Handler.total_exempt = tuple(u.strip() for u in exempt if u.strip())
    Handler.stop_abandoned = str(web.get('STOP_ABANDONED', '')).strip().lower() in ('on', 'yes', 'true', '1')

    Server.max_children = args.max_children

    port = args.port or int(web.get('HTTP_PORT', 8080))
    server = Server((args.host, port), Handler)

    if args.user:
        drop_privileges(args.user)

    # Places held by requests in progress (see Handler._admit); made after
    # the switch of user, so the request processes can write here.
    Handler.rundir = tempfile.mkdtemp(prefix='tap_clients_')
    Handler.dldir = tempfile.mkdtemp(prefix='tap_downloads_')

    # tap.py expects <workdir>/TAP to exist for VOSI and async requests.
    os.makedirs(os.path.join(Handler.workdir, 'TAP'), exist_ok=True)

    preload()

    # As a container's first process this one is sent SIGTERM to stop, and
    # the kernel delivers that to a first process only if it asked for it.
    # Without this the container sat out its whole stop timeout every time.
    def stop(signum, frame):
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, stop)

    log_versions()
    print(f'nexsciTAP fork server: http://{args.host}:{port}/TAP  (conf {Handler.conf})', flush=True)
    server.serve_forever(poll_interval=0.5)


if __name__ == '__main__':
    main()
