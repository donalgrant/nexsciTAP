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
import socketserver
import sys
import tempfile
import time
from urllib.parse import unquote, urlsplit

PRIVATE_FILES = ('upload_*.vot', 'worker', 'abort')

# Imported on demand by the service; loading them here keeps that cost out of
# the requests.  Missing optional packages are skipped.
PRELOAD = ('duckdb', 'numpy', 'pyarrow', 'astropy.units', 'astropy.io.votable',
           'astropy_healpix', 'lxml.etree', 'ipaddress', 'http.client', 'ssl')

_tap = None
_cgi = None


def preload():
    """Import the service and its lazy dependencies in the parent."""

    global _tap, _cgi

    # TAP.tap parses a CGI form when it is imported.  Give it an empty one
    # so the import neither reads standard input nor the command line.
    os.environ['REQUEST_METHOD'] = 'GET'
    os.environ['QUERY_STRING'] = ''

    import cgi

    from TAP import tap

    _tap, _cgi = tap, cgi

    for name in PRELOAD:
        try:
            __import__(name)
        except ImportError:
            pass


class Handler(http.server.BaseHTTPRequestHandler):

    conf = ''
    workdir = ''
    workurl = '/workspace'

    # Seconds a request may take to arrive; large uploads over slow links.
    timeout = 900

    client_header = ''
    client_limit = 0
    unlimited_clients = ()
    rundir = ''
    _place = None

    def _client(self):
        """Who is asking: the proxy's word for it if configured, else the peer address."""
        named = self.headers.get(self.client_header, '') if self.client_header else ''
        named = re.sub(r'[^0-9A-Za-z.:_-]', '', named.split(',')[-1])[:64]
        return named or self.client_address[0]

    def _admit(self, client):
        """Take a place for this request, unless the client has its share already."""
        if self.client_limit <= 0 or client in self.unlimited_clients:
            return True

        # One file per request in progress, named by its process.
        places = os.path.join(self.rundir, client.replace(':', '_'))
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
                try:                    # that request is over
                    os.remove(os.path.join(places, name))
                except OSError:
                    pass

        if held > self.client_limit:
            os.remove(mine)
            return False

        Handler._place = mine
        return True

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

        with open(full, 'rb') as f:
            self.send_response(200)
            self.send_header('Content-Type', ctype)
            self.send_header('Content-Length', str(os.fstat(f.fileno()).st_size))
            self.end_headers()
            if body:
                shutil.copyfileobj(f, self.wfile, 1 << 20)

    def _tap(self, path_info, query):
        """Run the CGI entry point in this (already forked) process."""

        try:
            length = int(self.headers.get('Content-Length', '0') or '0')
        except ValueError:
            return self.send_error(400)

        if length < 0:
            return self.send_error(400)

        client = self._client()
        if not self._admit(client):
            body = (f'Too many requests in progress from {client} '
                    f'(this service handles {self.client_limit} at once for one client).  Try again shortly.\n').encode()
            self.send_response(429)
            self.send_header('Retry-After', '5')
            self.send_header('Content-Type', 'text/plain')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
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

        self.close_connection = True
        self.rfile = self.wfile = _Closed()
        os.close(self.connection.detach())
        self.server.socket.close()

        os.chdir(self.workdir)

        _tap.Tap.pid = os.getpid()
        _tap.Tap.debugfname = '/tmp/tap_' + str(os.getpid()) + '.debug'
        _tap.Tap.form = _cgi.FieldStorage()

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
            import traceback
            traceback.print_exc()
            status = 1

        try:
            log_query(self.command, path_info, client, started, status,
                      seen[0] if seen else None, os.getpid() != request_pid)
        except Exception as e:                      # a record, never a reason to fail
            sys.stderr.write(f'query record failed: {e!r}\n')

        # Leave here: the socket server's own clean-up expects a connection
        # this process no longer holds.
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except (OSError, ValueError):
            pass

        self._release()
        _tap.jobworker.finished()

        os._exit(status)


QUERY_RECORD = 'TAPQ'
VERSION_RECORD = 'TAPV'
QUERY_TEXT_LIMIT = 4000


def log_query(method, path_info, client, started, status, tap, is_job):
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
        fmt, q  the output format and the query as the client sent it
        err     the service's message, for a query that failed
    """

    rec = {'t': round(started, 3), 'ms': int((time.time() - started) * 1000),
           'client': client, 'm': method, 'path': path_info or '/', 'job': bool(is_job)}

    if status:
        rec['phase'] = 'ERROR'
        rec['err'] = 'internal error'

    if tap is not None:
        query = getattr(tap, 'query', '') or ''
        phase = getattr(tap, 'phase', '') or ''

        if query:
            rec['q'] = ' '.join(str(query).split())[:QUERY_TEXT_LIMIT]
            rec['fmt'] = getattr(tap, 'format', '') or ''

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

    def service_actions(self):
        super().service_actions()

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
    Handler.workdir = web['TAP_WORKDIR']
    Handler.workurl = web.get('TAP_WORKURL', '/workspace')

    Handler.client_header = str(web.get('CLIENT_HEADER', '')).strip()
    Handler.client_limit = int(str(web.get('CLIENT_LIMIT', '') or 0).strip() or 0)
    unlimited = web.get('UNLIMITED_CLIENTS', '')
    if isinstance(unlimited, str):
        unlimited = unlimited.split(',')
    Handler.unlimited_clients = tuple(u.strip() for u in unlimited if u.strip())

    Server.max_children = args.max_children

    port = args.port or int(web.get('HTTP_PORT', 8080))
    server = Server((args.host, port), Handler)

    if args.user:
        drop_privileges(args.user)

    # Places held by requests in progress (see Handler._admit); made after
    # the switch of user, so the request processes can write here.
    Handler.rundir = tempfile.mkdtemp(prefix='tap_clients_')

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
