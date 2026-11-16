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

There is no TLS and no authentication:  run it behind a reverse proxy.
"""

import argparse
import fnmatch
import grp
import http.server
import mimetypes
import os
import pwd
import shutil
import socketserver
import sys
import tempfile
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

        if (not full.startswith(root + os.sep) or not os.path.isfile(full)
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

        status = 0
        try:
            _tap.Tap()
        except SystemExit:
            pass
        except BaseException:
            import traceback
            traceback.print_exc()
            status = 1

        # Leave here: the socket server's own clean-up expects a connection
        # this process no longer holds.
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except (OSError, ValueError):
            pass

        os._exit(status)


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

    Server.max_children = args.max_children

    port = args.port or int(web.get('HTTP_PORT', 8080))
    server = Server((args.host, port), Handler)

    if args.user:
        drop_privileges(args.user)

    # tap.py expects <workdir>/TAP to exist for VOSI and async requests.
    os.makedirs(os.path.join(Handler.workdir, 'TAP'), exist_ok=True)

    preload()

    print(f'nexsciTAP fork server: http://{args.host}:{port}/TAP  (conf {Handler.conf})', flush=True)
    server.serve_forever(poll_interval=0.5)


if __name__ == '__main__':
    main()
