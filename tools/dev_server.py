#!/usr/bin/env python
# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

"""
Run nexsciTAP as a local web service without Apache, for development.

    dev_server.py --conf TAP.conf [--port 8080] [--host 127.0.0.1]

The TAP endpoints appear under  http://<host>:<port>/TAP  (sync, async,
tables, capabilities, availability), and files in the configured
TAP_WORKDIR are served under TAP_WORKURL so async results can be fetched.

Each request runs the same CGI entry point a production deployment uses
(TAP.tap.Tap) in a subprocess, and its raw output is passed through, since
the service writes its own HTTP status line ("nph" CGI).  This mirrors the
handler the test suite uses in tests/conftest.py.

Not for production: there is no TLS, no authentication, and no process
limit.  It binds to localhost unless told otherwise.
"""

import argparse
import http.server
import mimetypes
import os
import subprocess
import sys
from urllib.parse import unquote, urlsplit

from configobj import ConfigObj

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENTRY = 'from TAP import tap\ntap.Tap()\n'


class Handler(http.server.BaseHTTPRequestHandler):

    conf = ''
    workdir = ''
    workurl = '/workspace'

    def do_GET(self):
        self._dispatch()

    def do_POST(self):
        self._dispatch()

    def do_DELETE(self):
        self._dispatch()

    def do_HEAD(self):
        # Clients (Firefly among them) probe a URL with HEAD before fetching it.
        path = urlsplit(self.path).path
        if path.startswith(self.workurl + '/'):
            return self._static(unquote(path[len(self.workurl) + 1:]), body=False)
        self.send_error(405)

    def _dispatch(self):
        parts = urlsplit(self.path)
        path = parts.path

        if path.startswith(self.workurl + '/'):
            return self._static(unquote(path[len(self.workurl) + 1:]))

        if path == '/TAP' or path.startswith('/TAP/'):
            return self._cgi(path[len('/TAP'):], parts.query)

        self.send_error(404)

    def _static(self, rel, body=True):
        root = os.path.realpath(self.workdir)
        full = os.path.realpath(os.path.join(root, rel))

        if not full.startswith(root + os.sep) or not os.path.isfile(full):
            return self.send_error(404)

        ctype = mimetypes.guess_type(full)[0] or 'application/octet-stream'
        if full.endswith('.xml'):
            ctype = 'text/xml'

        with open(full, 'rb') as f:
            data = f.read()

        self.send_response(200)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        if body:
            self.wfile.write(data)

    def _cgi(self, path_info, query):
        length = int(self.headers.get('Content-Length', '0') or '0')
        body = self.rfile.read(length) if length else b''

        env = os.environ.copy()
        env.update({
            'GATEWAY_INTERFACE': 'CGI/1.1',
            'SERVER_PROTOCOL': 'HTTP/1.1',
            'SERVER_SOFTWARE': 'nexsciTAP-dev-server/1.0',
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
        env['PYTHONPATH'] = os.pathsep.join(
            [REPO] + ([env['PYTHONPATH']] if env.get('PYTHONPATH') else []))

        proc = subprocess.Popen([sys.executable, '-c', ENTRY], stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                env=env, cwd=self.workdir)
        out, err = proc.communicate(input=body)

        if err:
            sys.stderr.write(err.decode('utf-8', 'replace'))

        try:
            self.wfile.write(out)
            self.wfile.flush()
        except BrokenPipeError:
            pass


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--conf', required=True, help='nexsciTAP configuration file')
    p.add_argument('--port', type=int, help='default: HTTP_PORT from the configuration')
    p.add_argument('--host', default='127.0.0.1')
    args = p.parse_args()

    web = ConfigObj(args.conf)['WEB']

    Handler.conf = os.path.abspath(args.conf)
    Handler.workdir = web['TAP_WORKDIR']
    Handler.workurl = web.get('TAP_WORKURL', '/workspace')

    # tap.py expects <workdir>/TAP to exist for VOSI and async requests.
    os.makedirs(os.path.join(Handler.workdir, 'TAP'), exist_ok=True)

    port = args.port or int(web.get('HTTP_PORT', 8080))
    server = http.server.ThreadingHTTPServer((args.host, port), Handler)
    print(f'nexsciTAP dev server: http://{args.host}:{port}/TAP  (conf {Handler.conf})', flush=True)
    server.serve_forever()


if __name__ == '__main__':
    main()
