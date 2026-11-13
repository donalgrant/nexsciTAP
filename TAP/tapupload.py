# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

"""
TAP table upload and positional cross-match (DuckDB backend).

A client sends  UPLOAD=<name>,param:<label>  with the table as a multipart
file part (or  UPLOAD=<name>,<http url>) and refers to it in ADQL as
TAP_UPLOAD.<name>.  The uploaded VOTable is kept in the job's work area and
loaded into a DuckDB temporary table when the query runs.

A positional join between a catalog and an uploaded table,

    CONTAINS(POINT('ICRS', t.ra, t.dec), CIRCLE('ICRS', ut.ra, ut.dec, r)) = 1

cannot be planned by the ADQL translator, which only handles cones around
constant positions, and would be a nested-loop join if passed through.  It
is rewritten here as an equality join on HEALPix cells:

  - a cell order k is chosen so cells are several times wider than r;
  - every uploaded row is repeated for its own cell and its 8 neighbors;
  - the catalog side joins on  (index >> shift) = cell , a hash join, and
    an exact great-circle test removes the candidates outside r.

Each catalog row lies in exactly one cell, so a pair can match only once.
Where the uploaded positions cover a small part of the sky, ranges on the
catalog's index column are added so sorted Parquet files (or a B-tree) can
skip everything else.
"""

import math
import os
import re

PLACEHOLDER = '(987654321 = 987654321)'
PLACEHOLDER_RE = re.compile(r'\(\s*987654321\s*=\s*987654321\s*\)')

MAX_UPLOAD_BYTES = 500 * 1024 * 1024
MAX_XMATCH_ORDER = 14          # cells about 13 arcsec across
MAX_PRUNE_RANGES = 300
PRUNE_ORDER = 8

_NAME = r'[A-Za-z_][A-Za-z0-9_]*'
_COL = r'(?:' + _NAME + r'\.)*"?' + _NAME + r'"?'
_NUM = r'[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?'

_GEOM = (r'CONTAINS\s*\(\s*POINT\s*\(\s*(?:\'[^\']*\'\s*,\s*)?(' + _COL + r')\s*,\s*(' + _COL + r')\s*\)\s*,'
         r'\s*CIRCLE\s*\(\s*(?:\'[^\']*\'\s*,\s*)?(' + _COL + r')\s*,\s*(' + _COL + r')\s*,\s*(' + _NUM + r')\s*\)\s*\)')

XMATCH_RE = re.compile(r'(?:' + _GEOM + r'\s*=\s*1|1\s*=\s*' + _GEOM + r')', re.IGNORECASE)

UPLOAD_REF_RE = re.compile(r'\bTAP_UPLOAD\.(' + _NAME + r')(?:\s+(?:AS\s+)?(' + _NAME + r'))?', re.IGNORECASE)

_NOT_ALIAS = {'where', 'on', 'join', 'inner', 'left', 'right', 'full', 'cross', 'natural',
              'group', 'order', 'having', 'union', 'limit', 'as', 'using'}


class UploadError(Exception):
    pass


# ---------------------------------------------------------------------------
# Receiving uploads
# ---------------------------------------------------------------------------

def upload_path(workdir, name):
    return os.path.join(workdir, 'upload_' + name.lower() + '.vot')


def save_uploads(form, workdir, allow_url=False):
    """
    Store the tables named by a request's UPLOAD parameter in `workdir`.

    `form` is the cgi.FieldStorage of the request.  Returns the list of
    upload table names saved (empty if the request has no UPLOAD).
    """

    spec = None
    files = {}

    for key in form.keys():
        if key.lower() == 'upload':
            spec = form[key].value
        files[key.lower()] = key

    if not spec:
        return []

    if isinstance(spec, bytes):
        spec = spec.decode()

    names = []

    for item in spec.split(';'):

        item = item.strip()
        if not item:
            continue

        if ',' not in item:
            raise UploadError('UPLOAD must have the form <table name>,<uri>.')

        name, uri = [s.strip() for s in item.split(',', 1)]

        if not re.fullmatch(_NAME, name):
            raise UploadError('Invalid upload table name: ' + name)

        if uri.lower().startswith('param:'):

            label = uri[len('param:'):].strip().lower()

            if label not in files:
                raise UploadError('UPLOAD refers to a missing request part: ' + uri)

            part = form[files[label]]
            data = part.file.read() if getattr(part, 'file', None) else part.value

            if isinstance(data, str):
                data = data.encode()

        elif uri.lower().startswith(('http://', 'https://')):

            if not allow_url:
                raise UploadError('This service accepts inline uploads only (UPLOAD=<name>,param:<part>).')

            data = fetch_public_url(uri)

        else:
            raise UploadError('Unsupported UPLOAD uri: ' + uri)

        if len(data) > MAX_UPLOAD_BYTES:
            raise UploadError('Uploaded table is larger than the service allows.')

        with open(upload_path(workdir, name), 'wb') as f:
            f.write(data)

        names.append(name.lower())

    return names


# ---------------------------------------------------------------------------
# Fetching an upload by URL
# ---------------------------------------------------------------------------
#
# UPLOAD=<name>,<url> makes the server fetch something a client chose.  Left
# unchecked, a client could aim that at the server's own network: other
# hosts behind the firewall, services on localhost, or a cloud provider's
# instance metadata address (169.254.169.254 on AWS), which can hand out the
# service's credentials.  So:
#
#   - only http and https, and no user:password@ in the URL;
#   - the host is resolved once and every address must be publicly routable;
#   - the connection is made to that resolved address, not to the name, so a
#     second DNS answer cannot redirect it after the check ("DNS rebinding");
#   - redirects are followed by hand, each target checked the same way.

MAX_URL_REDIRECTS = 3
URL_TIMEOUT = 60


def _public_addresses(host, port):
    """
    Resolve host and return its addresses, all of which must be public.
    Raises UploadError if any is private, loopback, link-local or reserved.
    """

    import ipaddress
    import socket

    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except Exception:
        raise UploadError('Cannot resolve upload host: ' + host)

    addresses = []

    for info in infos:

        text = info[4][0].split('%')[0]              # drop an IPv6 zone id
        ip = ipaddress.ip_address(text)

        if getattr(ip, 'ipv4_mapped', None) is not None:
            ip = ip.ipv4_mapped

        if not ip.is_global:
            raise UploadError('Upload URLs must point to a public address.')

        addresses.append(str(ip))

    if not addresses:
        raise UploadError('Cannot resolve upload host: ' + host)

    return addresses


def fetch_public_url(url, max_bytes=MAX_UPLOAD_BYTES):
    """Fetch url, refusing anything that is not on the public Internet."""

    import http.client
    import socket
    import ssl
    from urllib.parse import urljoin, urlsplit

    for _ in range(MAX_URL_REDIRECTS + 1):

        parts = urlsplit(url)
        scheme = parts.scheme.lower()

        if scheme not in ('http', 'https'):
            raise UploadError('Upload URLs must be http or https.')

        if parts.username is not None or parts.password is not None:
            raise UploadError('Upload URLs must not contain credentials.')

        host = parts.hostname

        if not host:
            raise UploadError('Upload URL has no host.')

        try:
            port = parts.port or (443 if scheme == 'https' else 80)
        except ValueError:
            raise UploadError('Upload URL has an invalid port.')

        address = _public_addresses(host, port)[0]

        # Connect to the address that was checked, keeping the original name
        # for the Host header and for TLS certificate verification.
        sock = socket.create_connection((address, port), timeout=URL_TIMEOUT)

        if scheme == 'https':
            sock = ssl.create_default_context().wrap_socket(sock, server_hostname=host)
            conn = http.client.HTTPSConnection(host, port, timeout=URL_TIMEOUT)
        else:
            conn = http.client.HTTPConnection(host, port, timeout=URL_TIMEOUT)

        conn.sock = sock

        try:
            path = parts.path or '/'
            if parts.query:
                path += '?' + parts.query

            conn.request('GET', path, headers={'User-Agent': 'nexsciTAP-upload', 'Accept': '*/*'})
            resp = conn.getresponse()

            if resp.status in (301, 302, 303, 307, 308):
                location = resp.getheader('Location')
                if not location:
                    raise UploadError('Upload URL redirected without a location.')
                url = urljoin(url, location)
                continue

            if resp.status != 200:
                raise UploadError('Upload URL returned HTTP status ' + str(resp.status) + '.')

            data = resp.read(max_bytes + 1)

            if len(data) > max_bytes:
                raise UploadError('Uploaded table is larger than the service allows.')

            return data

        finally:
            conn.close()

    raise UploadError('Upload URL redirected too many times.')


def find_uploads(workdir):
    """{table name: file path} for the uploads stored in a job's work area."""

    found = {}

    if workdir and os.path.isdir(workdir):
        for fn in os.listdir(workdir):
            m = re.fullmatch(r'upload_(' + _NAME + r')\.vot', fn)
            if m:
                found[m.group(1)] = os.path.join(workdir, fn)

    return found


# ---------------------------------------------------------------------------
# Loading uploads
# ---------------------------------------------------------------------------

def read_votable(path):
    """Read an uploaded VOTable into a dict of numpy columns."""

    import numpy as np
    from astropy.table import Table

    t = Table.read(path, format='votable')

    cols = {}

    for cname in t.colnames:

        col = t[cname]
        data = np.asarray(col.filled() if hasattr(col, 'filled') and col.dtype.kind in 'SUO' else col)
        mask = np.asarray(col.mask) if hasattr(col, 'mask') else None

        if data.dtype.kind in 'SUO':
            data = np.array([v.decode() if isinstance(v, bytes) else str(v) for v in data], dtype=object)
        elif data.dtype.byteorder not in ('=', '|'):
            data = data.astype(data.dtype.newbyteorder('='))

        if data.ndim != 1:
            continue                      # array-valued columns are not uploaded

        cols[cname] = (data, mask if mask is not None and np.ndim(mask) == 1 and mask.any() else None)

    return cols


def to_arrow(cols, take=None, extra=None):

    import pyarrow as pa

    arrays, names = [], []

    for cname, (data, mask) in cols.items():
        if take is not None:
            data = data[take]
            mask = mask[take] if mask is not None else None
        arrays.append(pa.array(data, mask=mask))
        names.append(cname)

    for cname, data in (extra or {}).items():
        arrays.append(pa.array(data))
        names.append(cname)

    return pa.table(arrays, names=names)


# ---------------------------------------------------------------------------
# Query rewriting
# ---------------------------------------------------------------------------

def _qualifier(col):
    col = col.replace('"', '')
    return col.rsplit('.', 1)[0].lower() if '.' in col else ''


def _bare(col):
    return col.replace('"', '').rsplit('.', 1)[-1]


def _merge(values):
    """Sorted unique integers -> list of inclusive (lo, hi) runs."""

    runs = []

    for v in values:
        v = int(v)
        if runs and v == runs[-1][1] + 1:
            runs[-1][1] = v
        else:
            runs.append([v, v])

    return runs


def _distance_sql(lon1, lat1, lon2, lat2):
    # operands parenthesized: a negative literal after "-" would read as a comment
    return ('(2*asin(sqrt(pow(sin(radians((' + lat2 + ')-(' + lat1 + '))/2),2) + '
            'cos(radians((' + lat1 + ')))*cos(radians((' + lat2 + ')))*'
            'pow(sin(radians((' + lon2 + ')-(' + lon1 + '))/2),2))))')


def prepare(adql, uploads, adqlparam):
    """
    Rewrite an ADQL query that uses uploaded tables, before translation.

    uploads    {name: VOTable path}
    adqlparam  the spatial index configuration (mode, level, colname,
               stored_level)

    Returns (adql, context).  Run the returned ADQL through the translator,
    then call finish(sql, context); context['tables'] holds the DuckDB
    temporary tables to create ({name: pyarrow table}).
    """

    context = {'tables': {}, 'xmatch_sql': None}

    aliases = {}                                     # alias (lower) -> upload name

    for m in UPLOAD_REF_RE.finditer(adql):

        name = m.group(1).lower()

        if name not in uploads:
            raise UploadError('Query refers to TAP_UPLOAD.' + m.group(1) + ' but no such table was uploaded.')

        alias = m.group(2)
        if alias and alias.lower() not in _NOT_ALIAS:
            aliases[alias.lower()] = name

        aliases['tap_upload.' + name] = name

    if not aliases:
        return adql, context

    used = set(aliases.values())
    data = {name: read_votable(uploads[name]) for name in used}
    target = {name: 'tap_upload__' + name for name in used}       # temp table per upload

    matches = list(XMATCH_RE.finditer(adql))

    if len(matches) > 1:
        raise UploadError('Only one positional cross-match condition per query is supported.')

    if matches:

        m = matches[0]
        g = [x for x in m.groups() if x is not None]
        plon, plat, clon, clat, radius = g[0], g[1], g[2], g[3], float(g[4])

        point_up = aliases.get(_qualifier(plon))
        circle_up = aliases.get(_qualifier(clon))

        if bool(point_up) == bool(circle_up):
            raise UploadError('A positional cross-match must compare one catalog table with one uploaded table.')

        if radius <= 0:
            raise UploadError('Cross-match radius must be positive.')

        if circle_up:
            upname, ulon, ulat, catlon, catlat = circle_up, clon, clat, plon, plat
        else:
            upname, ulon, ulat, catlon, catlat = point_up, plon, plat, clon, clat

        if str(adqlparam.get('mode', '')).upper() != 'HPX':
            raise UploadError('Cross-match needs a HEALPix (HPX) spatial index configuration.')

        stored = int(adqlparam.get('stored_level') or adqlparam['level'])
        order = int(max(0, min(MAX_XMATCH_ORDER, stored,
                               math.floor(math.log2(58.6 / (4.0 * radius))))))

        cols = data[upname]
        lookup = {c.lower(): c for c in cols}

        try:
            lon = cols[lookup[_bare(ulon).lower()]][0].astype(float)
            lat = cols[lookup[_bare(ulat).lower()]][0].astype(float)
        except KeyError:
            raise UploadError('Uploaded table has no column ' + _bare(ulon) + ' or ' + _bare(ulat) + '.')

        import astropy.units as u
        import numpy as np
        from astropy_healpix import lonlat_to_healpix, neighbours

        nside = 2 ** order
        good = np.flatnonzero(np.isfinite(lon) & np.isfinite(lat) & (np.abs(lat) <= 90))

        cell = np.asarray(lonlat_to_healpix(lon[good] * u.deg, lat[good] * u.deg, nside, order='nested'),
                          dtype=np.int64)
        near = np.asarray(neighbours(cell, nside, order='nested'), dtype=np.int64)      # (8, n)

        cells = np.vstack([cell[None, :], near]).ravel()
        rows = np.tile(good, 9)
        keep = cells >= 0
        cells, rows = cells[keep], rows[keep]

        cellcol = '_xm_cell'
        target[upname] = 'tap_upload__' + upname + '__cells'
        context['tables'][target[upname]] = to_arrow(cols, take=rows, extra={cellcol: cells})

        catq = catlon.replace('"', '').rsplit('.', 1)[0] + '.' if '.' in catlon else ''
        upq = ulon.replace('"', '').rsplit('.', 1)[0]
        if upq.lower().startswith('tap_upload.'):
            upq = target[upname]

        index = catq + adqlparam['colname']
        shift = 2 * (stored - order)

        cond = ('(' + index + ' >> ' + str(shift) + ') = ' + upq + '.' + cellcol + ' AND '
                + _distance_sql(catlon, catlat, upq + '.' + _bare(ulon), upq + '.' + _bare(ulat))
                + ' <= radians(' + repr(radius) + ')')

        porder = min(order, PRUNE_ORDER)
        runs = _merge(np.unique(cells >> (2 * (order - porder))))

        if 0 < len(runs) <= MAX_PRUNE_RANGES:
            s = 2 * (stored - porder)
            cond += ' AND (' + ' OR '.join(
                '(' + index + ' BETWEEN ' + str(lo << s) + ' AND ' + str(((hi + 1) << s) - 1) + ')'
                for lo, hi in runs) + ')'

        context['xmatch_sql'] = '(' + cond + ')'
        context['xmatch'] = {'upload': upname, 'order': order, 'radius': radius,
                             'rows': int(len(good)), 'prune_ranges': len(runs)}

        adql = adql[:m.start()] + PLACEHOLDER + adql[m.end():]

    for name in used:
        if target[name] not in context['tables']:
            context['tables'][target[name]] = to_arrow(data[name])

    adql = re.sub(r'\bTAP_UPLOAD\.(' + _NAME + r')',
                  lambda mm: target.get(mm.group(1).lower(), mm.group(0)), adql, flags=re.IGNORECASE)

    return adql, context


def finish(sql, context):
    """Put the cross-match join condition into the translated SQL."""

    if context.get('xmatch_sql'):

        if not PLACEHOLDER_RE.search(sql):
            raise UploadError('Internal error: cross-match condition was lost in translation.')

        sql = PLACEHOLDER_RE.sub(lambda m: context['xmatch_sql'], sql, count=1)

    return sql
