"""TAP_UPLOAD handling and the positional cross-match rewrite."""
from __future__ import annotations

import math

import pytest

from TAP import tapupload

XMATCH = ("select t.name, ut.id from stars as t, TAP_UPLOAD.mine as ut "
          "where contains(point('ICRS', t.ra, t.dec), circle('ICRS', ut.ra, ut.dec, 0.001)) = 1")


def test_xmatch_pattern_recognises_both_spellings():
    assert tapupload.XMATCH_RE.search(XMATCH)
    assert tapupload.XMATCH_RE.search(
        "where 1=CONTAINS(POINT(t.ra,t.dec),CIRCLE(ut.ra,ut.dec,2.5e-4))")


def test_xmatch_pattern_leaves_constant_cones_alone():
    cone = "where contains(point('icrs', ra, dec), circle('icrs', 52.9, -28.1, 0.01)) = 1"
    assert tapupload.XMATCH_RE.search(cone) is None


def test_query_without_upload_reference_is_untouched():
    adql, ctx = tapupload.prepare('select ra from stars', {'mine': '/nonexistent'}, {})
    assert adql == 'select ra from stars'
    assert ctx['tables'] == {}


def test_unknown_upload_table_is_rejected():
    with pytest.raises(tapupload.UploadError):
        tapupload.prepare('select * from TAP_UPLOAD.other', {'mine': '/nonexistent'}, {})


def test_merge_runs():
    assert tapupload._merge([1, 2, 3, 7, 9, 10]) == [[1, 3], [7, 7], [9, 10]]


class _Part:
    def __init__(self, value, file=None):
        self.value = value
        self.file = file


class _Form(dict):
    pass


def test_save_uploads_stores_inline_part(tmp_path):
    import io

    form = _Form(UPLOAD=_Part('Mine,param:f1'), f1=_Part(b'', io.BytesIO(b'<VOTABLE/>')))
    assert tapupload.save_uploads(form, str(tmp_path)) == ['mine']
    assert tapupload.find_uploads(str(tmp_path)) == {'mine': str(tmp_path / 'upload_mine.vot')}


@pytest.mark.parametrize('spec', ['../../etc/passwd,param:f1', 'a b,param:f1', 'mine', 'mine,file:///etc/passwd'])
def test_save_uploads_rejects_bad_specs(tmp_path, spec):
    import io

    form = _Form(UPLOAD=_Part(spec), f1=_Part(b'', io.BytesIO(b'x')))
    with pytest.raises(tapupload.UploadError):
        tapupload.save_uploads(form, str(tmp_path))


def test_save_uploads_refuses_urls_unless_allowed(tmp_path):
    form = _Form(UPLOAD=_Part('mine,http://127.0.0.1:1/x.vot'))
    with pytest.raises(tapupload.UploadError):
        tapupload.save_uploads(form, str(tmp_path))


@pytest.fixture
def sky(tmp_path):
    """A random catalog with an order-29 HEALPix index, and an upload of nearby positions."""
    duckdb = pytest.importorskip('duckdb')
    pytest.importorskip('pyarrow')
    ah = pytest.importorskip('astropy_healpix')
    np = pytest.importorskip('numpy')
    import astropy.units as u
    import pyarrow as pa
    from astropy.table import Table

    rng = np.random.default_rng(3)
    n = 20000
    # a patch that straddles RA 0 and reaches the pole, the awkward places
    ra = (rng.uniform(-3, 3, n)) % 360
    dec = rng.uniform(84, 90, n)
    hpx = np.asarray(ah.lonlat_to_healpix(ra * u.deg, dec * u.deg, 2 ** 29, order='nested'), dtype=np.int64)

    con = duckdb.connect()
    con.register('cat_src', pa.table({'name': np.arange(n), 'ra': ra, 'dec': dec, 'hpx29': hpx}))
    con.execute('create table stars as select * from cat_src')

    pick = rng.choice(n, 500, replace=False)
    up_ra = (ra[pick] + rng.normal(0, 0.0004, 500) / np.cos(np.radians(dec[pick]))) % 360
    up_dec = np.clip(dec[pick] + rng.normal(0, 0.0004, 500), -90, 90)
    path = tmp_path / 'upload_mine.vot'
    Table({'id': np.arange(500), 'ra': up_ra, 'dec': up_dec}).write(path, format='votable')

    return con, {'mine': str(path)}


@pytest.mark.parametrize('radius', [0.0002, 0.001, 0.02])
def test_cell_join_matches_brute_force(sky, radius):
    con, uploads = sky
    adqlparam = {'mode': 'HPX', 'level': 14, 'stored_level': 29, 'colname': 'hpx29'}

    adql, ctx = tapupload.prepare(XMATCH.replace('0.001', repr(radius)), uploads, adqlparam)

    assert tapupload.PLACEHOLDER in adql
    assert 'tap_upload__mine__cells' in adql

    sql = tapupload.finish(adql, ctx)             # no translator here: the rest is plain SQL
    for name, table in ctx['tables'].items():
        con.register(name, table)

    fast = set(con.execute(sql).fetchall())

    con.register('up', tapupload.to_arrow(tapupload.read_votable(uploads['mine'])))
    dist = tapupload._distance_sql('t.ra', 't.dec', 'u.ra', 'u.dec')
    brute = set(con.execute(
        f'select t.name, u.id from stars t, up u where {dist} <= radians({radius!r})').fetchall())

    assert len(brute) > 0
    assert fast == brute


def test_cell_order_follows_radius(sky):
    _, uploads = sky
    adqlparam = {'mode': 'HPX', 'level': 14, 'stored_level': 29, 'colname': 'hpx29'}

    _, small = tapupload.prepare(XMATCH, uploads, adqlparam)
    _, large = tapupload.prepare(XMATCH.replace('0.001', '1.0'), uploads, adqlparam)

    assert small['xmatch']['order'] == min(14, math.floor(math.log2(58.6 / 0.004)))
    assert large['xmatch']['order'] < small['xmatch']['order']


def test_two_catalog_tables_are_not_a_supported_cross_match(sky):
    _, uploads = sky
    q = ("select a.name from stars a, stars b, TAP_UPLOAD.mine ut "
         "where contains(point('ICRS', a.ra, a.dec), circle('ICRS', b.ra, b.dec, 0.001)) = 1")
    with pytest.raises(tapupload.UploadError):
        tapupload.prepare(q, uploads, {'mode': 'HPX', 'level': 14, 'stored_level': 29, 'colname': 'hpx29'})


# ---------------------------------------------------------------------------
# Upload by URL must not reach private addresses
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('url', [
    'http://127.0.0.1/x.vot',
    'http://localhost/x.vot',
    'http://169.254.169.254/latest/meta-data/iam/security-credentials/',   # AWS instance metadata
    'http://10.0.0.5/x.vot',
    'http://192.168.1.10:8080/x.vot',
    'http://172.16.0.1/x.vot',
    'http://[::1]/x.vot',
    'http://[::ffff:127.0.0.1]/x.vot',          # IPv4-mapped loopback
    'http://[fd00::1]/x.vot',                   # IPv6 unique local
    'http://0.0.0.0/x.vot',
    'http://2130706433/x.vot',                  # 127.0.0.1 written as one integer
])
def test_url_fetch_refuses_non_public_addresses(url):
    with pytest.raises(tapupload.UploadError):
        tapupload.fetch_public_url(url)


@pytest.mark.parametrize('url', [
    'file:///etc/passwd',
    'ftp://example.com/x.vot',
    'gopher://example.com/',
    'http://user:secret@example.com/x.vot',
    'http:///x.vot',
])
def test_url_fetch_refuses_bad_urls(url):
    with pytest.raises(tapupload.UploadError):
        tapupload.fetch_public_url(url)


@pytest.fixture
def local_site(monkeypatch):
    """A local web server that the address check is told to treat as public.

    Only this server's own host and port are exempted, so a redirect from it
    to anywhere else still meets the real check.
    """
    import http.server
    import threading

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/table.vot':
                body = b'<VOTABLE/>'
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif self.path == '/big':
                self.send_response(200)
                self.send_header('Content-Length', '5000')
                self.end_headers()
                self.wfile.write(b'x' * 5000)
            elif self.path == '/to-metadata':
                self.send_response(302)
                self.send_header('Location', 'http://169.254.169.254/latest/meta-data/')
                self.end_headers()
            elif self.path == '/to-table':
                self.send_response(302)
                self.send_header('Location', '/table.vot')
                self.end_headers()
            elif self.path == '/loop':
                self.send_response(302)
                self.send_header('Location', '/loop')
                self.end_headers()
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, *args):
            return

    server = http.server.HTTPServer(('127.0.0.1', 0), Handler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    real = tapupload._public_addresses

    def exempt_this_server(host, p):
        if host == '127.0.0.1' and p == port:
            return ['127.0.0.1']
        return real(host, p)

    monkeypatch.setattr(tapupload, '_public_addresses', exempt_this_server)

    yield f'http://127.0.0.1:{port}'

    server.shutdown()


def test_url_fetch_returns_the_table(local_site):
    assert tapupload.fetch_public_url(local_site + '/table.vot') == b'<VOTABLE/>'


def test_url_fetch_follows_a_redirect_to_an_allowed_address(local_site):
    assert tapupload.fetch_public_url(local_site + '/to-table') == b'<VOTABLE/>'


def test_url_fetch_refuses_a_redirect_to_the_metadata_address(local_site):
    with pytest.raises(tapupload.UploadError):
        tapupload.fetch_public_url(local_site + '/to-metadata')


def test_url_fetch_stops_redirect_loops(local_site):
    with pytest.raises(tapupload.UploadError):
        tapupload.fetch_public_url(local_site + '/loop')


def test_url_fetch_enforces_the_size_cap(local_site):
    with pytest.raises(tapupload.UploadError):
        tapupload.fetch_public_url(local_site + '/big', max_bytes=1000)


def test_url_fetch_reports_http_errors(local_site):
    with pytest.raises(tapupload.UploadError):
        tapupload.fetch_public_url(local_site + '/missing')
