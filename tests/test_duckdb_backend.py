"""DuckDB backend: Parquet files served through a DuckDB catalog file.

The unit tests need nothing beyond the TAP package.  The end-to-end tests
build a tiny Parquet catalog and are skipped when duckdb is not installed.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

from TAP import duckdbsupport

REPO = Path(__file__).parent.parent


def test_adql_dialect_maps_duckdb_to_limit_form():
    assert duckdbsupport.adql_dialect('duckdb') == 'sqlite3'
    assert duckdbsupport.adql_dialect('DuckDB') == 'sqlite3'
    assert duckdbsupport.adql_dialect('oracle') == 'oracle'


@pytest.mark.parametrize('duck,expected', [
    ('BIGINT', 'bigint'), ('INTEGER', 'integer'), ('SMALLINT', 'integer'),
    ('DOUBLE', 'double'), ('FLOAT', 'double'), ('DECIMAL(10,2)', 'double'),
    ('VARCHAR', 'varchar'), ('DATE', 'date'), ('TIMESTAMP', 'timestamp'),
])
def test_result_datatype(duck, expected):
    assert duckdbsupport.result_datatype(duck) == expected


def test_rescale_index_constraint_widens_cells_to_stored_level():
    sql = "select ra from t where (x>=0.5) AND ((hpx BETWEEN 8 AND 9) OR (hpx = 12))"
    out = duckdbsupport.rescale_index_constraint(sql, 'hpx', 14, 15)
    # one level deeper: each cell c covers 4c .. 4c+3
    assert '(hpx BETWEEN 32 AND 39)' in out
    assert '(hpx BETWEEN 48 AND 51)' in out
    assert '(x>=0.5)' in out


def test_rescale_index_constraint_is_a_noop_without_stored_level():
    sql = "select ra from t where (hpx BETWEEN 8 AND 9)"
    assert duckdbsupport.rescale_index_constraint(sql, 'hpx', 14, None) == sql
    assert duckdbsupport.rescale_index_constraint(sql, 'hpx', 14, 14) == sql


def test_rescale_index_constraint_rejects_shallower_stored_level():
    with pytest.raises(Exception):
        duckdbsupport.rescale_index_constraint("(hpx = 1)", 'hpx', 14, 10)


@pytest.fixture
def parquet_catalog(tmp_path):
    duckdb = pytest.importorskip('duckdb')

    pq = tmp_path / 'stars.parquet'
    con = duckdb.connect()
    con.execute(f"""COPY (SELECT * FROM (VALUES
            ('a', 10.0, -20.0, 9007199254740993, 1.5),
            ('b', 11.0, -21.0, -529002177281003050, 2.5),
            ('c', 12.0, -22.0, 3, 3.5)) t(name, ra, dec, objid, mag))
        TO '{pq}' (FORMAT parquet)""")
    con.close()

    db = tmp_path / 'catalog.duckdb'
    subprocess.run([sys.executable, str(REPO / 'tools' / 'make_duckdb_catalog.py'),
                    '--db', str(db), '--table', f'stars={pq}'], check=True, capture_output=True)

    conf = tmp_path / 'TAP.conf'
    conf.write_text(f"""
[WEB]
    TAP_WORKDIR={tmp_path}
    TAP_WORKURL=/workspace
    HTTP_URL=http://localhost
    HTTP_PORT=8080
    CGI_PGM=TAP
[DBMS]
    DBMS=duckdb
    DB={db}
    ALLOWED_DIRECTORIES={tmp_path}/
[SPTIND]
    MODE=HPX
    LEVEL=14
    XCOL=aiab_x
    YCOL=aiab_y
    ZCOL=aiab_z
    COLNAME=hpx14
    ENCODING=BASE10
""")
    return conf


def test_config_reads_duckdb_section(parquet_catalog):
    from TAP.configparam import configParam

    cfg = configParam(str(parquet_catalog))
    assert cfg.dbms == 'duckdb'
    assert cfg.connectInfo['tap_schema'] == 'tap_schema'
    assert cfg.connectInfo['db'].endswith('catalog.duckdb')


def test_query_returns_rows_with_64_bit_integers_intact(parquet_catalog, tmp_path):
    from TAP.configparam import configParam
    from TAP.tapquery import tapQuery

    cfg = configParam(str(parquet_catalog))
    out = tmp_path / 'result.csv'
    q = tapQuery(connectInfo=cfg.connectInfo, workdir=str(tmp_path),
                 query='select name, objid, mag from stars order by name',
                 filename=str(out), format='csv', maxrec=-1,
                 racol=cfg.racol, deccol=cfg.deccol)
    assert q.ntot == 3
    lines = out.read_text().strip().splitlines()
    assert lines[0] == 'name,objid,mag'
    assert lines[1].split(',')[1] == '9007199254740993'
    assert lines[2].split(',')[1] == '-529002177281003050'


def test_query_rejects_table_not_in_tap_schema(parquet_catalog, tmp_path):
    from TAP.configparam import configParam
    from TAP.tapquery import tapQuery

    cfg = configParam(str(parquet_catalog))
    with pytest.raises(Exception):
        tapQuery(connectInfo=cfg.connectInfo, workdir=str(tmp_path),
                 query='select * from duckdb_settings', filename=str(tmp_path / 'x.csv'),
                 format='csv', maxrec=-1, racol=cfg.racol, deccol=cfg.deccol)


def _fails(cfg, tmp_path, sql):
    from TAP.tapquery import tapQuery

    with pytest.raises(Exception):
        tapQuery(connectInfo=cfg.connectInfo, workdir=str(tmp_path), query=sql,
                 filename=str(tmp_path / 'leak.csv'), format='csv', maxrec=-1,
                 racol=cfg.racol, deccol=cfg.deccol)


def test_queries_cannot_read_files_outside_the_catalog(parquet_catalog, tmp_path):
    """DuckDB reads any path a query names unless external access is off.

    The second query hides the read in a subquery, where the TAP_SCHEMA
    table check does not look.
    """
    from TAP.configparam import configParam

    secret = tmp_path.parent / (tmp_path.name + '_outside.csv')
    secret.write_text('k,v\nsecret,1\n')

    cfg = configParam(str(parquet_catalog))

    _fails(cfg, tmp_path, f"select * from read_csv('{secret}')")
    _fails(cfg, tmp_path,
           f"select name from stars where mag in (select v from read_csv('{secret}'))")


def test_queries_cannot_reenable_external_access(parquet_catalog, tmp_path):
    from TAP.configparam import configParam

    cfg = configParam(str(parquet_catalog))
    conn = duckdbsupport.connect(cfg.connectInfo)
    cur = conn.cursor()
    with pytest.raises(Exception):
        cur.execute('SET enable_external_access = true')


def test_query_timeout_cancels_a_running_query(tmp_path):
    """QUERY_TIMEOUT interrupts DuckDB mid-query and refuses further work."""
    duckdb = pytest.importorskip('duckdb')
    import time

    from TAP import duckdbsupport

    db = tmp_path / 'empty.duckdb'
    duckdb.connect(str(db)).close()

    conn = duckdbsupport.connect({'db': str(db), 'query_timeout': '0.5'})
    cur = conn.cursor()

    start = time.time()
    with pytest.raises(duckdbsupport.QueryTimeout, match='time limit of 0.5 seconds'):
        cur.execute('select count(*) from range(100000000000) a, range(1000) b '
                    'where a.range % 7 = b.range')
        cur.fetchall()
    assert time.time() - start < 10

    with pytest.raises(duckdbsupport.QueryTimeout):
        conn.cursor().execute('select 1')


def test_no_query_timeout_by_default(parquet_catalog):
    from TAP import duckdbsupport
    from TAP.configparam import configParam

    cfg = configParam(str(parquet_catalog))
    assert cfg.connectInfo['query_timeout'] is None

    cur = duckdbsupport.connect(cfg.connectInfo).cursor()
    cur.execute('select count(*) from stars')
    assert cur.fetchone() == (3,)


def test_config_reads_query_timeout(parquet_catalog):
    from TAP.configparam import configParam

    text = parquet_catalog.read_text().replace('DBMS=duckdb', 'DBMS=duckdb\n    QUERY_TIMEOUT=45', 1)
    assert 'QUERY_TIMEOUT=45' in text
    parquet_catalog.write_text(text)

    assert configParam(str(parquet_catalog)).connectInfo['query_timeout'] == '45'


def test_http_hosts_lets_a_listed_host_name_the_service(parquet_catalog, monkeypatch):
    """URLs follow the request's Host header only when HTTP_HOSTS lists it."""
    from TAP.configparam import configParam

    text = parquet_catalog.read_text()
    assert 'HTTP_URL=' in text
    public = configParam(str(parquet_catalog)).httpurl

    parquet_catalog.write_text(text.replace('[WEB]', '[WEB]\n    HTTP_HOSTS=localhost, Inside:8081', 1))

    monkeypatch.setenv('HTTP_HOST', 'localhost')
    assert configParam(str(parquet_catalog)).httpurl == 'http://localhost'

    monkeypatch.setenv('HTTP_HOST', 'inside:8081')
    assert configParam(str(parquet_catalog)).httpurl == 'http://inside:8081'

    monkeypatch.setenv('HTTP_HOST', 'evil.example.com')
    assert configParam(str(parquet_catalog)).httpurl == public

    monkeypatch.delenv('HTTP_HOST')
    assert configParam(str(parquet_catalog)).httpurl == public


def _make_parquet(duckdb, path, columns='a.ra, a.dec, a.hpx29'):
    # The index must be the true one for each position: the catalog tool
    # checks it, and leaves out a folder whose index is wrong.
    from astropy import units as u
    from astropy_healpix import lonlat_to_healpix
    rows = [(10.0, -20.0), (11.0, -21.0)]
    values = ', '.join(f"({ra}, {dec}, {int(lonlat_to_healpix(ra * u.deg, dec * u.deg, 2 ** 29, order='nested'))})"
                       for ra, dec in rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"COPY (SELECT {columns} FROM (VALUES {values}) a(ra, dec, hpx29)) "
                f"TO '{path}' (FORMAT parquet)")
    con.close()


def _catalog(tmp_path, *extra):
    return subprocess.run([sys.executable, str(REPO / 'tools' / 'make_duckdb_catalog.py'),
                           '--db', str(tmp_path / 'c.duckdb'), '--discover', str(tmp_path / 'cat'),
                           '--require-column', 'hpx29', *extra], capture_output=True, text=True)


def _tables(duckdb, tmp_path):
    con = duckdb.connect(str(tmp_path / 'c.duckdb'), read_only=True)
    try:
        return sorted(r[0] for r in con.execute(
            "SELECT table_name FROM tap_schema.tables WHERE schema_name = 'main'").fetchall())
    finally:
        con.close()


def test_discover_serves_good_folders_and_skips_bad_ones(tmp_path):
    duckdb = pytest.importorskip('duckdb')
    cat = tmp_path / 'cat'

    _make_parquet(duckdb, cat / 'good_one' / 'part-00.parquet')
    _make_parquet(duckdb, cat / 'renamed_folder' / 'part-00.parquet')
    # A catalog's own x, y or z is its own business:  a redshift named z, for one.
    _make_parquet(duckdb, cat / 'own_z' / 'part-00.parquet', columns='a.ra, a.dec, a.hpx29, 2.5 AS z')
    _make_parquet(duckdb, cat / 'no_index' / 'part-00.parquet', columns='a.ra, a.dec')
    _make_parquet(duckdb, cat / 'bad-name' / 'part-00.parquet')
    _make_parquet(duckdb, cat / 'has_xyz' / 'part-00.parquet', columns='a.ra, a.dec, a.hpx29, 1 AS aiab_x')
    (cat / 'empty').mkdir()

    run = _catalog(tmp_path, '--table', f"nice={cat}/renamed_folder/*.parquet")
    assert run.returncode == 0, run.stderr

    # The explicit name wins for its folder; the unusable folders are left out and said so.
    assert _tables(duckdb, tmp_path) == ['good_one', 'nice', 'own_z']
    for folder in ('no_index', 'bad-name', 'has_xyz'):
        assert folder in run.stderr

    con = duckdb.connect(str(tmp_path / 'c.duckdb'), read_only=True)
    assert con.execute('SELECT count(*), min(aiab_x) IS NOT NULL FROM good_one').fetchone() == (2, True)
    assert con.execute('SELECT min(z), max(abs(aiab_z)) <= 1 FROM own_z').fetchone() == (2.5, True)
    con.close()


def test_discover_picks_up_and_drops_folders_without_a_restart(tmp_path):
    duckdb = pytest.importorskip('duckdb')
    import shutil
    cat = tmp_path / 'cat'
    db = tmp_path / 'c.duckdb'

    _make_parquet(duckdb, cat / 'first' / 'part-00.parquet')
    assert _catalog(tmp_path).returncode == 0
    assert _tables(duckdb, tmp_path) == ['first']

    # Nothing changed: the file is left alone.
    stamp = db.stat().st_mtime_ns
    run = _catalog(tmp_path, '--if-changed')
    assert run.returncode == 0 and db.stat().st_mtime_ns == stamp

    # A reader that already has the catalog open keeps working while it is replaced.
    reader = duckdb.connect(str(db), read_only=True)
    _make_parquet(duckdb, cat / 'second' / 'part-00.parquet')
    assert _catalog(tmp_path, '--if-changed').returncode == 0
    assert reader.execute('SELECT count(*) FROM first').fetchone() == (2,)
    reader.close()
    assert _tables(duckdb, tmp_path) == ['first', 'second']

    shutil.rmtree(cat / 'first')
    assert _catalog(tmp_path, '--if-changed').returncode == 0
    assert _tables(duckdb, tmp_path) == ['second']
    assert not (tmp_path / 'c.duckdb.new').exists()


def test_http_hosts_entry_can_carry_its_own_scheme(parquet_catalog, monkeypatch):
    """Public address over HTTPS, an inside client over plain HTTP."""
    from TAP.configparam import configParam

    text = parquet_catalog.read_text()
    text = text.replace('HTTP_URL=http://localhost', 'HTTP_URL=https://archive.example.org')
    text = text.replace('HTTP_PORT=8080', 'HTTP_PORT=443')
    parquet_catalog.write_text(text.replace('[WEB]', '[WEB]\n    HTTP_HOSTS=http://localhost, other.example.org', 1))

    monkeypatch.delenv('HTTP_HOST', raising=False)
    assert configParam(str(parquet_catalog)).httpurl == 'https://archive.example.org'

    monkeypatch.setenv('HTTP_HOST', 'localhost')
    assert configParam(str(parquet_catalog)).httpurl == 'http://localhost'

    monkeypatch.setenv('HTTP_HOST', 'other.example.org')
    assert configParam(str(parquet_catalog)).httpurl == 'https://other.example.org'


def test_discover_notices_a_file_replaced_under_the_same_name(tmp_path):
    duckdb = pytest.importorskip('duckdb')
    cat = tmp_path / 'cat'
    f = cat / 'first' / 'part-00.parquet'

    def datatype(column):
        con = duckdb.connect(str(tmp_path / 'c.duckdb'), read_only=True)
        try:
            return con.execute("SELECT datatype FROM tap_schema.columns WHERE table_name = 'first' "
                               "AND column_name = ?", [column]).fetchone()
        finally:
            con.close()

    _make_parquet(duckdb, f, columns="a.ra, a.dec, a.hpx29, 'text' AS flux")
    assert _catalog(tmp_path).returncode == 0
    assert datatype('flux') == ('char',)

    f.unlink()
    _make_parquet(duckdb, f, columns='a.ra, a.dec, a.hpx29, 1.5 AS flux')
    assert _catalog(tmp_path, '--if-changed').returncode == 0
    assert datatype('flux') == ('double',)


# ---- BOX and POLYGON: the index constraint is rebuilt from the edge conditions ----

def _triangle_sql(index_clause):
    # The octant x, y, z >= 0, written the way the translator writes a polygon's edges.
    return ('SELECT a FROM t WHERE ((1.000000000000*x)+(0.000000000000*y)+(0.000000000000*z)>=0. AND '
            '(0.000000000000*x)+(1.000000000000*y)+(0.000000000000*z)>=0. AND '
            '(0.000000000000*x)+(0.000000000000*y)+(1.000000000000*z)>=0.)' + index_clause)


def test_region_cover_holds_every_point_of_the_region():
    pytest.importorskip('astropy_healpix')
    import random

    import astropy.units as u
    from astropy_healpix import lonlat_to_healpix
    from TAP import duckdbsupport

    sql = _triangle_sql(' AND ((hpx29 BETWEEN 5 AND 6) OR (hpx29 = 9))')
    adql = "SELECT a FROM t WHERE CONTAINS(POINT('ICRS',ra,dec),POLYGON('ICRS',0,0,90,0,0,90))=1"
    out = duckdbsupport.replace_region_cover(sql, adql, 'hpx29', 29)

    assert 'hpx29 BETWEEN 5 AND 6' not in out and '(hpx29 = 9)' not in out     # the library's cells are gone
    assert out.count('>=0.') == 3                                              # the geometry test is untouched
    ranges = [(int(a), int(b)) for a, b in re.findall(r'hpx29 BETWEEN (\d+) AND (\d+)', out)]
    assert 0 < len(ranges) <= duckdbsupport.MAX_REGION_RANGES

    rng = random.Random(1)
    for _ in range(2000):
        lon, lat = rng.uniform(0, 90), rng.uniform(0, 90)                      # inside the octant
        cell = int(lonlat_to_healpix(lon * u.deg, lat * u.deg, 2 ** 29, order='nested'))
        assert any(lo <= cell <= hi for lo, hi in ranges), (lon, lat)


def test_region_cover_falls_back_to_a_scan_when_unsure():
    from TAP import duckdbsupport

    clause = ' AND ((hpx29 BETWEEN 5 AND 6) OR (hpx29 = 9))'
    box = "SELECT a FROM t WHERE CONTAINS(POINT('ICRS',ra,dec),BOX('ICRS',1,2,3,4))=1"

    # Edge conditions it cannot read: no index constraint at all, rather than a wrong one.
    odd = 'SELECT a FROM t WHERE (something_else(x, y, z))' + clause
    assert duckdbsupport.replace_region_cover(odd, box, 'hpx29', 29) == 'SELECT a FROM t WHERE (something_else(x, y, z))'

    # Two geometry constraints in one query.
    two = box + " AND CONTAINS(POINT('ICRS',ra,dec),CIRCLE('ICRS',1,2,3))=1"
    assert 'hpx29' not in duckdbsupport.replace_region_cover(_triangle_sql(clause), two, 'hpx29', 29)

    # A cone search is left exactly as it was.
    cone = "SELECT a FROM t WHERE CONTAINS(POINT('ICRS',ra,dec),CIRCLE('ICRS',1,2,3))=1"
    sql = 'SELECT a FROM t WHERE ((0.5*x)+(0.5*y)+(0.7*z)>=9.99e-01)' + clause
    assert duckdbsupport.replace_region_cover(sql, cone, 'hpx29', 29) == sql


def test_an_abandoned_query_is_interrupted():
    """abandon() stops a statement that is running, and says why."""
    duckdb = pytest.importorskip('duckdb')
    import threading
    import time

    conn = duckdbsupport.Connection(duckdb.connect(), None, None)
    conn._deadline = duckdbsupport.Deadline(conn._duck, 0)
    cur = conn.cursor()
    threading.Timer(0.5, duckdbsupport.abandon).start()
    started = time.time()
    with pytest.raises(duckdbsupport.QueryAbandoned, match='went away'):
        cur.execute('SELECT count(*) FROM range(100000000000) a, range(1000) b WHERE a.range % 7 = b.range')
        cur.fetchall()
    assert time.time() - started < 20
    # Nothing more runs on a connection whose client has gone.
    with pytest.raises(duckdbsupport.QueryAbandoned):
        cur.execute('SELECT 1')
    duckdbsupport._deadlines.clear()


def test_a_connection_spills_to_a_directory_of_its_own(tmp_path, caplog):
    """The spill directory is one per process, inside the one configured,
    and setting it up logs no warning (it failed silently for a day for
    want of an import)."""
    import logging
    import os
    import duckdb
    from TAP import duckdbsupport
    conn = duckdb.connect()
    conn.execute("SET temp_directory = '%s'" % tmp_path)
    with caplog.at_level(logging.WARNING):
        duckdbsupport._own_temp_directory(conn)
    assert 'no temp directory of its own' not in caplog.text
    mine = conn.execute("SELECT current_setting('temp_directory')").fetchone()[0]
    assert mine == os.path.join(str(tmp_path), 'q%d' % os.getpid())
    assert os.path.isdir(mine)
