"""DuckDB backend: Parquet files served through a DuckDB catalog file.

The unit tests need nothing beyond the TAP package.  The end-to-end tests
build a tiny Parquet catalog and are skipped when duckdb is not installed.
"""
from __future__ import annotations

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
    XCOL=x
    YCOL=y
    ZCOL=z
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
