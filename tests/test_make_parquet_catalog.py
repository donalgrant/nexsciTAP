"""tools/make_parquet_catalog.py: any table of positions to the served layout."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).parent.parent


def test_csv_becomes_sorted_indexed_parquet(tmp_path):
    duckdb = pytest.importorskip('duckdb')
    pytest.importorskip('astropy_healpix')
    import astropy.units as u
    from astropy_healpix import lonlat_to_healpix

    src = tmp_path / 'in.csv'
    src.write_text('Name,RA (deg),DEJ2000,x,Flux-1\n'
                   'a,359.9,-0.1,1,10\n'
                   'b,0.1,0.1,2,20\n'
                   'c,180.0,89.9,3,30\n'
                   'd,,5.0,4,40\n'
                   'e,45.0,-45.0,5,50\n')
    out = tmp_path / 'out'

    run = subprocess.run([sys.executable, str(REPO / 'tools' / 'make_parquet_catalog.py'), str(src), str(out),
                          '--ra', 'RA (deg)', '--dec', 'DEJ2000', '--rows-per-file', '3', '--row-group', '2'],
                         capture_output=True, text=True)
    assert run.returncode == 0, run.stderr
    assert sorted(p.name for p in out.iterdir()) == ['part-00.parquet', 'part-01.parquet']

    con = duckdb.connect()
    rows = con.execute(f"SELECT name, ra, dec, x_in, flux_1, hpx29 FROM read_parquet('{out}/*.parquet', "
                       "filename=true, file_row_number=true) ORDER BY filename, file_row_number").fetchall()

    # The row without a position is gone; names are plain identifiers.
    assert sorted(r[0] for r in rows) == ['a', 'b', 'c', 'e']

    # Sorted by the index across files, and the index is the order-29 nested pixel.
    assert [r[5] for r in rows] == sorted(r[5] for r in rows)
    for _, ra, dec, _, _, hpx in rows:
        assert hpx == int(lonlat_to_healpix(ra * u.deg, dec * u.deg, 2 ** 29, order='nested'))


def test_missing_position_column_is_reported(tmp_path):
    pytest.importorskip('duckdb')
    pytest.importorskip('astropy_healpix')

    src = tmp_path / 'in.csv'
    src.write_text('alpha,delta\n1.0,2.0\n')

    run = subprocess.run([sys.executable, str(REPO / 'tools' / 'make_parquet_catalog.py'), str(src), str(tmp_path / 'o')],
                         capture_output=True, text=True)
    assert run.returncode != 0
    assert 'No column "ra"' in run.stderr


def test_schema_file_overrides_guessed_types(tmp_path):
    """Types a CSV cannot convey: an empty column, date-like text, single precision."""
    duckdb = pytest.importorskip('duckdb')
    pytest.importorskip('astropy_healpix')

    src = tmp_path / 'in.csv'
    src.write_text('name,ra,dec,empty,obsdate,mag,big\n'
                   'a,10.0,-5.0,,2000-09-26,12.5,9007199254740993\n'
                   'b,11.0,-6.0,,1998-04-18,13.25,2\n')
    schema = tmp_path / 'schema.csv'
    schema.write_text('column_name,datatype\nname,char\nra,double\ndec,double\nempty,int\n'
                      'obsdate,char\nmag,float\nbig,long\nnot_in_file,int\n')

    def types(extra):
        out = tmp_path / ('out' + str(len(extra)))
        run = subprocess.run([sys.executable, str(REPO / 'tools' / 'make_parquet_catalog.py'), str(src), str(out), *extra],
                             capture_output=True, text=True)
        assert run.returncode == 0, run.stderr
        con = duckdb.connect()
        desc = {r[0]: r[1] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{out}/*.parquet')").fetchall()}
        rows = con.execute(f"SELECT obsdate, sum(empty), big FROM read_parquet('{out}/*.parquet') "
                           "GROUP BY ALL ORDER BY big").fetchall() if extra else None
        return desc, rows

    guessed, _ = types([])
    assert guessed['empty'] == 'VARCHAR' and guessed['obsdate'] == 'DATE'      # what goes wrong unaided

    declared, rows = types(['--schema', str(schema)])
    assert declared['empty'] == 'INTEGER'
    assert declared['obsdate'] == 'VARCHAR'
    assert declared['mag'] == 'FLOAT'
    assert declared['big'] == 'BIGINT'
    assert rows == [('1998-04-18', None, 2), ('2000-09-26', None, 9007199254740993)]


# ---- what the columns mean ---------------------------------------------

TOOLS = REPO / 'tools'


def _tool(name, *args):
    run = subprocess.run([sys.executable, str(TOOLS / name), *map(str, args)], capture_output=True, text=True)
    assert run.returncode == 0, run.stderr
    return run.stdout


def _tap_schema(duckdb, tmp_path, folder, name='t', *extra):
    db = tmp_path / f'{name}.duckdb'
    _tool('make_duckdb_catalog.py', '--db', db, '--table', f'{name}={folder}/*.parquet', *extra)
    con = duckdb.connect(str(db), read_only=True)
    cols = {r[0]: r[1:] for r in con.execute(
        'SELECT column_name, unit, ucd, description FROM tap_schema.columns WHERE table_name = ?', [name]).fetchall()}
    table = con.execute('SELECT description FROM tap_schema.tables WHERE table_name = ?', [name]).fetchone()[0]
    return cols, table


def test_units_and_descriptions_reach_tap_schema(tmp_path):
    """A schema file's unit, ucd and description columns end up in TAP_SCHEMA, by way of the Parquet files."""
    duckdb = pytest.importorskip('duckdb')
    pytest.importorskip('astropy_healpix')

    src = tmp_path / 'in.csv'
    src.write_text('RAJ2000,DEJ2000,Kmag,Name\n10.0,-5.0,12.5,a\n11.0,-6.0,13.5,b\n')
    schema = tmp_path / 'schema.csv'
    schema.write_text('description,column_name,unit,datatype,ucd\n'
                      'Right ascension,RAJ2000,deg,double,pos.eq.ra;meta.main\n'
                      'Declination,DEJ2000,deg,double,pos.eq.dec;meta.main\n'
                      '"K\'s magnitude, <Vega> & all",Kmag,mag,float,phot.mag;em.IR.K\n')
    out = tmp_path / 'out'
    said = _tool('make_parquet_catalog.py', src, out, '--ra', 'RAJ2000', '--dec', 'DEJ2000',
                 '--schema', schema, '--description', "The table's own description")
    assert '3 of 4 columns have a description, 3 a unit' in said

    cols, table = _tap_schema(duckdb, tmp_path, out)
    assert table == "The table's own description"
    assert cols['ra'] == ('deg', 'pos.eq.ra;meta.main', 'Right ascension')      # under its new name
    assert cols['kmag'] == ('mag', 'phot.mag;em.IR.K', "K's magnitude, <Vega> & all")
    assert cols['name'] == ('', None, '')                                       # nothing said, nothing invented
    assert 'HEALPix' in cols['hpx29'][2]
    assert 'unit vector' in cols['x'][2]

    # The type column still does its first job.
    con = duckdb.connect()
    assert con.execute(f"SELECT typeof(kmag) FROM read_parquet('{out}/*.parquet') LIMIT 1").fetchone()[0] == 'FLOAT'

    # --describe on the service side wins over what the files say.
    _, table = _tap_schema(duckdb, tmp_path, out, 'u', '--describe', 'u=Operator text')
    assert table == 'Operator text'


def test_table_without_metadata_is_still_served(tmp_path):
    duckdb = pytest.importorskip('duckdb')
    con = duckdb.connect()
    folder = tmp_path / 'plain'
    folder.mkdir()
    con.execute(f"COPY (SELECT 1.0 AS ra, 2.0 AS dec, 5 AS hpx29) TO '{folder}/a.parquet' (FORMAT parquet)")
    cols, table = _tap_schema(duckdb, tmp_path, folder)
    assert table == '' and cols['ra'] == ('', None, '')


@pytest.mark.parametrize('fmt,ext', [('fits', 'fits'), ('votable', 'vot')])
def test_fits_and_votable_input_keep_their_units(tmp_path, fmt, ext):
    """These formats say what their columns mean; the conversion must not lose it."""
    duckdb = pytest.importorskip('duckdb')
    pytest.importorskip('astropy_healpix')
    import astropy.units as u
    import numpy as np
    from astropy.table import Table

    t = Table()
    t['RA'] = np.array([10.0, 11.0, 12.0]) * u.deg
    t['DEC'] = np.array([-5.0, -6.0, 7.0]) * u.deg
    t['flux'] = np.array([1.5, np.nan, 3.5], dtype='f4') * u.mJy
    t['flux'].description = 'Total flux density'
    t['id'] = np.array([7, 8, 9], dtype='i8')
    t['name'] = ['a', 'bb', 'ccc']
    src = tmp_path / f'in.{ext}'
    t.write(src, format=fmt)

    out = tmp_path / 'out'
    _tool('make_parquet_catalog.py', src, out, '--ra', 'RA', '--dec', 'DEC')

    con = duckdb.connect()
    rows = con.execute(f"SELECT id, name, ra, dec, flux FROM read_parquet('{out}/*.parquet') ORDER BY id").fetchall()
    assert [r[:4] for r in rows] == [(7, 'a', 10.0, -5.0), (8, 'bb', 11.0, -6.0), (9, 'ccc', 12.0, 7.0)]
    assert rows[0][4] == 1.5 and rows[2][4] == 3.5

    cols, _ = _tap_schema(duckdb, tmp_path, out)
    assert cols['ra'][0] == 'deg'
    assert cols['flux'][0] == 'mJy' and cols['flux'][2] == 'Total flux density'


def test_text_columns_declare_their_width(tmp_path):
    """astropy's reader of the metadata needs a width for text, not 'any length'."""
    duckdb = pytest.importorskip('duckdb')
    pytest.importorskip('astropy_healpix')
    sys.path.insert(0, str(TOOLS))
    import votable_metadata

    src = tmp_path / 'in.csv'
    src.write_text('ra,dec,name\n10.0,-5.0,abc\n11.0,-6.0,abcdefg\n')
    out = tmp_path / 'out'
    _tool('make_parquet_catalog.py', src, out)
    con = duckdb.connect()
    value = con.execute('SELECT value FROM parquet_kv_metadata(?) WHERE key = ?',
                        [f'{out}/part-00.parquet', votable_metadata.CONTENT_KEY.encode()]).fetchall()[0][0]
    text = bytes(value).decode()
    assert '<FIELD name="name" datatype="char" arraysize="7"' in text
