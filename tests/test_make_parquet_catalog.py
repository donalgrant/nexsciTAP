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
    rows = con.execute(f"SELECT name, ra, dec, x, flux_1, hpx29 FROM read_parquet('{out}/*.parquet', "
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
    assert 'unit vector' in cols['aiab_x'][2]

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


def test_describe_adds_metadata_and_leaves_the_data_alone(tmp_path):
    """A catalog converted without metadata gets it later, file for file, with nothing else changed."""
    duckdb = pytest.importorskip('duckdb')
    pytest.importorskip('astropy_healpix')

    src = tmp_path / 'in.csv'
    src.write_text('ra,dec,kmag,name\n' + ''.join(f'{10 + i * 0.01},{-5 + i * 0.01},{12 + i % 7},n{i}\n' for i in range(950)))
    old = tmp_path / 'old'
    _tool('make_parquet_catalog.py', src, old, '--rows-per-file', '400', '--row-group', '100')
    schema = tmp_path / 'schema.csv'
    schema.write_text('column_name,datatype,unit,description\nKMAG,char,mag,K magnitude\nabsent,int,,not there\n')

    new = tmp_path / 'new'
    said = _tool('describe_parquet_catalog.py', old, new, '--schema', schema, '--description', 'A described table')
    assert '2 of 5 columns have a description, 3 a unit' in said and '1 columns in' in said

    con = duckdb.connect()
    assert sorted(p.name for p in new.iterdir()) == sorted(p.name for p in old.iterdir()) and len(list(new.iterdir())) == 3
    for part in sorted(p.name for p in old.iterdir()):
        a = con.execute(f"SELECT * FROM read_parquet('{old}/{part}')").fetchall()
        b = con.execute(f"SELECT * FROM read_parquet('{new}/{part}')").fetchall()
        assert a == b                                              # same rows, same order
        groups = lambda d: con.execute(f"SELECT row_group_id, row_group_num_rows FROM parquet_metadata('{d}/{part}') "
                                       'GROUP BY ALL ORDER BY 1').fetchall()
        assert groups(old) == groups(new)
    assert con.execute(f"SELECT typeof(kmag) FROM read_parquet('{new}/*.parquet') LIMIT 1").fetchone()[0] == 'BIGINT'

    cols, table = _tap_schema(duckdb, tmp_path, new)
    assert table == 'A described table'
    assert cols['kmag'] == ('mag', None, 'K magnitude') and cols['ra'][0] == 'deg' and 'HEALPix' in cols['hpx29'][2]


@pytest.mark.parametrize('with_schema', [False, True])
def test_a_folder_of_csv_files_is_one_table(tmp_path, with_schema):
    """As the ingest task passes a folder: a pattern ending in a star, with or without a column file."""
    duckdb = pytest.importorskip('duckdb')
    pytest.importorskip('astropy_healpix')

    src = tmp_path / 'in'
    src.mkdir()
    (src / 'part-0000.csv').write_text('name,ra,dec,n\na,10.0,5.0,1\nb,20.0,-5.0,2\n')
    (src / 'part-0001.csv').write_text('name,ra,dec,n\nc,30.0,15.0,3\n')
    out = tmp_path / 'out'
    args = [sys.executable, str(REPO / 'tools' / 'make_parquet_catalog.py'), str(src / '*.csv*'), str(out)]
    if with_schema:
        schema = tmp_path / 'columns.csv'
        schema.write_text('column_name,datatype,unit,ucd,description\nn,long,,,A count\nra,double,deg,,\n')
        args += ['--schema', str(schema)]

    run = subprocess.run(args, capture_output=True, text=True)
    assert run.returncode == 0, run.stderr

    con = duckdb.connect()
    assert con.execute(f"SELECT list(name ORDER BY name), sum(n) FROM read_parquet('{out}/*.parquet')").fetchone() \
        == (['a', 'b', 'c'], 6)
    if with_schema:
        assert con.execute(f"SELECT typeof(n) FROM read_parquet('{out}/*.parquet') LIMIT 1").fetchone() == ('BIGINT',)


def test_describe_can_give_a_column_its_name_back(tmp_path):
    """A column an earlier conversion renamed out of the way gets its own name, and keeps what was said of it."""
    duckdb = pytest.importorskip('duckdb')
    pytest.importorskip('astropy_healpix')

    old = tmp_path / 'old'
    old.mkdir()
    con = duckdb.connect()
    src = tmp_path / 'in.csv'
    src.write_text('ra,dec,z_in\n' + ''.join(f'{10 + i * 0.01},{-5 + i * 0.01},{i / 100}\n' for i in range(300)))
    schema = tmp_path / 'schema.csv'
    schema.write_text('column_name,unit,description\nz_in,,Redshift\n')
    _tool('make_parquet_catalog.py', src, old, '--schema', schema, '--row-group', '100')

    new = tmp_path / 'new'
    said = _tool('describe_parquet_catalog.py', old, new, '--rename', 'z_in=z')
    assert 'renamed: z_in -> z' in said
    a = con.execute(f"SELECT ra, dec, hpx29, z_in FROM read_parquet('{old}/*.parquet', file_row_number=true) ORDER BY file_row_number").fetchall()
    b = con.execute(f"SELECT ra, dec, hpx29, z FROM read_parquet('{new}/*.parquet', file_row_number=true) ORDER BY file_row_number").fetchall()
    assert a == b

    cols, _ = _tap_schema(duckdb, tmp_path, new)
    assert cols['z'][2] == 'Redshift' and 'z_in' not in cols and 'unit vector' in cols['aiab_z'][2]

    # Names the service or the converter owns are refused, and so is a name already taken.
    for bad in ('z_in=ra', 'z_in=aiab_z', 'z_in=dec', 'nope=q', 'ra=alpha', 'dec=delta', 'hpx29=cell'):
        run = subprocess.run([sys.executable, str(REPO / 'tools' / 'describe_parquet_catalog.py'), str(old),
                              str(tmp_path / 'never'), '--rename', bad], capture_output=True, text=True)
        assert run.returncode != 0, bad


def test_row_groups_are_sized_by_values_not_rows(tmp_path):
    """A wide table gets fewer rows to a group; a narrow one keeps the old 100,000; and an old catalog can be regrouped."""
    duckdb = pytest.importorskip('duckdb')
    pytest.importorskip('astropy_healpix')
    sys.path.insert(0, str(REPO / 'tools'))
    from make_parquet_catalog import row_group_size

    assert row_group_size(30) == 100_000                 # AllWISE's 30 columns: as before
    assert row_group_size(520) == 18_432                 # the Chandra catalog: the size that was tested
    assert row_group_size(394) == 24_576 and row_group_size(218) == 45_056
    assert row_group_size(100_000) == 2_048              # never less than one vector

    # 2,500 columns and 5,000 rows: one group under the old rule, groups of 2,048 rows under the new.
    n, wide = 5000, 2497
    src = tmp_path / 'wide.csv'
    src.write_text('ra,dec,' + ','.join(f'c{i}' for i in range(wide)) + '\n'
                   + ''.join(f'{10 + i * 0.001},{-5 + i * 0.0005},' + ','.join(['1'] * wide) + '\n' for i in range(n)))
    con = duckdb.connect()
    sizes = lambda d: [r[0] for r in con.execute(f"SELECT row_group_num_rows FROM (SELECT DISTINCT row_group_id, row_group_num_rows FROM parquet_metadata('{d}/part-00.parquet')) ORDER BY row_group_id").fetchall()]
    new = tmp_path / 'new'
    _tool('make_parquet_catalog.py', src, new)
    assert sizes(new) == [2048, 2048, n - 4096]
    old = tmp_path / 'old'
    _tool('make_parquet_catalog.py', src, old, '--row-group', '100000')
    assert sizes(old) == [n]

    # The same catalog regrouped in place of a new conversion: same rows, same order, smaller groups.
    again = tmp_path / 'again'
    said = _tool('describe_parquet_catalog.py', old, again, '--row-group', '4096')
    assert 'to a row group' in said and sizes(again) == [4096, n - 4096]
    order = lambda d: con.execute(f"SELECT hpx29, ra, c0 FROM read_parquet('{d}/*.parquet', file_row_number=true) ORDER BY file_row_number").fetchall()
    assert order(again) == order(old)
    auto = tmp_path / 'auto'
    _tool('describe_parquet_catalog.py', old, auto, '--row-group', 'auto')
    assert order(auto) == order(old) and sizes(auto) == sizes(new)


# ---- HATS catalogs, as they are published ---------------------------------------

def _hats_rows(n, seed):
    import numpy as np
    rng = np.random.default_rng(seed)
    return {'ra': rng.uniform(0, 360, n), 'dec': np.degrees(np.arcsin(rng.uniform(-1, 1, n))),
            'name': [f's{seed}_{i}' for i in range(n)]}


def _write_hats(root, layout, ra='RA', dec='DEC', extra=False):
    """A small HATS tree:  'file' is data.lsdb.io's layout (Npix=P.parquet), 'dir' is IRSA's (Npix=P/part0.snappy.parquet)."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    total = 0
    for order, pix, n in ((0, 1, 40), (0, 7, 25), (1, 16, 35)):
        r = _hats_rows(n, pix)
        cols = {'_healpix_29': list(range(n)), ra: r['ra'], dec: r['dec'], 'Name': r['name']}
        if extra:
            cols.update({'_healpix_19': [1] * n, '_healpix_9': [1] * n, 'Norder': [order] * n, 'Dir': [0] * n, 'Npix': [pix] * n})
        d = root / 'dataset' / f'Norder={order}' / 'Dir=0'
        path = d / f'Npix={pix}.parquet' if layout == 'file' else d / f'Npix={pix}' / 'part0.snappy.parquet'
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table(cols), path)
        total += n
    (root / 'dataset' / '_metadata').write_bytes(b'not a table')          # present in real catalogs; must not be read as data
    (root / 'partition_info.csv').write_text('Norder,Npix\n0,1\n0,7\n1,16\n')
    props = (f'#HATS catalog\nobs_collection=little\nhats_nrows={total}\nhats_col_ra={ra}\nhats_col_dec={dec}\n'
             "hats_cols_sort=['_healpix_29', '_healpix_19']\nhats_creation_date=2026-02-09T16\\:19UTC\n")
    (root / ('properties' if layout == 'file' else 'hats.properties')).write_text(props)
    return total


def _convert(src, out, *extra):
    return subprocess.run([sys.executable, str(REPO / 'tools' / 'make_parquet_catalog.py'), str(src), str(out), *extra],
                          capture_output=True, text=True)


@pytest.mark.parametrize('layout', ['file', 'dir'])
def test_a_hats_catalog_is_read_as_published(tmp_path, layout):
    import duckdb
    ra, dec = ('RA', 'DEC') if layout == 'file' else ('ra_from_object_id', 'dec_from_object_id')
    total = _write_hats(tmp_path / 'cat', layout, ra, dec, extra=(layout == 'dir'))
    run = _convert(tmp_path / 'cat', tmp_path / 'out')                    # no --ra, --dec:  the catalog says
    assert run.returncode == 0, run.stderr
    assert 'HATS catalog little' in run.stdout and f'{total:,} rows by its properties' in run.stdout
    got = duckdb.sql(f"SELECT * FROM read_parquet('{tmp_path}/out/*.parquet')")
    assert got.columns == ['ra', 'dec', 'name', 'hpx29'] or set(got.columns) == {'ra', 'dec', 'name', 'hpx29'}
    rows = got.fetchall()
    assert len(rows) == total
    hpx = [r[got.columns.index('hpx29')] for r in rows]
    assert hpx == sorted(hpx)                                             # sorted on the service's index, not HATS's partitions


def test_a_hats_collection_leads_to_its_primary_catalog(tmp_path):
    import duckdb
    total = _write_hats(tmp_path / 'coll' / 'main', 'file')
    _write_hats(tmp_path / 'coll' / 'main_margin', 'file')                # beside it, and not the catalog
    (tmp_path / 'coll' / 'collection.properties').write_text('obs_collection=little\nhats_primary_table_url=main\nall_margins=main_margin\n')
    run = _convert(tmp_path / 'coll', tmp_path / 'out')
    assert run.returncode == 0, run.stderr
    assert duckdb.sql(f"SELECT count(*) FROM read_parquet('{tmp_path}/out/*.parquet')").fetchall()[0][0] == total


def test_a_directory_that_is_not_hats_is_refused_with_the_reason(tmp_path):
    (tmp_path / 'd').mkdir()
    (tmp_path / 'd' / 'partition_info.csv').write_text('Norder,Npix\n0,1\n')
    run = _convert(tmp_path / 'd', tmp_path / 'out')
    assert run.returncode != 0 and 'not a HATS catalog' in run.stderr


def _carried(out_dir):
    """What the converted table says of itself and of its columns."""
    import duckdb
    sys.path.insert(0, str(REPO / 'tools'))
    try:
        import votable_metadata
    finally:
        sys.path.remove(str(REPO / 'tools'))
    first = sorted(Path(out_dir).glob('*.parquet'))[0]
    row = duckdb.connect().execute('SELECT value FROM parquet_kv_metadata(?) WHERE key = ? LIMIT 1',
                                   [str(first), votable_metadata.CONTENT_KEY.encode()]).fetchone()
    return votable_metadata.parse(bytes(row[0]).decode())


def test_a_hats_catalogs_own_units_and_descriptions_are_kept(tmp_path):
    """IRSA writes a unit and a description on every column of its HATS files; they travel into the table."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    root = tmp_path / 'cat'
    r = _hats_rows(30, 3)
    fields = [pa.field('_healpix_29', pa.int64(), metadata={'unit': '', 'description': 'HEALPix order 29 pixel index.'}),
              pa.field('ra_obj', pa.float64(), metadata={'unit': 'deg', 'description': 'Right ascension of the object'}),
              pa.field('dec_obj', pa.float64(), metadata={'unit': 'deg', 'description': 'Declination of the object'}),
              pa.field('Flux', pa.float64(), metadata={'unit': 'mJy', 'description': 'Flux in the band'}),
              pa.field('label', pa.string(), metadata={'unit': 'none', 'description': 'A name'})]
    table = pa.table([list(range(30)), r['ra'], r['dec'], r['ra'], r['name']], schema=pa.schema(fields))
    path = root / 'dataset' / 'Norder=0' / 'Dir=0' / 'Npix=2' / 'part0.snappy.parquet'
    path.parent.mkdir(parents=True)
    pq.write_table(table, path)
    pq.write_metadata(table.schema, root / 'dataset' / '_common_metadata')
    (root / 'hats.properties').write_text('# Little Survey Sources - HATS Catalog\nobs_collection=little\nhats_nrows=30\n'
                                          'hats_col_ra=ra_obj\nhats_col_dec=dec_obj\nbib_reference_url=https://example.org/little\n')
    run = _convert(root, tmp_path / 'out')
    assert run.returncode == 0, run.stderr
    description, columns = _carried(tmp_path / 'out')
    assert columns['flux'] == {'unit': 'mJy', 'description': 'Flux in the band'} or (
        columns['flux'].get('unit') == 'mJy' and columns['flux'].get('description') == 'Flux in the band')
    assert columns['ra'].get('description') == 'Right ascension of the object' and columns['ra'].get('unit') == 'deg'
    assert columns['label'].get('description') == 'A name' and not columns['label'].get('unit')      # "none" is no unit
    assert description == 'Little Survey Sources.  Source:  https://example.org/little'


def test_a_description_given_wins_over_the_catalogs_own(tmp_path):
    _write_hats(tmp_path / 'cat', 'file')
    run = _convert(tmp_path / 'cat', tmp_path / 'out', '--description', 'Mine')
    assert run.returncode == 0, run.stderr
    assert _carried(tmp_path / 'out')[0] == 'Mine'


def test_names_are_plain_adql_identifiers(tmp_path):
    """Headings that begin with a digit or a sign, or are ADQL's own words, come out typeable.

    TOPCAT's validator reported "_2mass", "__" and "size" in served tables
    (22 November 2026):  each can be asked for only in double quotes.
    """
    duckdb = pytest.importorskip('duckdb')
    pytest.importorskip('astropy_healpix')

    src = tmp_path / 'in.csv'
    src.write_text('RA,Dec,2MASS,**,#Planets,Planets,Size,Date,_row\n'
                   '10.0,5.0,a,b,1,2,3.5,2020,7\n'
                   '20.0,-5.0,c,d,3,4,4.5,2021,8\n')
    out = tmp_path / 'out'
    run = subprocess.run([sys.executable, str(REPO / 'tools' / 'make_parquet_catalog.py'), str(src), str(out),
                          '--ra', 'RA', '--dec', 'Dec'], capture_output=True, text=True)
    assert run.returncode == 0, run.stderr

    names = [d[0] for d in duckdb.connect().execute(f"SELECT * FROM read_parquet('{out}/*.parquet') LIMIT 0").description]
    assert names[:9] == ['ra', 'dec', 'c_2mass', 'col', 'planets', 'planets_2', 'size_', 'date_', 'row']
    assert '2MASS -> c_2mass' in run.stdout and 'Size -> size_' in run.stdout

    sys.path.insert(0, str(REPO / 'tools'))
    try:
        import make_parquet_catalog as m
    finally:
        sys.path.pop(0)
    import re
    for name in names:
        assert re.fullmatch(r'[a-z][a-z0-9_]*', name) and name not in m.ADQL_RESERVED, name
    assert 'dec' not in m.ADQL_RESERVED and m.clean('Mass (Msun)') == 'mass__msun_'
