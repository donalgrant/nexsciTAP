#!/usr/bin/env python
# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

"""
Convert a table of sky positions to the Parquet layout the DuckDB backend serves.

    make_parquet_catalog.py INPUT OUTPUT_DIR [--ra COL] [--dec COL] [--schema FILE]
                            [--description TEXT] [--rows-per-file N] [--row-group N]

INPUT is a CSV, Parquet, FITS or VOTable file (CSV and Parquet may be a glob,
and may be s3:// or https:// locations DuckDB can read).  OUTPUT_DIR receives
part-00.parquet, part-01.parquet, ...; copy that directory to the catalog's
folder in S3 and list it with make_duckdb_catalog.py.

The output has every input column, with these changes:

  * the position columns are named "ra" and "dec" (degrees), whatever they
    were called, because the service uses one pair of names for all tables;
  * column names are lower-cased and anything but letters, digits and "_"
    becomes "_", so that they are plain SQL identifiers;
  * a column "hpx29" is added: the nested HEALPix order-29 index of the
    position (the quantity HATS calls _healpix_29);
  * rows with no position are dropped, since they could never be found by one.

A CSV file says nothing about column types, so they are guessed from the
values.  The guess goes wrong where the values do not settle it: a column with
no values at all becomes text, a text column that happens to look like dates
becomes dates, and single precision is not told from double.  --schema FILE
gives the types instead.  FILE is a CSV with a header and two columns, the
column name and its type (int, long, short, float, double, char, boolean),
which is what "SELECT column_name, datatype FROM TAP_SCHEMA.columns WHERE
table_name = '...'" returns from a TAP service.  Columns it does not list keep
their guessed type.

The same file says what the columns mean.  Further columns headed "unit",
"ucd", "utype" and "description" (any of them, in any order; add them to the
TAP_SCHEMA query above) are recorded in the Parquet files, and --description
does the same for the table as a whole.  The service shows them to users, in
its table listing and in the results it returns.  FITS tables and VOTables
carry units and descriptions themselves, which are kept unless FILE says
otherwise.  The metadata is stored the way the IVOA note "Parquet in the
Virtual Observatory" lays out (see votable_metadata.py), so other tools that
read Parquet can use it too.

Rows are sorted by hpx29 and written in small row groups.  Parquet keeps the
smallest and largest value of every column in each row group, so a positional
search reads only the few groups whose hpx29 range it touches.
"""

import argparse
import csv
import os
import re
import sys

import duckdb
import numpy as np
import votable_metadata
import pyarrow as pa
from astropy_healpix import lonlat_to_healpix
from astropy import units as u

ORDER = 29


TYPES = {'int': 'INTEGER', 'integer': 'INTEGER', 'short': 'SMALLINT', 'smallint': 'SMALLINT',
         'long': 'BIGINT', 'bigint': 'BIGINT', 'float': 'FLOAT', 'real': 'FLOAT',
         'double': 'DOUBLE', 'char': 'VARCHAR', 'varchar': 'VARCHAR', 'string': 'VARCHAR',
         'unicodechar': 'VARCHAR', 'boolean': 'BOOLEAN'}


NAME_HEADS = ('column_name', 'name', 'column')
TYPE_HEADS = ('datatype', 'type')
META_HEADS = ('unit', 'ucd', 'utype', 'description')


def read_schema(path):
    """
    ({column name: DuckDB type}, {column name: {unit, ucd, utype, description}})
    from a CSV with a header.  Columns are found by their headings; a file
    whose headings are not recognized is read as name, type.
    """

    with open(path, newline='') as f:
        rows = list(csv.reader(f))
    if not rows:
        return {}, {}

    heads = [h.strip().lower() for h in rows[0]]

    def position(names, default):
        return next((heads.index(n) for n in names if n in heads), default)

    name_at = position(NAME_HEADS, 0)
    type_at = position(TYPE_HEADS, 1 if not set(heads) & set(META_HEADS) else None)
    meta_at = {h: heads.index(h) for h in META_HEADS if h in heads}

    types, meta = {}, {}
    for row in rows[1:]:
        if len(row) <= name_at or not row[name_at].strip():
            continue
        name = row[name_at].strip()

        kind = row[type_at].strip().lower() if type_at is not None and len(row) > type_at else ''
        if kind:
            if kind not in TYPES:
                sys.exit(f'{path}: unknown type "{row[type_at]}" for column "{name}" '
                         f'(known: {", ".join(sorted(TYPES))})')
            types[name] = TYPES[kind]

        found = {h: row[i].strip() for h, i in meta_at.items() if len(row) > i and row[i].strip()}
        if found:
            meta[name] = found

    return types, meta


def read_input(con, path, types=None):
    """(DuckDB relation, {column: metadata the input itself carries}) for the input table."""

    # A folder of files arrives as a pattern, "dir/*.csv*"; judge it by the name before the star.
    low = path.lower().rstrip('*')

    if low.endswith(('.parquet', '.pq')) or '.parquet' in low:
        return con.sql(f"SELECT * FROM read_parquet('{path}')"), parquet_metadata(con, path)

    if low.endswith(('.csv', '.csv.gz', '.tsv', '.txt')):
        if types:
            # Only for columns the file has; read_csv refuses unknown names.
            have = con.sql(f"SELECT * FROM read_csv('{path}', all_varchar=true) LIMIT 0").columns
            decl = ', '.join(f"'{c}': '{t}'" for c, t in types.items() if c in have)
            return con.sql(f"SELECT * FROM read_csv('{path}', sample_size=-1, types={{{decl}}})"), {}
        return con.sql(f"SELECT * FROM read_csv('{path}', sample_size=-1)"), {}

    # FITS tables and VOTables go through astropy.
    from astropy.table import Table

    table = Table.read(path)

    def column(col):
        """A pyarrow array for an astropy column: native byte order, text as text, blanks as nulls."""
        mask = np.asarray(col.mask) if hasattr(col, 'mask') and np.ndim(col.mask) else None
        data = np.asarray(col.filled() if hasattr(col, 'filled') else col)
        if data.ndim != 1:
            sys.exit(f'Column "{col.name}" holds arrays; only one value per row is supported.')
        if data.dtype.kind == 'S':
            data = np.char.decode(data, 'utf-8', 'replace')
        if data.dtype.kind == 'U':
            data = np.char.rstrip(data).astype(object)
        elif not data.dtype.isnative:                      # FITS is big-endian
            data = data.astype(data.dtype.newbyteorder('='))
        if data.dtype.kind == 'f':
            if mask is not None:
                data = np.where(mask, np.nan, data)
            mask = np.isnan(data)                          # a missing number is a null, as in CSV input
        return pa.array(data, mask=mask if mask is not None and mask.any() else None)

    arrow = pa.table({name: column(table[name]) for name in table.colnames})
    con.register('astropy_input', arrow)

    carried = {}
    for name in table.colnames:
        col = table[name]
        found = {'unit': str(col.unit) if col.unit is not None else '',
                 'description': col.description or '',
                 'ucd': (col.meta or {}).get('ucd', '') or ''}
        carried[name] = {k: v for k, v in found.items() if v}
    return con.sql('SELECT * FROM astropy_input'), carried


def parquet_metadata(con, path):
    """Column metadata already in Parquet input that follows the IVOA note."""

    try:
        row = con.execute('SELECT value FROM parquet_kv_metadata(?) WHERE key = ? LIMIT 1',
                          [path, votable_metadata.CONTENT_KEY.encode()]).fetchone()
    except duckdb.Error:
        return {}
    return votable_metadata.parse(bytes(row[0]).decode('utf-8', 'replace'))[1] if row else {}


def clean(name):
    name = re.sub(r'[^0-9a-z_]', '_', name.strip().lower())
    return name if re.match(r'[a-z_]', name) else '_' + name


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('input')
    p.add_argument('output_dir')
    p.add_argument('--ra', default='ra', help='input column holding right ascension in degrees')
    p.add_argument('--dec', default='dec', help='input column holding declination in degrees')
    p.add_argument('--schema', metavar='FILE',
                   help='CSV of column name and type, to use instead of guessing types from the values; '
                        'may also give unit, ucd, utype and description')
    p.add_argument('--description', default='', help='what the table is, shown in the table listing')
    p.add_argument('--rows-per-file', type=int, default=5_000_000)
    p.add_argument('--row-group', type=int, default=100_000)
    p.add_argument('--work', metavar='DIR',
                   help='keep the working tables on disk in DIR, for a table too large for memory')
    p.add_argument('--memory', metavar='SIZE', help="most memory DuckDB may use, e.g. 8GB (default: DuckDB's own)")
    args = p.parse_args()

    # By default the whole table is held in memory, which is quickest and
    # fine for a few million rows.  --work puts the working tables in a
    # database file there instead, and --memory bounds what DuckDB keeps in
    # memory; with both, the size of the table is limited by the disk.
    # DuckDB reads a machine's memory, not a container's allowance, so in a
    # container --memory is what keeps the conversion from being killed.
    if args.work:
        os.makedirs(args.work, exist_ok=True)
        scratch = os.path.join(args.work, f'convert-{os.getpid()}.duckdb')
        con = duckdb.connect(scratch)
        con.execute(f"SET temp_directory = '{os.path.join(args.work, 'spill')}'")
    else:
        scratch = None
        con = duckdb.connect()
    if args.memory:
        con.execute(f"SET memory_limit = '{args.memory}'")
    con.execute('SET preserve_insertion_order = true')
    temp = '' if scratch else 'TEMP'       # a table in the database file, or in memory

    types, declared = read_schema(args.schema) if args.schema else ({}, {})
    rel, carried = read_input(con, args.input, types)  # noqa: F841  (rel is used by name in SQL)

    # Output names: cleaned, positions renamed, no collisions.
    names = {}
    for col in rel.columns:
        if col == args.ra:
            new = 'ra'
        elif col == args.dec:
            new = 'dec'
        else:
            new = clean(col)
            if new in ('ra', 'dec', 'hpx29', 'aiab_x', 'aiab_y', 'aiab_z'):
                # The aiab_ three are added by the service's view; the rest are ours.
                new = new + '_in'
        if new in names.values():
            sys.exit(f'Columns collide after cleaning their names: "{col}" -> "{new}"')
        names[col] = new

    for need, given in (('ra', args.ra), ('dec', args.dec)):
        if given not in rel.columns:
            sys.exit(f'No column "{given}" in the input for {need}; name it with --{need}.  '
                     f'Columns: {", ".join(rel.columns)}')

    select = ', '.join((f'CAST("{old}" AS {types[old]})' if old in types else f'"{old}"') + f' AS "{new}"'
                       for old, new in names.items())
    con.execute(f'CREATE {temp} TABLE src AS SELECT {select} FROM rel '
                'WHERE "{0}" IS NOT NULL AND "{1}" IS NOT NULL'.format(args.ra, args.dec))

    total_in = con.execute('SELECT count(*) FROM rel').fetchone()[0]
    kept, lo, hi = con.execute('SELECT count(*), min(dec), max(dec) FROM src').fetchone()

    if kept == 0:
        sys.exit('No rows with a position.')
    if lo < -90 or hi > 90:
        sys.exit(f'Declination runs from {lo} to {hi}; it must be in degrees.')

    # The index is computed in numpy, a slice at a time, and joined back by row id.
    con.execute(f'CREATE {temp} TABLE idx (rid BIGINT, hpx29 BIGINT)')
    nside = 2 ** ORDER
    step = 5_000_000
    for start in range(0, kept, step):
        pos = con.sql(f'SELECT rowid, ra, dec FROM src WHERE rowid >= {start} AND rowid < {start + step}').to_arrow_table()
        hpx = lonlat_to_healpix(pos['ra'].to_numpy() * u.deg, pos['dec'].to_numpy() * u.deg,
                                nside, order='nested')
        chunk = pa.table({'rid': pos['rowid'], 'hpx29': pa.array(np.asarray(hpx, dtype=np.int64))})  # noqa: F841
        con.execute('INSERT INTO idx SELECT * FROM chunk')

    os.makedirs(args.output_dir, exist_ok=True)

    parts = max(1, -(-kept // args.rows_per_file))
    # Each row's place in the output.  Only the index is sorted as a whole,
    # three numbers a row; the rows themselves are brought together a file
    # at a time below, so memory is set by --rows-per-file, not by the table.
    con.execute(f'CREATE {temp} TABLE place AS SELECT rid, hpx29, '
                'row_number() OVER (ORDER BY hpx29, rid) - 1 AS _n FROM idx')
    con.execute('DROP TABLE idx')

    # What the columns mean, under their output names: what the input
    # carried, then what --schema says, which wins.
    meta = {}
    for old, new in names.items():
        meta[new] = {**carried.get(old, {}), **declared.get(old, {})}
    for pos in ('ra', 'dec'):
        meta[pos].setdefault('unit', 'deg')
    meta['hpx29'] = {'description': 'Nested HEALPix index of (ra, dec) at order 29, added on conversion; '
                                    'the files are sorted by it'}
    out_cols = [(r[0], r[1]) for r in con.execute('DESCRIBE SELECT src.*, place.hpx29 FROM src JOIN place ON src.rowid = place.rid').fetchall()]
    text_cols = [c for c, t in out_cols if t.upper() == 'VARCHAR']
    if text_cols:
        widths = con.execute('SELECT ' + ', '.join(f'max(length("{c}"))' for c in text_cols) + ' FROM src').fetchone()
        for c, w in zip(text_cols, widths):
            meta.setdefault(c, {})['width'] = max(1, w or 1)
    votable = votable_metadata.build([(c, t, meta.get(c, {})) for c, t in out_cols], args.description)
    kv = (f"KV_METADATA {{'{votable_metadata.VERSION_KEY}': '1.0', "
          f"'{votable_metadata.CONTENT_KEY}': '{votable.replace(chr(39), chr(39) * 2)}'}}")

    for part in range(parts):
        out = os.path.join(args.output_dir, f'part-{part:02d}.parquet')
        a, b = part * args.rows_per_file, (part + 1) * args.rows_per_file
        con.execute(f"""COPY (SELECT src.*, place.hpx29 FROM src JOIN place ON src.rowid = place.rid
                              WHERE place._n >= {a} AND place._n < {b} ORDER BY place._n)
                        TO '{out}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE {args.row_group}, {kv})""")

    if scratch:
        con.close()
        for leftover in (scratch, scratch + '.wal'):
            if os.path.exists(leftover):
                os.remove(leftover)

    print(f'{kept:,} rows in {parts} file(s) under {args.output_dir}'
          + (f'  ({total_in - kept:,} rows without a position dropped)' if total_in != kept else ''))
    renamed = {o: n for o, n in names.items() if o != n}
    if renamed:
        print('renamed: ' + ', '.join(f'{o} -> {n}' for o, n in renamed.items()))
    described = sum(1 for c in names.values() if meta[c].get('description'))
    with_unit = sum(1 for c in names.values() if meta[c].get('unit'))
    print(f'{described} of {len(names)} columns have a description, {with_unit} a unit')


if __name__ == '__main__':
    main()
