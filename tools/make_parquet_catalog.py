#!/usr/bin/env python
# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

"""
Convert a table of sky positions to the Parquet layout the DuckDB backend serves.

    make_parquet_catalog.py INPUT OUTPUT_DIR [--ra COL] [--dec COL] [--schema FILE]
                            [--rows-per-file N] [--row-group N]

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

Rows are sorted by hpx29 and written in small row groups.  Parquet keeps the
smallest and largest value of every column in each row group, so a positional
search reads only the few groups whose hpx29 range it touches.
"""

import argparse
import os
import re
import sys

import duckdb
import numpy as np
import pyarrow as pa
from astropy_healpix import lonlat_to_healpix
from astropy import units as u

ORDER = 29


TYPES = {'int': 'INTEGER', 'integer': 'INTEGER', 'short': 'SMALLINT', 'smallint': 'SMALLINT',
         'long': 'BIGINT', 'bigint': 'BIGINT', 'float': 'FLOAT', 'real': 'FLOAT',
         'double': 'DOUBLE', 'char': 'VARCHAR', 'varchar': 'VARCHAR', 'string': 'VARCHAR',
         'unicodechar': 'VARCHAR', 'boolean': 'BOOLEAN'}


def read_schema(path):
    """{column name: DuckDB type} from a two-column CSV (name, type)."""

    import csv

    types = {}
    with open(path, newline='') as f:
        rows = list(csv.reader(f))
    for row in rows[1:]:
        if len(row) < 2 or not row[0].strip():
            continue
        name, kind = row[0].strip(), row[1].strip().lower()
        if kind not in TYPES:
            sys.exit(f'{path}: unknown type "{row[1]}" for column "{name}" '
                     f'(known: {", ".join(sorted(TYPES))})')
        types[name] = TYPES[kind]
    return types


def read_input(con, path, types=None):
    """Return a DuckDB relation for the input table."""

    low = path.lower()

    if low.endswith(('.parquet', '.pq')) or '.parquet' in low:
        return con.sql(f"SELECT * FROM read_parquet('{path}')")

    if low.endswith(('.csv', '.csv.gz', '.tsv', '.txt')):
        if types:
            # Only for columns the file has; read_csv refuses unknown names.
            have = con.sql(f"SELECT * FROM read_csv('{path}', all_varchar=true) LIMIT 0").columns
            decl = ', '.join(f"'{c}': '{t}'" for c, t in types.items() if c in have)
            return con.sql(f"SELECT * FROM read_csv('{path}', sample_size=-1, types={{{decl}}})")
        return con.sql(f"SELECT * FROM read_csv('{path}', sample_size=-1)")

    # FITS tables and VOTables go through astropy.
    from astropy.table import Table

    table = Table.read(path)
    arrow = pa.table({name: np.asarray(table[name].filled(np.nan) if hasattr(table[name], 'filled')
                                       and table[name].dtype.kind == 'f' else table[name])
                      for name in table.colnames})
    con.register('astropy_input', arrow)
    return con.sql('SELECT * FROM astropy_input')


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
                   help='CSV of column name and type, to use instead of guessing types from the values')
    p.add_argument('--rows-per-file', type=int, default=5_000_000)
    p.add_argument('--row-group', type=int, default=100_000)
    args = p.parse_args()

    con = duckdb.connect()
    con.execute('SET preserve_insertion_order = true')

    types = read_schema(args.schema) if args.schema else {}
    rel = read_input(con, args.input, types)

    # Output names: cleaned, positions renamed, no collisions.
    names = {}
    for col in rel.columns:
        if col == args.ra:
            new = 'ra'
        elif col == args.dec:
            new = 'dec'
        else:
            new = clean(col)
            if new in ('ra', 'dec', 'hpx29', 'x', 'y', 'z'):
                # x, y, z are added by the service's view; the rest are ours.
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
    con.execute(f'CREATE TEMP TABLE src AS SELECT {select} FROM rel '
                'WHERE "{0}" IS NOT NULL AND "{1}" IS NOT NULL'.format(args.ra, args.dec))

    total_in = con.execute('SELECT count(*) FROM rel').fetchone()[0]
    kept, lo, hi = con.execute('SELECT count(*), min(dec), max(dec) FROM src').fetchone()

    if kept == 0:
        sys.exit('No rows with a position.')
    if lo < -90 or hi > 90:
        sys.exit(f'Declination runs from {lo} to {hi}; it must be in degrees.')

    # The index is computed in numpy, a slice at a time, and joined back by row id.
    con.execute('CREATE TEMP TABLE idx (rid BIGINT, hpx29 BIGINT)')
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
    con.execute('CREATE TEMP TABLE sorted AS SELECT src.*, idx.hpx29, '
                'row_number() OVER (ORDER BY idx.hpx29) - 1 AS _n '
                'FROM src JOIN idx ON src.rowid = idx.rid')

    for part in range(parts):
        out = os.path.join(args.output_dir, f'part-{part:02d}.parquet')
        a, b = part * args.rows_per_file, (part + 1) * args.rows_per_file
        con.execute(f"""COPY (SELECT * EXCLUDE (_n) FROM sorted WHERE _n >= {a} AND _n < {b} ORDER BY _n)
                        TO '{out}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE {args.row_group})""")

    print(f'{kept:,} rows in {parts} file(s) under {args.output_dir}'
          + (f'  ({total_in - kept:,} rows without a position dropped)' if total_in != kept else ''))
    renamed = {o: n for o, n in names.items() if o != n}
    if renamed:
        print('renamed: ' + ', '.join(f'{o} -> {n}' for o, n in renamed.items()))


if __name__ == '__main__':
    main()
