#!/usr/bin/env python
# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

"""
Add column units and descriptions to a catalog that is already converted.

    describe_parquet_catalog.py INPUT OUTPUT_DIR --schema FILE [--description TEXT]

INPUT is the catalog's folder of Parquet files (a local directory, or an
s3:// prefix DuckDB can read), as make_parquet_catalog.py wrote it.  Each file
is written again under the same name in OUTPUT_DIR with the same rows, in the
same order and the same row groups, and with the metadata recorded in it;
copy the files over the old ones and the service shows the units and
descriptions at its next look.  Nothing about the data changes, so a file can
replace its old version at any moment, one at a time.

Parquet keeps a file's metadata in its footer, which cannot be changed
without writing the file again; that is all this does.

FILE is the CSV make_parquet_catalog.py takes for --schema: a header, a
column of names, and any of unit, ucd, utype, description.  A type column is
ignored here, since the data is not touched.  Columns FILE does not list keep
whatever the files already say about them.
"""

import argparse
import os
import sys

import duckdb
import votable_metadata
from make_parquet_catalog import read_schema


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('input', help='folder of Parquet files')
    p.add_argument('output_dir')
    p.add_argument('--schema', metavar='FILE', required=True)
    p.add_argument('--description', default=None, help='what the table is (default: keep what the files say)')
    args = p.parse_args()

    con = duckdb.connect()
    con.execute('SET preserve_insertion_order = true')

    _, declared = read_schema(args.schema)
    declared = {name.lower(): meta for name, meta in declared.items()}

    prefix = args.input.rstrip('/')
    files = [r[0] for r in con.execute('SELECT file FROM glob(?) ORDER BY 1', [prefix + '/*.parquet']).fetchall()]
    if not files:
        sys.exit(f'No Parquet files in {args.input}')
    if os.path.abspath(args.output_dir) == os.path.abspath(prefix):
        sys.exit('OUTPUT_DIR must not be the input folder.')
    os.makedirs(args.output_dir, exist_ok=True)

    cols = [(r[0], r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{prefix}/*.parquet')").fetchall()]

    # What the files say already, which the schema file adds to or replaces.
    row = con.execute('SELECT value FROM parquet_kv_metadata(?) WHERE key = ? ORDER BY file_name LIMIT 1',
                      [files[0], votable_metadata.CONTENT_KEY.encode()]).fetchone()
    was_table, was = votable_metadata.parse(bytes(row[0]).decode('utf-8', 'replace')) if row else ('', {})

    meta = {c: {**was.get(c, {}), **declared.get(c.lower(), {})} for c, _ in cols}
    for pos in ('ra', 'dec'):
        if pos in meta:
            meta[pos].setdefault('unit', 'deg')
    if 'hpx29' in meta:
        meta['hpx29'].setdefault('description', 'Nested HEALPix index of (ra, dec) at order 29, added on conversion; '
                                                'the files are sorted by it')

    text_cols = [c for c, t in cols if t.upper() == 'VARCHAR']
    if text_cols:
        widths = con.execute('SELECT ' + ', '.join(f'max(length("{c}"))' for c in text_cols)
                             + f" FROM read_parquet('{prefix}/*.parquet')").fetchone()
        for c, w in zip(text_cols, widths):
            meta[c]['width'] = max(1, w or 1)

    description = was_table if args.description is None else args.description
    votable = votable_metadata.build([(c, t, meta[c]) for c, t in cols], description)
    kv = (f"KV_METADATA {{'{votable_metadata.VERSION_KEY}': '1.0', "
          f"'{votable_metadata.CONTENT_KEY}': '{votable.replace(chr(39), chr(39) * 2)}'}}")

    for path in files:
        # The row groups are what make positional searches fast; keep their size.
        group = con.execute('SELECT max(row_group_num_rows) FROM parquet_metadata(?)', [path]).fetchone()[0]
        out = os.path.join(args.output_dir, os.path.basename(path))
        con.execute(f"""COPY (SELECT * FROM read_parquet('{path}'))
                        TO '{out}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE {group}, {kv})""")
        rows = con.execute('SELECT count(*) FROM read_parquet(?)', [out]).fetchone()[0]
        print(f'{os.path.basename(path)}: {rows:,} rows', flush=True)

    named = [c for c, _ in cols]
    unknown = sorted(set(declared) - {c.lower() for c in named})
    described = sum(1 for c in named if meta[c].get('description'))
    with_unit = sum(1 for c in named if meta[c].get('unit'))
    print(f'{described} of {len(named)} columns have a description, {with_unit} a unit'
          + (f';  {len(unknown)} columns in {args.schema} are not in the catalog' if unknown else ''))


if __name__ == '__main__':
    main()
