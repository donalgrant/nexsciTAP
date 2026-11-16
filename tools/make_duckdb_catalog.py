#!/usr/bin/env python
# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

"""
Build the small DuckDB database file nexsciTAP uses to serve Parquet catalogs.

The file holds no catalog data.  For each --table it creates a view over the
Parquet files (local path or s3:// URL, globs allowed) and registers the table
and its columns in TAP_SCHEMA, taking names and types from the Parquet schema.

Units, UCDs and descriptions of the columns, and the table's description, are
taken from the Parquet files when they carry them the IVOA way (written by
make_parquet_catalog.py; see votable_metadata.py).  --describe overrides the
table description.

Each view adds the unit-vector columns x, y, z that the ADQL translator's
exact cone test needs, computed from the RA and Dec columns, so the Parquet
files only have to carry RA, Dec and one spatial index column.

Example:

    make_duckdb_catalog.py --db pilot.duckdb \\
        --table "allwise=s3://my-bucket/catalogs/allwise/*.parquet" \\
        --table "euclid_q1=s3://my-bucket/catalogs/euclid_q1/*.parquet" \\
        --init-sql-file s3_init.sql \\
        --describe "allwise=AllWISE Source Catalog (subset)"

With --discover PREFIX, every folder directly under PREFIX that holds Parquet
files becomes a table named after the folder, unless a --table already covers
it.  A folder that cannot be served (a name that is not a plain identifier, or
files without the position and index columns) is reported and skipped, so one
bad upload does not take the other tables down.

The database is written beside its final name and moved into place, so a
service reading it never sees it half built.  With --if-changed the work is
skipped when the tables and their files (names, sizes, row counts and column
types) are the same as last time, which makes
the tool cheap to run on a timer:  new folders then appear in the service, and
removed ones disappear, without a restart.
"""

import argparse
import hashlib
import os
import re
import sys

import duckdb
import votable_metadata


def tap_type(duck_type):
    """(TAP_SCHEMA datatype, output format) for a DuckDB column type."""

    t = duck_type.upper()

    if t.startswith('DECIMAL') or t in ('DOUBLE', 'FLOAT', 'REAL'):
        return 'double', '22.14e'

    if t in ('BIGINT', 'UBIGINT', 'UINTEGER', 'HUGEINT'):
        return 'long', '20d'

    if t in ('INTEGER', 'SMALLINT', 'TINYINT', 'USMALLINT', 'UTINYINT', 'BOOLEAN'):
        return 'int', '11d'

    return 'char', None


def main():

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--db', required=True, help='DuckDB database file to create (overwritten).')
    p.add_argument('--table', action='append', default=[], metavar='NAME=PARQUET',
                   help='Table name and Parquet path or glob.  Repeat for more tables.')
    p.add_argument('--discover', metavar='PREFIX',
                   help='Also serve every folder of Parquet files directly under this '
                        'directory or URL prefix, as a table named after the folder.')
    p.add_argument('--require-column', action='append', default=[], metavar='COL',
                   help='Column a discovered folder must have besides the position columns '
                        '(the spatial index column).  Repeatable.')
    p.add_argument('--if-changed', action='store_true',
                   help='Do nothing if the tables and their files are unchanged since the last run.')
    p.add_argument('--describe', action='append', default=[], metavar='NAME=TEXT',
                   help='Description for a table.')
    p.add_argument('--racol', default='ra')
    p.add_argument('--deccol', default='dec')
    p.add_argument('--tap-schema', default='tap_schema')
    p.add_argument('--init-sql-file', help='SQL to run first (e.g. LOAD httpfs; CREATE SECRET ...).')
    args = p.parse_args()

    if not args.table and not args.discover:
        p.error('give at least one --table, or --discover')

    tmp = args.db + '.new'
    if os.path.exists(tmp):
        os.remove(tmp)

    con = duckdb.connect(tmp)

    if args.init_sql_file:
        for stmt in open(args.init_sql_file).read().split(';'):
            if stmt.strip():
                con.execute(stmt)

    def files_of(pattern):
        return [r[0] for r in con.execute('SELECT file FROM glob(?) ORDER BY 1', [pattern]).fetchall()]

    # (name, glob, discovered)
    tables = [tuple(spec.split('=', 1)) + (False,) for spec in args.table]

    if args.discover:
        prefix = args.discover if args.discover.endswith('/') else args.discover + '/'
        covered = [path for _, path, _ in tables]
        folders = sorted({f[len(prefix):].split('/', 1)[0] for f in files_of(prefix + '*/*.parquet')})

        for folder in folders:
            if any(path.startswith(prefix + folder + '/') for path in covered):
                continue
            if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', folder):
                print(f'skipped {prefix}{folder}/: the folder name cannot be a table name', file=sys.stderr)
                continue
            if any(folder.lower() == name.lower() for name, _, _ in tables):
                print(f'skipped {prefix}{folder}/: a table named {folder} already exists', file=sys.stderr)
                continue
            tables.append((folder, prefix + folder + '/*.parquet', True))

    # What is being served: the tables and every file behind them.
    # Sizes, row counts and column types come from each file's footer, so
    # a file replaced under the same name is noticed too.
    def footers_of(pattern):
        try:
            sizes = con.execute('SELECT file_name, num_rows, file_size_bytes, footer_size '
                                'FROM parquet_file_metadata(?) ORDER BY 1', [pattern]).fetchall()
            schema = con.execute("SELECT file_name, md5(string_agg(name || ':' || coalesce(type, '') || ':' || "
                                 "coalesce(logical_type, ''), ',' ORDER BY column_id)) "
                                 'FROM parquet_schema(?) GROUP BY 1 ORDER BY 1', [pattern]).fetchall()
            # What the columns mean is served too, so a change to it counts.
            meta = con.execute('SELECT file_name, md5(string_agg(md5(key) || md5(value), \',\' ORDER BY key)) '
                               'FROM parquet_kv_metadata(?) GROUP BY 1 ORDER BY 1', [pattern]).fetchall()
            return sizes, schema, meta
        except Exception as e:
            return str(e)

    listing = [(name, path, files_of(path), footers_of(path)) for name, path, _ in tables]
    signature = hashlib.sha256(repr(listing).encode()).hexdigest()
    signature_file = args.db + '.files'

    if (args.if_changed and os.path.exists(args.db) and os.path.exists(signature_file)
            and open(signature_file).read().strip() == signature):
        con.close()
        os.remove(tmp)
        return

    ts = args.tap_schema
    descriptions = dict(d.split('=', 1) for d in args.describe)

    con.execute(f'CREATE SCHEMA {ts}')
    con.execute(f"""CREATE TABLE {ts}.schemas (schema_name VARCHAR, utype VARCHAR,
                    description VARCHAR, schema_index INTEGER)""")
    con.execute(f"""CREATE TABLE {ts}.tables (schema_name VARCHAR, table_name VARCHAR,
                    table_type VARCHAR, utype VARCHAR, description VARCHAR, table_index INTEGER)""")
    con.execute(f"""CREATE TABLE {ts}.columns (table_name VARCHAR, column_name VARCHAR,
                    datatype VARCHAR, arraysize VARCHAR, xtype VARCHAR, "size" INTEGER,
                    description VARCHAR, utype VARCHAR, unit VARCHAR, ucd VARCHAR,
                    indexed INTEGER, principal INTEGER, std INTEGER, format VARCHAR,
                    column_index INTEGER)""")
    con.execute(f"""CREATE TABLE {ts}.keys (key_id VARCHAR, from_table VARCHAR,
                    target_table VARCHAR, utype VARCHAR, description VARCHAR)""")
    con.execute(f"""CREATE TABLE {ts}.key_columns (key_id VARCHAR, from_column VARCHAR,
                    target_column VARCHAR)""")

    con.execute(f"INSERT INTO {ts}.schemas VALUES ('main', NULL, 'Catalogs served from Parquet files.', 1)")
    con.execute(f"INSERT INTO {ts}.schemas VALUES ('{ts}', NULL, 'Standard TAP schema description tables.', 2)")

    tindex = 0
    ra, dec = args.racol, args.deccol

    for name, path, discovered in tables:

        try:
            have = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}

            missing = [c for c in [ra, dec] + args.require_column if c not in have]
            if missing:
                raise ValueError('no column ' + ', '.join(missing))

            clash = [c for c in ('x', 'y', 'z') if c in have]
            if clash:
                raise ValueError('columns named ' + ', '.join(clash) + ' clash with the ones the service adds')

            con.execute(f"""CREATE VIEW {name} AS
                SELECT *,
                       cos(radians({dec})) * cos(radians({ra})) AS x,
                       cos(radians({dec})) * sin(radians({ra})) AS y,
                       sin(radians({dec}))                      AS z
                FROM read_parquet('{path}')""")

            cols = con.execute(f'DESCRIBE {name}').fetchall()

            # Units and descriptions, from the first file that carries them.
            row = con.execute('SELECT value FROM parquet_kv_metadata(?) WHERE key = ? ORDER BY file_name LIMIT 1',
                              [path, votable_metadata.CONTENT_KEY.encode()]).fetchone()
            about_table, about = votable_metadata.parse(bytes(row[0]).decode('utf-8', 'replace')) if row else ('', {})

            # String widths from a sample, so fixed-width (IPAC) output has room.
            strcols = [c[0] for c in cols if tap_type(c[1])[0] == 'char']
            widths = {}
            if strcols:
                exprs = ', '.join(f'max(length(CAST("{c}" AS VARCHAR)))' for c in strcols)
                row = con.execute(f'SELECT {exprs} FROM (SELECT * FROM {name} LIMIT 20000)').fetchone()
                widths = {c: int((w or 1) * 1.5) + 2 for c, w in zip(strcols, row)}

        except Exception as e:
            # A folder someone uploaded must not be able to stop the service;
            # a table the operator named must not go missing silently.
            if not discovered:
                raise
            con.execute(f'DROP VIEW IF EXISTS {name}')
            print(f'skipped {path}: {e}', file=sys.stderr)
            continue

        tindex += 1
        con.execute(f'INSERT INTO {ts}.tables VALUES (?, ?, ?, NULL, ?, ?)',
                    ['main', name, 'table', descriptions.get(name, about_table), tindex])

        for i, c in enumerate(cols):
            cname, ctype = c[0], c[1]
            dtype, fmt = tap_type(ctype)
            if dtype == 'char':
                fmt = f'{widths[cname]}s'
            if cname in (ra, dec):
                fmt = '14.8f'
            principal = 0 if cname in ('x', 'y', 'z') else 1
            known = about.get(cname, {})
            if cname in ('x', 'y', 'z'):
                known = {'description': f'{cname} component of the unit vector of (ra, dec); computed by the service'}
            con.execute(f'INSERT INTO {ts}.columns VALUES (?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?, ?, 0, ?, ?)',
                        [name, cname, dtype, known.get('description', ''), known.get('utype'),
                         known.get('unit', ''), known.get('ucd'), 0, principal, fmt, i + 1])

        print(f'{name}: {len(cols)} columns from {path}' + ('  (discovered)' if discovered else ''))

    for i, t in enumerate(['schemas', 'tables', 'columns', 'keys', 'key_columns']):
        con.execute(f'INSERT INTO {ts}.tables VALUES (?, ?, ?, NULL, ?, ?)',
                    [ts, f'{ts}.{t}', 'table', f'TAP_SCHEMA {t}', tindex + i + 1])

    con.close()

    # Into place in one step: a reader has either the old file or the new one.
    os.replace(tmp, args.db)
    if os.path.exists(tmp + '.wal'):
        os.remove(tmp + '.wal')
    with open(signature_file, 'w') as f:
        f.write(signature + '\n')

    print(f'wrote {args.db}')


if __name__ == '__main__':
    main()
