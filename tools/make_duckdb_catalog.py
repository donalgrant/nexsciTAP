#!/usr/bin/env python
# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

"""
Build the small DuckDB database file nexsciTAP uses to serve Parquet catalogs.

The file holds no catalog data.  For each --table it creates a view over the
Parquet files (local path or s3:// URL, globs allowed) and registers the table
and its columns in TAP_SCHEMA, taking names and types from the Parquet schema.

Each view adds the unit-vector columns x, y, z that the ADQL translator's
exact cone test needs, computed from the RA and Dec columns, so the Parquet
files only have to carry RA, Dec and one spatial index column.

Example:

    make_duckdb_catalog.py --db pilot.duckdb \\
        --table "allwise=s3://my-bucket/catalogs/allwise/*.parquet" \\
        --table "euclid_q1=s3://my-bucket/catalogs/euclid_q1/*.parquet" \\
        --init-sql-file s3_init.sql \\
        --describe "allwise=AllWISE Source Catalog (subset)"
"""

import argparse
import os

import duckdb


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
    p.add_argument('--table', action='append', required=True, metavar='NAME=PARQUET',
                   help='Table name and Parquet path or glob.  Repeat for more tables.')
    p.add_argument('--describe', action='append', default=[], metavar='NAME=TEXT',
                   help='Description for a table.')
    p.add_argument('--racol', default='ra')
    p.add_argument('--deccol', default='dec')
    p.add_argument('--tap-schema', default='tap_schema')
    p.add_argument('--init-sql-file', help='SQL to run first (e.g. LOAD httpfs; CREATE SECRET ...).')
    args = p.parse_args()

    if os.path.exists(args.db):
        os.remove(args.db)

    con = duckdb.connect(args.db)

    if args.init_sql_file:
        for stmt in open(args.init_sql_file).read().split(';'):
            if stmt.strip():
                con.execute(stmt)

    ts = args.tap_schema
    descriptions = dict(d.split('=', 1) for d in args.describe)

    con.execute(f'CREATE SCHEMA {ts}')
    con.execute(f'''CREATE TABLE {ts}.schemas (schema_name VARCHAR, utype VARCHAR,
                    description VARCHAR, schema_index INTEGER)''')
    con.execute(f'''CREATE TABLE {ts}.tables (schema_name VARCHAR, table_name VARCHAR,
                    table_type VARCHAR, utype VARCHAR, description VARCHAR, table_index INTEGER)''')
    con.execute(f'''CREATE TABLE {ts}.columns (table_name VARCHAR, column_name VARCHAR,
                    datatype VARCHAR, arraysize VARCHAR, xtype VARCHAR, "size" INTEGER,
                    description VARCHAR, utype VARCHAR, unit VARCHAR, ucd VARCHAR,
                    indexed INTEGER, principal INTEGER, std INTEGER, format VARCHAR,
                    column_index INTEGER)''')
    con.execute(f'''CREATE TABLE {ts}.keys (key_id VARCHAR, from_table VARCHAR,
                    target_table VARCHAR, utype VARCHAR, description VARCHAR)''')
    con.execute(f'''CREATE TABLE {ts}.key_columns (key_id VARCHAR, from_column VARCHAR,
                    target_column VARCHAR)''')

    con.execute(f"INSERT INTO {ts}.schemas VALUES ('main', NULL, 'Catalogs served from Parquet files.', 1)")
    con.execute(f"INSERT INTO {ts}.schemas VALUES ('{ts}', NULL, 'Standard TAP schema description tables.', 2)")

    tindex = 0

    for spec in args.table:

        name, path = spec.split('=', 1)
        ra, dec = args.racol, args.deccol

        con.execute(f'''CREATE VIEW {name} AS
            SELECT *,
                   cos(radians({dec})) * cos(radians({ra})) AS x,
                   cos(radians({dec})) * sin(radians({ra})) AS y,
                   sin(radians({dec}))                      AS z
            FROM read_parquet('{path}')''')

        tindex += 1
        con.execute(f'INSERT INTO {ts}.tables VALUES (?, ?, ?, NULL, ?, ?)',
                    ['main', name, 'table', descriptions.get(name, ''), tindex])

        cols = con.execute(f'DESCRIBE {name}').fetchall()

        # String widths from a sample, so fixed-width (IPAC) output has room.
        strcols = [c[0] for c in cols if tap_type(c[1])[0] == 'char']
        widths = {}
        if strcols:
            exprs = ', '.join(f'max(length(CAST("{c}" AS VARCHAR)))' for c in strcols)
            row = con.execute(f'SELECT {exprs} FROM (SELECT * FROM {name} LIMIT 20000)').fetchone()
            widths = {c: int((w or 1) * 1.5) + 2 for c, w in zip(strcols, row)}

        for i, c in enumerate(cols):
            cname, ctype = c[0], c[1]
            dtype, fmt = tap_type(ctype)
            if dtype == 'char':
                fmt = f'{widths[cname]}s'
            if cname in (ra, dec):
                fmt = '14.8f'
            principal = 0 if cname in ('x', 'y', 'z') else 1
            con.execute(f'INSERT INTO {ts}.columns VALUES (?, ?, ?, NULL, NULL, NULL, ?, NULL, ?, NULL, ?, ?, 0, ?, ?)',
                        [name, cname, dtype, '', '', 0, principal, fmt, i + 1])

        print(f'{name}: {len(cols)} columns from {path}')

    for i, t in enumerate(['schemas', 'tables', 'columns', 'keys', 'key_columns']):
        con.execute(f'INSERT INTO {ts}.tables VALUES (?, ?, ?, NULL, ?, ?)',
                    [ts, f'{ts}.{t}', 'table', f'TAP_SCHEMA {t}', tindex + i + 1])

    con.close()
    print(f'wrote {args.db}')


if __name__ == '__main__':
    main()
