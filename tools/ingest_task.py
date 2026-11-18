#!/usr/bin/env python
# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

"""
One ingest, start to finish, for the control panel (deploy/panel).

Run as a one-off task in the service's own image.  It fetches a table from
S3, converts it with make_parquet_catalog.py (or, for a catalog already
served, rewrites its metadata with describe_parquet_catalog.py), checks the
result, and leaves it in a staging folder with a report.  It never writes
to the folder the service reads; publishing is a separate step, taken by a
person who has read the report.

Everything is given in the environment:

    MODE            convert | describe
    NAME            table name, for the report
    SRC_BUCKET      bucket of the input
    SRC_KEY         convert:  key of the file to convert, or, ending in "/", a folder
                    of CSV files or of Parquet files that make up one table
    SRC_PREFIX      describe: folder of the catalog to describe, ending in "/"
    SCHEMA_KEY      optional (required to describe): key, in SRC_BUCKET or
                    SCHEMA_BUCKET, of the CSV of column types and metadata
    SCHEMA_BUCKET   bucket of SCHEMA_KEY, if not SRC_BUCKET
    DEST_BUCKET     bucket of the staging folder
    DEST_PREFIX     the staging folder, ending in "/"
    DESCRIPTION     what the table is (optional)
    RA, DEC         names of the position columns in the input (default ra, dec)
    WORK            a local directory with room for input and output (default /tmp/ingest)

The report is DEST_PREFIX + "_report.json".  It is written whether the
ingest worked or not, so the panel always has something to show; "ok" says
which.  The exit status is 0 only if it worked.
"""

import json
import os
import subprocess
import sys
import time
import traceback

REPORT = '_report.json'
HERE = os.path.dirname(os.path.abspath(__file__))


def env(name, default=''):
    return os.environ.get(name, default).strip()


def run_tool(args, report):
    """Run one of the tools beside this file; its output goes to the log and its tail to the report."""
    cmd = [sys.executable, os.path.join(HERE, args[0])] + args[1:]
    print('+', ' '.join(cmd), flush=True)
    # Run where there is room: the tools sort on disk when memory runs out,
    # in the directory they are started from.
    done = subprocess.run(cmd, cwd=os.environ.get('WORK_DIR') or HERE,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    print(done.stdout, flush=True)
    report['toolOutput'] = done.stdout[-4000:]
    if done.returncode != 0:
        last = [line for line in done.stdout.strip().splitlines() if line.strip()][-1:] or ['no output']
        raise RuntimeError(f'The conversion failed: {last[0]}')


def check(outdir, report):
    """What an administrator should see before publishing.  Each check stands alone."""
    import duckdb

    sys.path.insert(0, HERE)
    import votable_metadata

    files = sorted(f for f in os.listdir(outdir) if f.endswith('.parquet'))
    if not files:
        raise RuntimeError('The conversion wrote no files.')

    report['files'] = [{'name': f, 'bytes': os.path.getsize(os.path.join(outdir, f))} for f in files]
    report['bytes'] = sum(f['bytes'] for f in report['files'])

    con = duckdb.connect()
    src = f"read_parquet('{outdir}/*.parquet')"
    notes = report.setdefault('notes', [])

    def one(sql):
        return con.execute(sql).fetchone()

    report['rows'] = one(f'SELECT count(*) FROM {src}')[0]
    if report['rows'] == 0:
        notes.append('The table has no rows.')

    described = con.execute(f'DESCRIBE SELECT * FROM {src}').fetchall()
    names = [d[0] for d in described]

    about_table, about = '', {}
    try:
        row = con.execute("SELECT value FROM parquet_kv_metadata(?) WHERE key = ?",
                          [os.path.join(outdir, files[0]), votable_metadata.CONTENT_KEY.encode()]).fetchone()
        if row:
            about_table, about = votable_metadata.parse(bytes(row[0]).decode('utf-8', 'replace'))
    except Exception as e:
        notes.append(f'The column metadata could not be read back: {e}')
    report['description'] = about_table

    nulls = {}
    try:
        counts = one('SELECT ' + ', '.join(f'count("{n}")' for n in names) + f' FROM {src}')
        nulls = {n: report['rows'] - c for n, c in zip(names, counts)}
    except Exception as e:
        notes.append(f'Empty values could not be counted: {e}')

    report['columns'] = [{
        'name': n, 'type': t,
        'unit': about.get(n, {}).get('unit', ''), 'ucd': about.get(n, {}).get('ucd', ''),
        'description': about.get(n, {}).get('description', ''),
        'nulls': nulls.get(n),
    } for n, t in ((d[0], d[1]) for d in described)]

    own = [c for c in report['columns'] if c['name'] != 'hpx29']
    report['noDescription'] = sum(1 for c in own if not c['description'])
    report['noUnit'] = sum(1 for c in own if not c['unit'])
    empty = [c['name'] for c in own if report['rows'] and c['nulls'] == report['rows']]
    if empty:
        notes.append(f'{len(empty)} column(s) have no values at all: ' + ', '.join(empty[:12])
                     + (' ...' if len(empty) > 12 else ''))
    if report['noDescription']:
        notes.append(f"{report['noDescription']} of {len(own)} columns have no description.")

    for need in ('ra', 'dec', 'hpx29'):
        if need not in names:
            raise RuntimeError(f'The result has no "{need}" column; the service could not serve it.')

    try:
        r = one(f'SELECT min(ra), max(ra), min(dec), max(dec) FROM {src}')
        report['position'] = {'raMin': r[0], 'raMax': r[1], 'decMin': r[2], 'decMax': r[3]}
        if r[0] is not None and (r[0] < 0 or r[1] > 360 or r[2] < -90 or r[3] > 90):
            notes.append('Some positions are outside RA 0 to 360 or Dec -90 to 90.  Are the columns in degrees?')
        elif r[1] is not None and r[1] <= 24.0 and report['rows'] > 100:
            notes.append('No right ascension is above 24.  Is the column in hours instead of degrees?')
    except Exception as e:
        notes.append(f'The range of positions could not be read: {e}')

    try:
        same = one(f'SELECT count(*) - count(DISTINCT (ra, dec)) FROM {src}')[0]
        report['samePosition'] = same
        if same:
            notes.append(f'{same:,} rows share their exact position with another row.')
    except Exception as e:
        notes.append(f'Repeated positions could not be counted: {e}')

    try:
        cur = con.execute(f'SELECT * EXCLUDE (hpx29) FROM {src} LIMIT 8')
        heads = [d[0] for d in cur.description][:12]
        report['sample'] = {'columns': heads,
                            'rows': [[None if v is None else str(v)[:40] for v in row[:12]] for row in cur.fetchall()]}
    except Exception as e:
        notes.append(f'No sample rows: {e}')


def main():
    import boto3

    s3 = boto3.client('s3')
    mode = env('MODE', 'convert')
    dest_bucket, dest = env('DEST_BUCKET'), env('DEST_PREFIX')
    work = env('WORK', '/tmp/ingest')
    indir, outdir = os.path.join(work, 'in'), os.path.join(work, 'out')
    os.makedirs(indir, exist_ok=True)
    os.makedirs(outdir, exist_ok=True)
    os.environ['WORK_DIR'] = work

    if not dest_bucket or not dest.endswith('/') or dest.count('/') < 2:
        sys.exit('DEST_BUCKET and DEST_PREFIX (a folder inside the staging area, ending in "/") are required.')

    report = {'ok': False, 'name': env('NAME'), 'mode': mode, 'started': time.time(), 'notes': [],
              'source': env('SRC_KEY') or env('SRC_PREFIX')}
    status = 1

    try:
        schema = None
        if env('SCHEMA_KEY'):
            schema = os.path.join(indir, 'schema.csv')
            s3.download_file(env('SCHEMA_BUCKET') or env('SRC_BUCKET'), env('SCHEMA_KEY'), schema)

        if mode == 'convert':
            key = env('SRC_KEY')
            if key.endswith('/'):
                # A folder of files that are one table: all CSV, or all Parquet.
                src = os.path.join(indir, 'table')
                os.makedirs(src, exist_ok=True)
                kinds, size = set(), 0
                for page in s3.get_paginator('list_objects_v2').paginate(Bucket=env('SRC_BUCKET'), Prefix=key):
                    for o in page.get('Contents', []):
                        name = o['Key'][len(key):]
                        low = name.lower()
                        kind = ('parquet' if low.endswith(('.parquet', '.pq')) else
                                'csv' if low.endswith(('.csv', '.csv.gz')) else None)
                        if '/' in name or not kind:
                            continue
                        kinds.add(kind)
                        size += o['Size']
                        print(f'fetching {o["Key"]} ({o["Size"]:,} bytes)', flush=True)
                        s3.download_file(env('SRC_BUCKET'), o['Key'], os.path.join(src, name))
                if len(kinds) != 1:
                    raise RuntimeError('The folder must hold CSV files only or Parquet files only; it holds '
                                       + (' and '.join(sorted(kinds)) if kinds else 'neither') + '.')
                local = os.path.join(src, '*.parquet' if kinds == {'parquet'} else '*.csv*')
            else:
                local = os.path.join(indir, os.path.basename(key))
                size = s3.head_object(Bucket=env('SRC_BUCKET'), Key=key)['ContentLength']
                print(f'fetching s3://{env("SRC_BUCKET")}/{key} ({size:,} bytes)', flush=True)
                s3.download_file(env('SRC_BUCKET'), key, local)
            report['inputBytes'] = size

            args = ['make_parquet_catalog.py', local, outdir, '--ra', env('RA', 'ra'), '--dec', env('DEC', 'dec')]
            if schema:
                args += ['--schema', schema]
            if env('DESCRIPTION'):
                args += ['--description', env('DESCRIPTION')]
            run_tool(args, report)

        elif mode == 'describe':
            if not schema:
                raise RuntimeError('Describing a catalog needs the list of column units and descriptions.')
            prefix = env('SRC_PREFIX')
            src = os.path.join(indir, 'catalog')
            os.makedirs(src, exist_ok=True)
            for page in s3.get_paginator('list_objects_v2').paginate(Bucket=env('SRC_BUCKET'), Prefix=prefix):
                for o in page.get('Contents', []):
                    name = o['Key'][len(prefix):]
                    if '/' not in name and name.endswith('.parquet'):
                        print(f'fetching {o["Key"]} ({o["Size"]:,} bytes)', flush=True)
                        s3.download_file(env('SRC_BUCKET'), o['Key'], os.path.join(src, name))

            args = ['describe_parquet_catalog.py', src, outdir, '--schema', schema]
            if env('DESCRIPTION'):
                args += ['--description', env('DESCRIPTION')]
            run_tool(args, report)

        else:
            raise RuntimeError(f'Unknown MODE {mode!r}')

        check(outdir, report)

        # Staging holds this ingest and nothing from an earlier one.
        for page in s3.get_paginator('list_objects_v2').paginate(Bucket=dest_bucket, Prefix=dest):
            for o in page.get('Contents', []):
                s3.delete_object(Bucket=dest_bucket, Key=o['Key'])

        for f in report['files']:
            print(f'storing {dest}{f["name"]}', flush=True)
            s3.upload_file(os.path.join(outdir, f['name']), dest_bucket, dest + f['name'])

        report['ok'] = True
        status = 0

    except Exception as e:
        traceback.print_exc()
        report['error'] = str(e)[:1000]

    report['ended'] = time.time()
    s3.put_object(Bucket=dest_bucket, Key=dest + REPORT, Body=json.dumps(report, default=str).encode(),
                  ContentType='application/json')
    print(('staged ' if status == 0 else 'failed ') + dest, flush=True)
    sys.exit(status)


if __name__ == '__main__':
    main()
