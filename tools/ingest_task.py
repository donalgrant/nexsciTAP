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

    MODE            convert | describe | check
    NAME            table name, for the report
    SRC_BUCKET      bucket of the input
    SRC_KEY         convert:  key of the file to convert, or, ending in "/", a folder
                    of CSV files or of Parquet files that make up one table
    SRC_PREFIX      describe, check: folder of the catalog, ending in "/"
    RENAME          describe: columns to rename, as old=new,old=new
    SCHEMA_KEY      optional (required to describe unless RENAME is given): key, in SRC_BUCKET or
                    SCHEMA_BUCKET, of the CSV of column types and metadata
    SCHEMA_BUCKET   bucket of SCHEMA_KEY, if not SRC_BUCKET
    DEST_BUCKET     bucket of the staging folder
    DEST_PREFIX     the staging folder, ending in "/"
    DESCRIPTION     what the table is (optional)
    RA, DEC         names of the position columns in the input (default ra, dec)
    TASK_MEMORY_MB  the task's memory; the conversion keeps to half of it
    WORK            a local directory with room for input and output (default /tmp/ingest)

The report is DEST_PREFIX + "_report.json".  It is written whether the
ingest worked or not, so the panel always has something to show; "ok" says
which.  The exit status is 0 only if it worked.

MODE=check converts nothing and stores no table.  It fetches a catalog as
it is served and runs the same checks on it, writing only the report, so
the checks can be run again on a table at any time, and on a table that
was put in the bucket by hand.
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
    import re
    dropped = re.search(r'\(([\d,]+) rows without a position dropped\)', done.stdout)
    report['dropped'] = int(dropped.group(1).replace(',', '')) if dropped else 0
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
        raise RuntimeError('There are no Parquet files to check.')

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
    if report.get('dropped'):
        notes.append(f"{report['dropped']:,} rows of the input had no position and were left out; "
                     f"{report['rows']:,} of {report['rows'] + report['dropped']:,} remain.")

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

    # Units and UCDs are stored as given; say which ones other software may not understand.
    for kind, problem in (('unit', unit_problem), ('ucd', ucd_problem)):
        bad = []
        for c in report['columns']:
            try:
                why = problem(c[kind])
            except Exception as e:
                why = None
                print(f'could not check the {kind} of {c["name"]}: {e!r}', flush=True)
            if why:
                c[kind + 'Problem'] = why
                bad.append(f"{c['name']} ({c[kind]})")
        report['bad' + kind.capitalize() + 's'] = len(bad)
        if bad:
            word = 'units' if kind == 'unit' else 'UCDs'
            notes.append(f"{len(bad)} {word} are not written as the IVOA standard has them: "
                         + ', '.join(bad[:8]) + (' ...' if len(bad) > 8 else ''))

    own = [c for c in report['columns'] if c['name'] != 'hpx29']
    report['noDescription'] = sum(1 for c in own if not c['description'])
    report['noUnit'] = sum(1 for c in own if not c['unit'])
    empty = [c['name'] for c in own if report['rows'] and c['nulls'] == report['rows']]
    if empty:
        notes.append(f'{len(empty)} column(s) have no values at all: ' + ', '.join(empty[:12])
                     + (' ...' if len(empty) > 12 else ''))
    if report['noDescription']:
        notes.append(f"{report['noDescription']} of {len(own)} columns {'has' if report['noDescription'] == 1 else 'have'} no description.")

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

    search_test(con, src, outdir, files, report)

    try:
        cur = con.execute(f'SELECT * EXCLUDE (hpx29) FROM {src} LIMIT 8')
        heads = [d[0] for d in cur.description][:12]
        report['sample'] = {'columns': heads,
                            'rows': [[None if v is None else str(v)[:40] for v in row[:12]] for row in cur.fetchall()]}
    except Exception as e:
        notes.append(f'No sample rows: {e}')


def unit_problem(text):
    """None if `text` is a unit as the IVOA writes them (VOUnits), else what is wrong with it."""
    import warnings

    from astropy import units as u

    if not text:
        return None

    def reads(fmt):
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('error')
                u.Unit(text, format=fmt) if fmt else u.Unit(text)
            return True
        except Exception:
            return False

    if reads('vounit'):
        return None
    for fmt, name in (('cds', "the CDS form"), ('fits', "the FITS form"), (None, "astropy's own form")):
        if reads(fmt):
            return f'not a VOUnits unit (it is a unit in {name})'
    return 'not a unit any reader here understands'


def ucd_problem(text):
    """None if `text` is a valid UCD1+ from the IVOA's list of words, else what is wrong."""
    from astropy.io.votable import ucd

    if not text:
        return None
    try:
        return None if ucd.check_ucd(text, check_controlled_vocabulary=True, has_colon=False) else \
            'not a UCD the IVOA list allows (an unknown word, or words in the wrong order)'
    except Exception as e:
        return f'could not be checked ({e})'


def cone_cover(lon, lat, radius):
    """
    HEALPix cells sure to hold every point within `radius` degrees of a
    position, as (order, cells): the cell the position is in and its eight
    neighbours, at an order whose cells are at least four radii across.
    (The library's own cone search leaves out about one point in 500.)
    """
    import math

    import numpy as np
    from astropy import units as u
    from astropy_healpix import HEALPix

    order = int(max(0, min(20, math.floor(math.log2(58.6 / (4 * radius))))))     # a cell is about 58.6 deg / 2**order across
    hp = HEALPix(nside=2 ** order, order='nested')
    cell = int(hp.lonlat_to_healpix(lon * u.deg, lat * u.deg))
    with np.errstate(invalid='ignore'):
        around = np.ravel(hp.neighbours(cell))
    return order, sorted({cell} | {int(c) for c in around if c >= 0})


def search_test(con, src, outdir, files, report):
    """
    Would searches on these files give right answers?  The service finds
    rows by the index column alone, so a wrong index, or rows out of order,
    gives wrong or slow results with no error.  Three checks; any failure
    fails the ingest:

      * within each file, and from file to file, rows are in index order;
      * for a sample of rows, the index is the one their position gives;
      * a set of cone searches gives the same rows whether found through
        the index, as the service does, or by measuring the distance from
        every row in the table.
    """
    import numpy as np
    from astropy import units as u
    from astropy_healpix import lonlat_to_healpix

    test = report['searchTest'] = {}

    def one(sql):
        return con.execute(sql).fetchone()

    spans = []
    for f in files:
        path = os.path.join(outdir, f)
        back = one(f"SELECT count(*) FROM (SELECT hpx29, lag(hpx29) OVER (ORDER BY file_row_number) AS before "
                   f"FROM read_parquet('{path}', file_row_number=true)) WHERE hpx29 < before")[0]
        if back:
            raise RuntimeError(f'The rows of {f} are not in index order ({back:,} places where the index goes down).')
        spans.append(one(f"SELECT min(hpx29), max(hpx29) FROM read_parquet('{path}')"))
    for (_, hi), (lo, _), f in zip(spans, spans[1:], files[1:]):
        if lo < hi:
            raise RuntimeError(f'{f} starts before the file ahead of it ends, in index order.')
    test['sorted'] = True

    sample = con.execute(f'SELECT ra, dec, hpx29 FROM {src} USING SAMPLE 20000 ROWS').fetchnumpy()
    n = len(sample['ra'])
    if n:
        right = lonlat_to_healpix(sample['ra'] * u.deg, sample['dec'] * u.deg, 2 ** 29, order='nested')
        wrong = int(np.sum(np.asarray(right, dtype=np.int64) != sample['hpx29']))
        test['indexChecked'], test['indexWrong'] = n, wrong
        if wrong:
            raise RuntimeError(f'The index column does not match the positions: {wrong:,} of {n:,} sampled rows are wrong.')

    radii = [5 / 3600, 30 / 3600, 0.05, 0.2, 1.0]
    picks = np.linspace(0, n - 1, min(n, 25)).astype(int) if n else []
    cones = []
    for i, at in enumerate(picks):
        lon, lat, radius = float(sample['ra'][at]), float(sample['dec'][at]), radii[i % len(radii)]
        near = (f'2 * asin(sqrt(pow(sin(radians(dec - ({lat!r})) / 2), 2) + cos(radians({lat!r})) * cos(radians(dec)) '
                f'* pow(sin(radians(ra - ({lon!r})) / 2), 2))) <= radians({radius!r})')
        order, cells = cone_cover(lon, lat, radius)
        shift = 2 * (29 - order)
        through = ' OR '.join(f'hpx29 BETWEEN {c << shift} AND {((c + 1) << shift) - 1}' for c in cells)
        tally = 'count(*), coalesce(sum(hpx29 % 1000003), 0)'
        every = one(f'SELECT {tally} FROM {src} WHERE {near}')
        index = one(f'SELECT {tally} FROM {src} WHERE ({through}) AND {near}')
        cones.append({'ra': lon, 'dec': lat, 'radius': radius, 'count': int(every[0])})
        if tuple(every) != tuple(index):
            raise RuntimeError(f'A cone search of {radius * 3600:.0f} arcsec at RA {lon:.5f}, Dec {lat:.5f} finds '
                               f'{index[0]:,} rows through the index and {every[0]:,} by measuring every row.')
    test['cones'] = len(cones)
    test['rowsFound'] = sum(c['count'] for c in cones)
    # One of them, for the panel to ask the service once the table is published.
    middling = [c for c in cones if 0 < c['count'] < 200000 and c['radius'] >= 0.05] or cones
    report['checkCone'] = middling[len(middling) // 2] if middling else None


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
        sys.exit('DEST_BUCKET and DEST_PREFIX (a folder, ending in "/") are required.')

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

            args = ['make_parquet_catalog.py', local, outdir, '--ra', env('RA', 'ra'), '--dec', env('DEC', 'dec'),
                    '--work', os.path.join(work, 'scratch')]
            # Half of the task's memory for DuckDB; the rest is for Python,
            # which holds five million positions at a time, and for the system.
            if env('TASK_MEMORY_MB').isdigit():
                args += ['--memory', f"{int(env('TASK_MEMORY_MB')) // 2}MB"]
            if schema:
                args += ['--schema', schema]
            if env('DESCRIPTION'):
                args += ['--description', env('DESCRIPTION')]
            run_tool(args, report)

        elif mode in ('describe', 'check'):
            if mode == 'describe' and not schema and not env('RENAME'):
                raise RuntimeError('Describing a catalog needs the list of column units and descriptions, or columns to rename.')
            prefix = env('SRC_PREFIX')
            src = os.path.join(indir, 'catalog')
            os.makedirs(src, exist_ok=True)
            for page in s3.get_paginator('list_objects_v2').paginate(Bucket=env('SRC_BUCKET'), Prefix=prefix):
                for o in page.get('Contents', []):
                    name = o['Key'][len(prefix):]
                    if '/' not in name and name.endswith('.parquet'):
                        print(f'fetching {o["Key"]} ({o["Size"]:,} bytes)', flush=True)
                        s3.download_file(env('SRC_BUCKET'), o['Key'], os.path.join(src, name))

            if mode == 'check':
                outdir = src                    # the files as they are served
            else:
                args = ['describe_parquet_catalog.py', src, outdir] + (['--schema', schema] if schema else [])
                for pair in filter(None, (env('RENAME') or '').split(',')):
                    args += ['--rename', pair.strip()]
                if env('DESCRIPTION'):
                    args += ['--description', env('DESCRIPTION')]
                run_tool(args, report)

        else:
            raise RuntimeError(f'Unknown MODE {mode!r}')

        check(outdir, report)
        if mode == 'check':
            report.pop('sample', None)            # nothing is stored but the report
        else:
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
    print((('checked ' if mode == 'check' else 'staged ') if status == 0 else 'failed ') + dest, flush=True)
    sys.exit(status)


if __name__ == '__main__':
    main()
