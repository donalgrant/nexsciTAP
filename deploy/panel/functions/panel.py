# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

"""
The control panel's functions:  views (stage 1) and controls (stage 2).

One Lambda function answers every "/api/<view>" request of the panel page,
carries out the controls sent to "/api/control", and on a timer runs the
canary and keeps the standby schedule.

Views read the service's records.  Controls change four things and no
others:  how many tasks the service runs (none is standby), what the load
balancer answers while there are none, which image is tagged "live", and
whether a build is started.  An async job is stopped by asking the service
itself, as any TAP client may.  Every control used is written to a record.

Views:  status, load, activity, jobs, datasets, columns, ingestlog, cost,
components, logs, record.
Controls:  tasks, settle, schedule, rebuild, rollback, abort, and for
datasets (stage 3):  upload, ingest, describe, publish, discard, hide,
unhide, remove, restore.

Datasets move between five areas of the catalog bucket.  The service reads
only the first:

    catalogs/<folder>/            what the service serves
    staging/<folder>/             a converted table and its report, awaiting a decision
    hidden/<folder>/              a table taken out of the service, kept whole
    versions/<folder>/<when>/     what a folder held before it was replaced or removed

Uploads arrive in the panel's own bucket, under upload/, and are converted
by a one-off Fargate task (tools/ingest_task.py in the service's image).
The panel never deletes a served table:  "remove" moves it to versions/.

Environment:

    SERVICE_STACK   name of the service's CloudFormation stack (also its
                    ECS cluster and service, and "/ecs/<name>" its log group)
    SERVICE_URL     the service's public address, https://...
    PANEL_SECRET    value of the X-Panel-Secret header that lets these
                    functions through the service's access check
    CATALOG_BUCKET, CATALOG_PREFIX   where the catalogs are
    STATE_BUCKET    the panel's bucket
    ECR_PREFIX      image repositories are <prefix>-tap, -proxy, -firefly
    BUILD_PROJECT   the service's CodeBuild project
    TASK_CAP        most tasks the panel will ever ask for
    INGEST_CLUSTER, INGEST_TASKDEF, INGEST_SUBNETS, INGEST_SG, INGEST_LOGS
                    where and how the one-off conversion task runs
"""

import csv
import io
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

STACK = os.environ.get('SERVICE_STACK', '')
SERVICE_URL = os.environ.get('SERVICE_URL', '').rstrip('/')
SECRET = os.environ.get('PANEL_SECRET', '')
CATALOG_BUCKET = os.environ.get('CATALOG_BUCKET', '')
CATALOG_PREFIX = os.environ.get('CATALOG_PREFIX', 'catalogs').strip('/')
STATE_BUCKET = os.environ.get('STATE_BUCKET', '')
ECR_PREFIX = os.environ.get('ECR_PREFIX', 'aiab')
BUILD_PROJECT = os.environ.get('BUILD_PROJECT', '')
TASK_CAP = int(os.environ.get('TASK_CAP', '3'))
LOG_GROUP = '/ecs/' + STACK
INGEST_CLUSTER = os.environ.get('INGEST_CLUSTER', '')
INGEST_TASKDEF = os.environ.get('INGEST_TASKDEF', '')
INGEST_SUBNETS = [x for x in os.environ.get('INGEST_SUBNETS', '').split(',') if x]
INGEST_SG = os.environ.get('INGEST_SG', '')
INGEST_LOGS = os.environ.get('INGEST_LOGS', '')

HERE = os.path.dirname(os.path.abspath(__file__))

_clients = {}


def aws(name, region=None):
    key = (name, region)
    if key not in _clients:
        _clients[key] = boto3.client(name, region_name=region) if region else boto3.client(name)
    return _clients[key]


def now():
    return datetime.now(timezone.utc)


# ---- the panel's own state ------------------------------------------------

def load_state(name, default=None):
    try:
        body = aws('s3').get_object(Bucket=STATE_BUCKET, Key=f'state/{name}.json')['Body'].read()
        return json.loads(body)
    except (ClientError, ValueError):
        return default


def save_state(name, value):
    aws('s3').put_object(Bucket=STATE_BUCKET, Key=f'state/{name}.json',
                         Body=json.dumps(value).encode(), ContentType='application/json')


def cached(name, max_age, make):
    """A stored answer if it is young enough, else a new one, stored."""
    held = load_state(name)
    if held and time.time() - held.get('at', 0) < max_age:
        return held
    try:
        value = make()
    except Exception as e:
        if held:                    # an old answer beats none
            held['stale'] = str(e)[:300]
            return held
        raise
    value['at'] = time.time()
    save_state(name, value)
    return value


# ---- asking the service ---------------------------------------------------

class ServiceError(Exception):
    pass


def service(path, data=None, timeout=30):
    """A request to the service's TAP through the panel's door; (status, text).  POST when data is given."""
    if not SECRET:
        raise ServiceError('The panel has no way into the service yet (no panel secret set).')
    body = urllib.parse.urlencode(data).encode() if data is not None else None
    req = urllib.request.Request(SERVICE_URL + '/TAP' + path, data=body,
                                 headers={'X-Panel-Secret': SECRET, 'User-Agent': 'aiab-panel'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode('utf-8', 'replace')
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8', 'replace')
    except Exception as e:
        raise ServiceError(f'No answer from the service: {e}')


def tap(query, timeout=50):
    """Run a sync query on the service through the panel's door; rows as dicts, seconds taken."""
    if not SECRET:
        raise ServiceError('The panel has no way into the service yet (no panel secret set).')

    url = SERVICE_URL + '/TAP/sync?' + urllib.parse.urlencode({'QUERY': query, 'FORMAT': 'csv'})
    req = urllib.request.Request(url, headers={'X-Panel-Secret': SECRET, 'User-Agent': 'aiab-panel'})
    started = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            text = r.read().decode('utf-8', 'replace')
    except urllib.error.HTTPError as e:
        if e.code == 403:
            raise ServiceError('The service refused the panel (403).  Its access check does not know the panel yet.')
        raise ServiceError(f'The service answered {e.code}.')
    except Exception as e:
        raise ServiceError(f'No answer from the service: {e}')
    took = time.time() - started

    if text.lstrip().startswith('<'):
        raise ServiceError('The service returned an error document instead of a table.')
    return list(csv.DictReader(io.StringIO(text))), took


# ---- views ----------------------------------------------------------------

def ecs_service():
    r = aws('ecs').describe_services(cluster=STACK, services=[STACK])
    if not r['services']:
        raise RuntimeError(f'No ECS service named {STACK}')
    return r['services'][0]


def view_status(params):
    svc = ecs_service()
    canary = load_state('canary', {})
    last = canary.get('last')

    desired, running = svc['desiredCount'], svc['runningCount']
    if desired == 0:
        state = 'standby'
    elif running == 0:
        state = 'starting'          # asked for, not yet up; 'failing' is for a service that is up and wrong
    elif last and last.get('ok') is False and time.time() - last.get('at', 0) < 900:
        state = 'failing'
    else:
        state = 'running'

    deployments = [{
        'status': d['status'], 'rollout': d.get('rolloutState', ''), 'running': d['runningCount'],
        'desired': d['desiredCount'], 'created': d['createdAt'].timestamp(),
        'taskDefinition': d['taskDefinition'].split('/')[-1],
    } for d in svc.get('deployments', [])]

    events = [{'at': e['createdAt'].timestamp(), 'message': e['message']} for e in svc.get('events', [])[:8]]

    history = canary.get('history', [])
    day = [h for h in history if time.time() - h['at'] < 86400]

    paused = None
    try:
        paused = is_paused()
    except ClientError:
        pass
    if state == 'running' and paused:
        state = 'starting'          # a task is up, but users still get the paused page

    return {
        'state': state, 'desired': desired, 'running': running, 'pending': svc.get('pendingCount', 0),
        'taskCap': TASK_CAP, 'serviceUrl': SERVICE_URL, 'paused': paused,
        'schedule': load_state('schedule', dict(DEFAULT_SCHEDULE)), 'scheduleNow': scheduled_state(),
        'canary': {'last': last, 'history': history[-72:],
                   'day': {'runs': len(day), 'failed': sum(1 for h in day if h['ok'] is False),
                           'skipped': sum(1 for h in day if h['ok'] is None)},
                   'door': bool(SECRET)},
        'deployments': deployments, 'events': events,
    }


def metric(i, namespace, name, dims, stat, period):
    return {'Id': f'm{i}', 'ReturnData': True, 'MetricStat': {
        'Metric': {'Namespace': namespace, 'MetricName': name,
                   'Dimensions': [{'Name': k, 'Value': v} for k, v in dims.items()]},
        'Period': period, 'Stat': stat}}


def service_resources():
    def make():
        out = {}
        pages = aws('cloudformation').get_paginator('list_stack_resources').paginate(StackName=STACK)
        for page in pages:
            for r in page['StackResourceSummaries']:
                out[r['LogicalResourceId']] = r['PhysicalResourceId']
        return {'ids': out}
    return cached('resources', 3600, make)['ids']


def view_load(params):
    hours = max(1, min(int(params.get('hours', '24')), 168))
    period = 300 if hours <= 24 else 3600
    ids = service_resources()

    lb = ids.get('LoadBalancer', '').split('loadbalancer/')[-1]
    tg = 'targetgroup/' + ids.get('TargetGroup', '').split('targetgroup/')[-1]
    ecs = {'ClusterName': STACK, 'ServiceName': STACK}
    alb = {'LoadBalancer': lb}

    want = [
        ('cpu', 'AWS/ECS', 'CPUUtilization', ecs, 'Average'),
        ('cpuMax', 'AWS/ECS', 'CPUUtilization', ecs, 'Maximum'),
        ('memory', 'AWS/ECS', 'MemoryUtilization', ecs, 'Average'),
        ('memoryMax', 'AWS/ECS', 'MemoryUtilization', ecs, 'Maximum'),
        ('requests', 'AWS/ApplicationELB', 'RequestCount', alb, 'Sum'),
        ('responseP50', 'AWS/ApplicationELB', 'TargetResponseTime', alb, 'p50'),
        ('responseP95', 'AWS/ApplicationELB', 'TargetResponseTime', alb, 'p95'),
        ('errors5xx', 'AWS/ApplicationELB', 'HTTPCode_Target_5XX_Count', alb, 'Sum'),
        ('gateway5xx', 'AWS/ApplicationELB', 'HTTPCode_ELB_5XX_Count', alb, 'Sum'),
        ('refused429', 'AWS/ApplicationELB', 'HTTPCode_Target_4XX_Count', alb, 'Sum'),
        ('healthy', 'AWS/ApplicationELB', 'HealthyHostCount', {'LoadBalancer': lb, 'TargetGroup': tg}, 'Minimum'),
    ]

    end = now().replace(second=0, microsecond=0)
    r = aws('cloudwatch').get_metric_data(
        MetricDataQueries=[metric(i, ns, name, dims, stat, period) for i, (_, ns, name, dims, stat) in enumerate(want)],
        StartTime=end - timedelta(hours=hours), EndTime=end, ScanBy='TimestampAscending')

    series = {}
    for res in r['MetricDataResults']:
        key = want[int(res['Id'][1:])][0]
        series[key] = [[t.timestamp(), round(v, 4)] for t, v in zip(res['Timestamps'], res['Values'])]

    work = None
    fs = ids.get('WorkFileSystem')
    if fs:
        try:
            d = aws('efs').describe_file_systems(FileSystemId=fs)['FileSystems'][0]
            work = {'bytes': d['SizeInBytes']['Value']}
        except ClientError:
            pass

    params_now = stack_parameters()
    return {'hours': hours, 'period': period, 'series': series, 'workArea': work,
            'limits': {k: params_now.get(k) for k in
                       ('ClientLimit', 'AsyncLimit', 'AsyncClientLimit', 'QueryMemory', 'QueryTimeout', 'MaxRecLimit')}}


def stack_parameters():
    def make():
        st = aws('cloudformation').describe_stacks(StackName=STACK)['Stacks'][0]
        return {'params': {p['ParameterKey']: p.get('ParameterValue', '') for p in st.get('Parameters', [])},
                'status': st['StackStatus'],
                'updated': (st.get('LastUpdatedTime') or st['CreationTime']).timestamp()}
    return cached('stack', 300, make)['params']


QUERY_WORD = 'TAPQ'
VERSION_WORD = 'TAPV'


def log_events(pattern, since, limit=20000, stream_prefix=None):
    kw = {'logGroupName': LOG_GROUP, 'startTime': int(since * 1000), 'filterPattern': pattern}
    if stream_prefix:
        kw['logStreamNamePrefix'] = stream_prefix
    out, deadline = [], time.time() + 20
    for page in aws('logs').get_paginator('filter_log_events').paginate(**kw):
        out.extend(page['events'])
        if len(out) >= limit or time.time() > deadline:
            break
    return out


def query_records(hours):
    recs = []
    for e in log_events(f'"{QUERY_WORD}"', time.time() - hours * 3600):
        m = e['message']
        i = m.find(QUERY_WORD + ' {')
        if i < 0:
            continue
        try:
            recs.append(json.loads(m[i + len(QUERY_WORD) + 1:]))
        except ValueError:
            pass
    return recs


def is_query(r):
    """A record of a query being answered: a sync request, or an async job ending."""
    return 'q' in r and (r.get('job') or r.get('path') == '/sync')


def view_activity(params):
    hours = max(1, min(int(params.get('hours', '24')), 168))
    recs = query_records(hours)
    queries = [r for r in recs if is_query(r)]

    end = int(time.time() // 3600 + 1) * 3600
    buckets = {end - 3600 * (i + 1): {'ok': 0, 'failed': 0} for i in range(hours)}
    for r in queries:
        b = int(r['t'] // 3600) * 3600
        if b in buckets:
            buckets[b]['failed' if r.get('phase') == 'ERROR' else 'ok'] += 1

    clients = {}
    for r in recs:
        c = clients.setdefault(r.get('client', '?'), {'requests': 0, 'queries': 0, 'failed': 0, 'ms': 0, 'rows': 0})
        c['requests'] += 1
        if is_query(r):
            c['queries'] += 1
            c['ms'] += r.get('ms', 0)
            c['rows'] += r.get('rows', 0)
            c['failed'] += r.get('phase') == 'ERROR'

    def brief(r):
        return {k: r.get(k) for k in ('t', 'ms', 'client', 'path', 'job', 'phase', 'rows', 'fmt', 'q', 'err')}

    done = [r for r in queries if r.get('phase') == 'COMPLETED']
    times = sorted(r['ms'] for r in done)

    return {
        'hours': hours, 'recorded': bool(recs),
        'perHour': [[t, v['ok'], v['failed']] for t, v in sorted(buckets.items())],
        'totals': {'requests': len(recs), 'queries': len(queries),
                   'failed': sum(1 for r in queries if r.get('phase') == 'ERROR'),
                   'async': sum(1 for r in queries if r.get('job')),
                   'medianMs': times[len(times) // 2] if times else None,
                   'p95Ms': times[int(len(times) * 0.95)] if times else None},
        'clients': sorted(({'client': k, **v} for k, v in clients.items()),
                          key=lambda c: -c['requests'])[:15],
        'slow': [brief(r) for r in sorted(done, key=lambda r: -r['ms'])[:10]],
        'failed': [brief(r) for r in sorted((r for r in queries if r.get('phase') == 'ERROR'),
                                            key=lambda r: -r['t'])[:15]],
        'recent': [brief(r) for r in sorted(queries, key=lambda r: -r['t'])[:25]],
    }


def catalog_folders():
    """What is in the catalog area of the bucket, by folder."""
    folders = {}
    prefix = CATALOG_PREFIX + '/'
    for page in aws('s3').get_paginator('list_objects_v2').paginate(Bucket=CATALOG_BUCKET, Prefix=prefix):
        for o in page.get('Contents', []):
            rest = o['Key'][len(prefix):]
            if '/' not in rest or not rest.endswith('.parquet'):
                continue
            f = folders.setdefault(rest.split('/')[0], {'files': 0, 'bytes': 0, 'modified': 0})
            f['files'] += 1
            f['bytes'] += o['Size']
            f['modified'] = max(f['modified'], o['LastModified'].timestamp())
    return folders


def table_folders(folders):
    """The stack's TABLES setting renames folders: 'allwise=allwise_k0_8/*.parquet'."""
    names = {}
    for item in (stack_parameters().get('Tables') or '').split(','):
        if '=' in item:
            name, path = item.split('=', 1)
            names[path.strip().split('/')[0]] = name.strip()
    return {names.get(folder, folder): folder for folder in folders}


def view_datasets(params):
    folders = catalog_folders()
    by_table = table_folders(folders)
    signature = json.dumps(sorted((k, v['files'], v['bytes'], v['modified']) for k, v in folders.items()))

    def describe():
        tables, _ = tap('SELECT table_name, description FROM TAP_SCHEMA.tables')
        columns, _ = tap('SELECT table_name, column_name, unit, description FROM TAP_SCHEMA.columns')
        out = {}
        for t in tables:
            name = t['table_name']
            if name.lower().startswith('tap_schema.'):
                continue
            cols = [c for c in columns if c['table_name'] == name]
            rows, _ = tap(f'SELECT count(*) AS n FROM {name}')
            out[name] = {
                'description': t.get('description', ''), 'columns': len(cols),
                'noUnit': sum(1 for c in cols if not (c.get('unit') or '').strip()),
                'noDescription': sum(1 for c in cols if not (c.get('description') or '').strip()),
                'rows': int(rows[0]['n']),
            }
        return {'signature': signature, 'tables': out}

    served, problem = {}, None
    held = load_state('datasets')
    if held and held.get('signature') == signature and time.time() - held.get('at', 0) < 86400:
        served = held['tables']
    else:
        try:
            fresh = describe()
            fresh['at'] = time.time()
            save_state('datasets', fresh)
            served = fresh['tables']
        except ServiceError as e:
            problem = str(e)
            served = (held or {}).get('tables', {})

    use = {}
    try:
        names = sorted(set(by_table) | set(served), key=len, reverse=True)
        for r in query_records(24):
            if not is_query(r):
                continue
            q = r['q'].lower()
            for name in names:
                if re.search(r'(?<![\w.])' + re.escape(name.lower()) + r'(?![\w])', q):
                    use[name] = use.get(name, 0) + 1
                    break
    except ClientError:
        pass

    out = []
    for name in sorted(set(by_table) | set(served)):
        folder = by_table.get(name)
        f = folders.get(folder, {})
        s = served.get(name, {})
        out.append({'table': name, 'folder': folder, 'files': f.get('files'), 'bytes': f.get('bytes'),
                    'modified': f.get('modified'), 'served': name in served if served else None,
                    'queries24h': use.get(name, 0), **s})
    return {'tables': out, 'problem': problem, 'bucket': CATALOG_BUCKET, 'prefix': CATALOG_PREFIX,
            'totalBytes': sum(f['bytes'] for f in folders.values()),
            'staged': staged(), 'ingests': ingests(), 'hidden': area_folders('hidden/'),
            'versions': versions(), 'moves': moves()}


def view_cost(params):
    def make():
        ce = aws('ce', 'us-east-1')
        today = now().date()
        start = today - timedelta(days=14)
        r = ce.get_cost_and_usage(
            TimePeriod={'Start': start.isoformat(), 'End': (today + timedelta(days=1)).isoformat()},
            Granularity='DAILY', Metrics=['UnblendedCost'],
            GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}])
        days = []
        for d in r['ResultsByTime']:
            parts = {g['Keys'][0]: float(g['Metrics']['UnblendedCost']['Amount']) for g in d['Groups']}
            parts = {k: round(v, 4) for k, v in parts.items() if v >= 0.005}
            days.append({'day': d['TimePeriod']['Start'], 'total': round(sum(parts.values()), 2),
                         'parts': parts, 'estimated': d.get('Estimated', False)})

        month_start = today.replace(day=1)
        m = ce.get_cost_and_usage(
            TimePeriod={'Start': month_start.isoformat(), 'End': (today + timedelta(days=1)).isoformat()},
            Granularity='MONTHLY', Metrics=['UnblendedCost'])
        month = sum(float(x['Total']['UnblendedCost']['Amount']) for x in m['ResultsByTime'])

        forecast = None
        try:
            nxt = (month_start + timedelta(days=32)).replace(day=1)
            if today + timedelta(days=1) < nxt:
                f = ce.get_cost_forecast(
                    TimePeriod={'Start': (today + timedelta(days=1)).isoformat(), 'End': nxt.isoformat()},
                    Metric='UNBLENDED_COST', Granularity='MONTHLY')
                forecast = round(month + float(f['Total']['Amount']), 2)
        except ClientError:
            pass

        budgets = []
        try:
            account = aws('sts').get_caller_identity()['Account']
            for b in aws('budgets', 'us-east-1').describe_budgets(AccountId=account).get('Budgets', []):
                budgets.append({'name': b['BudgetName'], 'limit': float(b['BudgetLimit']['Amount']),
                                'unit': b['TimeUnit'],
                                'actual': float(b.get('CalculatedSpend', {}).get('ActualSpend', {}).get('Amount', 0))})
        except ClientError:
            pass

        return {'days': days, 'month': round(month, 2), 'forecast': forecast, 'budgets': budgets}

    # Each look at Cost Explorer is charged for, so a few a day.
    out = dict(cached('cost', 6 * 3600, make))
    out['scope'] = 'whole account'
    return out


def image_tags(repo):
    """The tags on the image now tagged 'live', and when it was pushed."""
    try:
        d = aws('ecr').describe_images(repositoryName=repo, imageIds=[{'imageTag': 'live'}])['imageDetails'][0]
        tags = [t for t in d.get('imageTags', []) if t != 'live']
        return {'repo': repo, 'tags': tags, 'pushed': d['imagePushedAt'].timestamp(), 'digest': d['imageDigest'][7:19]}
    except (ClientError, IndexError, KeyError):
        return {'repo': repo}


def earlier_images():
    """The service images that can be run again, newest first."""
    try:
        out = []
        for page in aws('ecr').get_paginator('describe_images').paginate(repositoryName=f'{ECR_PREFIX}-tap'):
            for d in page['imageDetails']:
                tags = d.get('imageTags', [])
                commit = [t for t in tags if t != 'live']
                if commit:
                    out.append({'tag': commit[0], 'pushed': d['imagePushedAt'].timestamp(), 'live': 'live' in tags})
        return sorted(out, key=lambda i: -i['pushed'])[:12]
    except ClientError:
        return []


def latest_release(repo):
    req = urllib.request.Request(f'https://api.github.com/repos/{repo}/releases/latest',
                                 headers={'User-Agent': 'aiab-panel', 'Accept': 'application/vnd.github+json'})
    with urllib.request.urlopen(req, timeout=10) as r:
        d = json.loads(r.read())
    return {'repo': repo, 'tag': d.get('tag_name'), 'published': d.get('published_at')}


def view_components(params):
    svc = ecs_service()
    td = aws('ecs').describe_task_definition(taskDefinition=svc['taskDefinition'])['taskDefinition']
    containers = [{'name': c['name'], 'image': c['image'].split('/')[-1]} for c in td['containerDefinitions']]

    # The service names its versions once, when a task starts; no task now
    # running started before the oldest deployment still in place.
    oldest = min(d['createdAt'].timestamp() for d in svc['deployments']) - 600

    def reported():
        found = None
        for e in reversed(log_events(f'"{VERSION_WORD}"', oldest)):
            i = e['message'].find(VERSION_WORD + ' {')
            if i >= 0:
                found = json.loads(e['message'][i + len(VERSION_WORD) + 1:])
                found['reported'] = e['timestamp'] / 1000
                break
        return {'versions': found, 'task': svc['taskDefinition']}

    versions = None
    try:
        held = cached('versions', 600, reported)
        if held.get('task') != svc['taskDefinition']:
            held = reported()
            held['at'] = time.time()
            save_state('versions', held)
        versions = held['versions']
    except (ClientError, ValueError):
        pass

    def upstream():
        out = []
        for repo in ('Caltech-IPAC/nexsciTAP', 'Caltech-IPAC/firefly', 'duckdb/duckdb'):
            try:
                out.append(latest_release(repo))
            except Exception as e:
                out.append({'repo': repo, 'error': str(e)[:120]})
        return {'releases': out}

    builds = []
    if BUILD_PROJECT:
        try:
            cb = aws('codebuild')
            ids = cb.list_builds_for_project(projectName=BUILD_PROJECT, sortOrder='DESCENDING')['ids'][:10]
            if ids:
                for b in cb.batch_get_builds(ids=ids)['builds']:
                    builds.append({'number': b.get('buildNumber'), 'status': b['buildStatus'],
                                   'started': b['startTime'].timestamp(),
                                   'ended': b['endTime'].timestamp() if b.get('endTime') else None,
                                   'commit': (b.get('resolvedSourceVersion') or '')[:7],
                                   'by': b.get('initiator', '')})
        except ClientError:
            pass

    stack_parameters()
    stack = load_state('stack', {})
    p = stack.get('params', {})

    return {
        'stack': {'name': STACK, 'status': stack.get('status'), 'updated': stack.get('updated'),
                  'sourceRepo': p.get('SourceRepo'), 'sourceRef': p.get('SourceRef'),
                  'allowed': [p.get('AllowedCidr1'), p.get('AllowedCidr2')]},
        'task': {'definition': td['taskDefinitionArn'].split('/')[-1], 'cpu': td.get('cpu'),
                 'memory': td.get('memory'), 'containers': containers},
        'images': [image_tags(f'{ECR_PREFIX}-{n}') for n in ('tap', 'proxy', 'firefly')],
        'earlier': earlier_images(),
        'versions': versions,
        'upstream': cached('upstream', 86400, upstream)['releases'],
        'builds': builds,
    }


def view_logs(params):
    minutes = max(1, min(int(params.get('minutes', '60')), 1440))
    container = params.get('container', '')
    text = params.get('q', '').strip()

    # Stream names are <prefix>/<container>/<task id>.
    kw = {'logGroupName': LOG_GROUP, 'startTime': int((time.time() - minutes * 60) * 1000)}
    if text:
        kw['filterPattern'] = '"' + text.replace('"', '') + '"'

    lines, deadline = [], time.time() + 15
    for page in aws('logs').get_paginator('filter_log_events').paginate(**kw):
        for e in page['events']:
            parts = e['logStreamName'].split('/')
            name = parts[1] if len(parts) > 2 else e['logStreamName']
            if container and name != container:
                continue
            lines.append({'at': e['timestamp'] / 1000, 'container': name,
                          'task': parts[-1][:8], 'message': e['message'][:2000]})
        if len(lines) > 5000 or time.time() > deadline:
            break
    return {'minutes': minutes, 'container': container, 'q': text, 'lines': lines[-400:], 'found': len(lines)}


# ---- controls -------------------------------------------------------------

class Refused(Exception):
    """A control that will not be carried out; the message is for the administrator."""


PAUSED_PAGE = (
    '<!doctype html><html lang="en"><head><meta charset="utf-8">'
    '<meta name="viewport" content="width=device-width, initial-scale=1"><title>Archive paused</title>'
    '<style>body{font:16px/1.5 -apple-system,Segoe UI,Helvetica,Arial,sans-serif;max-width:34em;'
    'margin:18vh auto;padding:0 1.5em;color:#222}h1{font-size:1.4em}p{color:#555}</style></head><body>'
    '<h1>This archive is paused</h1>'
    '<p>The service is switched off to save cost, or is just starting.  '
    'Nothing is lost:  the catalogs and the results of earlier jobs are kept.</p>'
    '<p>If it is starting, it will answer within a few minutes.  Otherwise, ask its administrator to start it.</p>'
    '</body></html>')

DEFAULT_SCHEDULE = {'enabled': False, 'days': [0, 1, 2, 3, 4], 'start': '07:00', 'end': '19:00',
                    'tz': 'America/Los_Angeles', 'count': 1}


def record(who, action, detail):
    """Append to the record of controls used."""
    entries = load_state('record', {}).get('entries', [])
    entries.append({'at': time.time(), 'who': who, 'action': action, 'detail': detail})
    save_state('record', {'entries': entries[-2000:]})


def view_record(params):
    return {'entries': list(reversed(load_state('record', {}).get('entries', [])))[:300]}


def rule_arn():
    arn = service_resources().get('ListenerRule')
    if not arn:
        raise Refused("The service's load balancer rule could not be found.")
    return arn


def is_paused():
    """Is the load balancer answering with the paused page instead of the service?"""
    rule = aws('elbv2').describe_rules(RuleArns=[rule_arn()])['Rules'][0]
    return any(a['Type'] == 'fixed-response' for a in rule['Actions'])


def set_paused(on):
    if on:
        actions = [{'Type': 'fixed-response', 'FixedResponseConfig': {
            'StatusCode': '503', 'ContentType': 'text/html', 'MessageBody': PAUSED_PAGE}}]
    else:
        actions = [{'Type': 'forward', 'TargetGroupArn': service_resources()['TargetGroup']}]
    aws('elbv2').modify_rule(RuleArn=rule_arn(), Actions=actions)


def healthy_tasks():
    r = aws('elbv2').describe_target_health(TargetGroupArn=service_resources()['TargetGroup'])
    return sum(1 for t in r['TargetHealthDescriptions'] if t['TargetHealth']['State'] == 'healthy')


def settle():
    """Once a task answers again, give users the service back instead of the paused page."""
    svc = ecs_service()
    # A task of the new start must be running: for a short while after
    # standby begins, the task being stopped still counts as healthy.
    if svc['desiredCount'] > 0 and svc['runningCount'] > 0 and is_paused() and healthy_tasks() > 0:
        set_paused(False)
        save_state('gaveback', {'at': time.time()})
        return True
    return False


def just_given_back():
    """The load balancer takes some seconds to act on a changed rule (20 s seen); don't judge the service in them."""
    return time.time() - (load_state('gaveback') or {}).get('at', 0) < 90


def set_tasks(count, who, why='asked'):
    if not 0 <= count <= TASK_CAP:
        raise Refused(f'The panel runs between 0 and {TASK_CAP} tasks.')
    before = ecs_service()['desiredCount']
    if count == before:
        return {'tasks': count, 'changed': False}

    if count == 0:
        set_paused(True)        # first, so that no user meets a bare gateway error
    aws('ecs').update_service(cluster=STACK, service=STACK, desiredCount=count)
    record(who, 'standby' if count == 0 else ('wake' if before == 0 else 'tasks'),
           {'before': before, 'after': count, 'why': why})
    return {'tasks': count, 'changed': True, 'before': before}


def scheduled_state(at=None):
    """'run' or 'standby' by the schedule, or None when there is no schedule in force."""
    sch = load_state('schedule', DEFAULT_SCHEDULE)
    if not sch.get('enabled'):
        return None
    try:
        from zoneinfo import ZoneInfo
        local = (at or now()).astimezone(ZoneInfo(sch.get('tz', 'UTC')))
    except Exception:
        return None
    hhmm = local.strftime('%H:%M')
    inside = local.weekday() in sch.get('days', []) and sch['start'] <= hhmm < sch['end']
    return 'run' if inside else 'standby'


def keep_schedule():
    """
    Called by the timer.  The schedule acts on its edges only:  when the
    scheduled state changes, it is applied once.  What the administrator
    sets by hand in between stands until the next edge.
    """
    want = scheduled_state()
    if want is None:
        return None
    sch = load_state('schedule', DEFAULT_SCHEDULE)
    if sch.get('applied') == want:
        return None
    set_tasks(0 if want == 'standby' else max(1, min(int(sch.get('count', 1)), TASK_CAP)), 'schedule', 'schedule')
    sch['applied'] = want
    save_state('schedule', sch)
    return want


def set_schedule(body, who):
    sch = dict(DEFAULT_SCHEDULE)
    sch['enabled'] = bool(body.get('enabled'))
    days = sorted({int(d) for d in body.get('days', []) if 0 <= int(d) <= 6})
    start, end = str(body.get('start', '')), str(body.get('end', ''))
    for v in (start, end):
        if not re.fullmatch(r'([01]\d|2[0-3]):[0-5]\d', v):
            raise Refused('Times are written HH:MM, 00:00 to 23:59.')
    if not start < end:
        raise Refused('The service must start before it stops, on the same day.')
    if sch['enabled'] and not days:
        raise Refused('Choose at least one day for the service to run.')
    tz = str(body.get('tz') or DEFAULT_SCHEDULE['tz'])
    try:
        from zoneinfo import ZoneInfo
        ZoneInfo(tz)
    except Exception:
        raise Refused(f'Unknown time zone {tz!r}.')
    count = int(body.get('count', 1))
    if not 1 <= count <= TASK_CAP:
        raise Refused(f'The schedule runs between 1 and {TASK_CAP} tasks.')

    sch.update(days=days, start=start, end=end, tz=tz, count=count)
    before = load_state('schedule', dict(DEFAULT_SCHEDULE))
    # Start from the state the schedule calls for now, so that saving it
    # changes nothing by itself; the next edge is the first to act.
    save_state('schedule', sch)
    sch['applied'] = scheduled_state()
    save_state('schedule', sch)
    record(who, 'schedule', {'before': {k: before.get(k) for k in DEFAULT_SCHEDULE},
                             'after': {k: sch.get(k) for k in DEFAULT_SCHEDULE}})
    return {'schedule': sch}


def start_build(who):
    cb = aws('codebuild')
    ids = cb.list_builds_for_project(projectName=BUILD_PROJECT, sortOrder='DESCENDING')['ids'][:1]
    if ids and cb.batch_get_builds(ids=ids)['builds'][0]['buildStatus'] == 'IN_PROGRESS':
        raise Refused('A build is already running.  One at a time.')
    b = cb.start_build(projectName=BUILD_PROJECT)['build']
    record(who, 'rebuild', {'build': b.get('buildNumber')})
    return {'build': b.get('buildNumber')}


def roll_back(tag, who):
    """Run an earlier image: move the 'live' tag to it and restart the tasks."""
    if not re.fullmatch(r'[0-9a-f]{7,40}', tag or ''):
        raise Refused('Name the image by its commit tag.')
    ecr = aws('ecr')
    before = (image_tags(f'{ECR_PREFIX}-tap').get('tags') or ['?'])[0]
    moved = []
    for repo in (f'{ECR_PREFIX}-tap', f'{ECR_PREFIX}-proxy'):
        got = ecr.batch_get_image(repositoryName=repo, imageIds=[{'imageTag': tag}],
                                  acceptedMediaTypes=['application/vnd.docker.distribution.manifest.v2+json',
                                                      'application/vnd.oci.image.manifest.v1+json',
                                                      'application/vnd.oci.image.index.v1+json',
                                                      'application/vnd.docker.distribution.manifest.list.v2+json'])
        if not got.get('images'):
            raise Refused(f'There is no image tagged {tag} in {repo}.')
        image = got['images'][0]
        try:
            ecr.put_image(repositoryName=repo, imageManifest=image['imageManifest'],
                          imageManifestMediaType=image.get('imageManifestMediaType'), imageTag='live')
            moved.append(repo)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ImageAlreadyExistsException':
                raise
    restarted = False
    if ecs_service()['desiredCount'] > 0:
        aws('ecs').update_service(cluster=STACK, service=STACK, forceNewDeployment=True)
        restarted = True
    record(who, 'rollback', {'before': before, 'after': tag, 'moved': moved, 'restarted': restarted})
    return {'live': tag, 'moved': moved, 'restarted': restarted}


JOB_ID = re.compile(r'tap_[A-Za-z0-9_]+')


def view_jobs(params):
    """Async jobs that started and have not been seen to end, checked against the service."""
    hours = max(1, min(int(params.get('hours', '6')), 48))
    jobs = {}
    for r in sorted(query_records(hours), key=lambda r: r.get('t', 0)):
        jid = r.get('id')
        if not jid or not JOB_ID.fullmatch(jid):
            continue
        j = jobs.setdefault(jid, {'id': jid, 'client': r.get('client'), 'started': r.get('t'), 'q': '', 'ended': False})
        if r.get('q') and not j['q']:
            j['q'] = r['q']
        if r.get('job') or r.get('m') == 'DELETE':
            j['ended'] = True

    running, problem = [], None
    for j in [j for j in jobs.values() if not j['ended'] and j['q']][-25:]:
        try:
            code, text = service(f"/async/{j['id']}/phase", timeout=10)
        except ServiceError as e:
            problem = str(e)
            break
        j['phase'] = text.strip() if code == 200 else f'({code})'
        if j['phase'] in ('EXECUTING', 'QUEUED', 'PENDING'):
            running.append(j)
    return {'hours': hours, 'jobs': running, 'problem': problem}


def abort_job(jid, who):
    if not JOB_ID.fullmatch(jid or ''):
        raise Refused('That is not a job id.')
    code, text = service(f'/async/{jid}/phase', {'PHASE': 'ABORT'}, timeout=20)
    _, phase = service(f'/async/{jid}/phase', timeout=10)
    record(who, 'abort', {'job': jid, 'answer': code, 'phase': phase.strip()[:20]})
    return {'job': jid, 'phase': phase.strip()[:20]}


def control(body, who):
    action = body.get('action')
    if action == 'tasks':
        return set_tasks(int(body.get('count', -1)), who)
    if action == 'settle':
        return {'settled': settle()}
    if action == 'schedule':
        return set_schedule(body, who)
    if action == 'rebuild':
        return start_build(who)
    if action == 'rollback':
        return roll_back(body.get('tag'), who)
    if action == 'abort':
        return abort_job(body.get('id'), who)
    if action == 'upload':
        return upload_slot(body)
    if action == 'ingest':
        return start_ingest(body, who)
    if action == 'describe':
        return start_describe(body, who)
    if action in ('publish', 'discard', 'hide', 'unhide', 'remove', 'restore'):
        return start_move(action, body.get('folder'), who, confirm=body.get('confirm'), version=body.get('version'))
    raise Refused(f'No control named {action!r}.')


# ---- datasets: ingest, publish, hide, remove (stage 3) --------------------

FOLDER = re.compile(r'[a-z][a-z0-9_]{0,62}')
RESERVED = {'tap_schema', 'staging', 'hidden', 'versions', 'catalogs', 'upload', 'incoming'}
AREAS = ('staging/', 'hidden/', 'versions/')


def folder_name(name):
    name = str(name or '').strip()
    if not FOLDER.fullmatch(name) or name in RESERVED:
        raise Refused('A table name starts with a letter and has only lower-case letters, digits and "_".')
    return name


def keys_under(prefix, bucket=None):
    out = []
    for page in aws('s3').get_paginator('list_objects_v2').paginate(Bucket=bucket or CATALOG_BUCKET, Prefix=prefix):
        out.extend(page.get('Contents', []))
    return out


def area_folders(prefix):
    """Folders of Parquet files directly under a prefix: name -> files, bytes, modified."""
    folders = {}
    for o in keys_under(prefix):
        rest = o['Key'][len(prefix):]
        if rest.count('/') != 1 or not rest.endswith('.parquet'):
            continue
        f = folders.setdefault(rest.split('/')[0], {'files': 0, 'bytes': 0, 'modified': 0})
        f['files'] += 1
        f['bytes'] += o['Size']
        f['modified'] = max(f['modified'], o['LastModified'].timestamp())
    return [{'folder': k, **v} for k, v in sorted(folders.items())]


def versions():
    out = {}
    for o in keys_under('versions/'):
        parts = o['Key'].split('/')
        if len(parts) != 4 or not parts[3].endswith('.parquet'):
            continue
        v = out.setdefault(parts[1], {}).setdefault(parts[2], {'version': parts[2], 'files': 0, 'bytes': 0})
        v['files'] += 1
        v['bytes'] += o['Size']
    return {k: sorted(v.values(), key=lambda x: x['version'], reverse=True) for k, v in out.items()}


def staged():
    """Every folder in staging with its report."""
    out = []
    seen = set()
    for o in keys_under('staging/'):
        parts = o['Key'].split('/')
        if len(parts) != 3 or parts[1] in seen or parts[2] != '_report.json':
            continue
        seen.add(parts[1])
        try:
            report = json.loads(aws('s3').get_object(Bucket=CATALOG_BUCKET, Key=o['Key'])['Body'].read())
        except (ClientError, ValueError) as e:
            report = {'ok': False, 'error': f'The report could not be read: {e}'}
        report['folder'] = parts[1]
        report['replaces'] = bool(keys_under(f'{CATALOG_PREFIX}/{parts[1]}/')[:1])
        out.append(report)
    return out


def ingests():
    """The conversion tasks the panel has started, newest first, with how each stands now."""
    held = load_state('ingests', {}).get('list', [])
    live = [i for i in held if i.get('status') in ('PROVISIONING', 'PENDING', 'RUNNING', 'STARTING')]
    if live:
        try:
            got = aws('ecs').describe_tasks(cluster=INGEST_CLUSTER, tasks=[i['task'] for i in live])
            by_arn = {t['taskArn']: t for t in got.get('tasks', [])}
            for i in live:
                t = by_arn.get(i['task'])
                if not t:
                    i['status'] = 'GONE'
                    continue
                if t['lastStatus'] == 'STOPPED':
                    code = (t.get('containers') or [{}])[0].get('exitCode')
                    i['status'] = 'STAGED' if code == 0 else 'FAILED'
                    i['ended'] = (t.get('stoppedAt') or now()).timestamp()
                    if code != 0:
                        i['reason'] = t.get('stoppedReason') or (t.get('containers') or [{}])[0].get('reason') or ''
                else:
                    i['status'] = 'RUNNING' if t['lastStatus'] == 'RUNNING' else 'STARTING'
            save_state('ingests', {'list': held[-50:]})
        except ClientError as e:
            print(f'ingests: {e!r}')
    return list(reversed(held))[:15]


def upload_slot(body):
    """Somewhere to put a file, and a link good for an hour that lets the browser put it there."""
    import secrets
    name = os.path.basename(str(body.get('filename') or 'table'))
    name = re.sub(r'[^A-Za-z0-9._-]', '_', name)[:120] or 'table'
    key = f"upload/{time.strftime('%Y%m%dT%H%M%S', time.gmtime())}-{secrets.token_hex(5)}/{name}"
    url = aws('s3').generate_presigned_url('put_object', Params={'Bucket': STATE_BUCKET, 'Key': key}, ExpiresIn=3600)
    return {'key': key, 'url': url}


def run_ingest(folder, env, who, what):
    held = load_state('ingests', {}).get('list', [])
    ingests()
    held = load_state('ingests', {}).get('list', [])
    if any(i['folder'] == folder and i.get('status') in ('STARTING', 'RUNNING') for i in held):
        raise Refused(f'An ingest of {folder} is already running.')

    env = dict(env, NAME=folder, DEST_BUCKET=CATALOG_BUCKET, DEST_PREFIX=f'staging/{folder}/')
    r = aws('ecs').run_task(
        cluster=INGEST_CLUSTER, taskDefinition=INGEST_TASKDEF, launchType='FARGATE', count=1,
        networkConfiguration={'awsvpcConfiguration': {
            'subnets': INGEST_SUBNETS, 'securityGroups': [INGEST_SG], 'assignPublicIp': 'ENABLED'}},
        overrides={'containerOverrides': [{'name': 'ingest', 'environment': [
            {'name': k, 'value': str(v)} for k, v in env.items() if v not in (None, '')]}]},
        propagateTags='TASK_DEFINITION')
    if not r.get('tasks'):
        raise Refused('The conversion task could not be started: ' + json.dumps(r.get('failures', []))[:300])
    task = r['tasks'][0]['taskArn']
    held.append({'folder': folder, 'task': task, 'started': time.time(), 'status': 'STARTING', 'what': what})
    save_state('ingests', {'list': held[-50:]})
    record(who, what, {'table': folder, 'task': task.split('/')[-1], 'source': env.get('SRC_KEY') or env.get('SRC_PREFIX')})
    return {'folder': folder, 'task': task.split('/')[-1]}


def start_ingest(body, who):
    folder = folder_name(body.get('name'))
    key = str(body.get('key') or '')
    if not key.startswith('upload/') or '..' in key:
        raise Refused('Upload the file first.')
    try:
        aws('s3').head_object(Bucket=STATE_BUCKET, Key=key)
    except ClientError:
        raise Refused('The uploaded file is not there.  Upload it again.')
    schema = str(body.get('schemaKey') or '')
    if schema and (not schema.startswith('upload/') or '..' in schema):
        raise Refused('The column list was not uploaded.')
    col = re.compile(r'[A-Za-z_][A-Za-z0-9_ .-]{0,63}')
    ra, dec = str(body.get('ra') or 'ra').strip(), str(body.get('dec') or 'dec').strip()
    if not col.fullmatch(ra) or not col.fullmatch(dec):
        raise Refused('Give the names of the two position columns as they appear in the file.')
    return run_ingest(folder, {'MODE': 'convert', 'SRC_BUCKET': STATE_BUCKET, 'SRC_KEY': key, 'SCHEMA_KEY': schema,
                               'DESCRIPTION': str(body.get('description') or '')[:500], 'RA': ra, 'DEC': dec},
                      who, 'ingest')


def view_columns(params):
    """A served table's columns as the service lists them, for the metadata editor."""
    table = folder_name(params.get('table'))
    cols, _ = tap(f"SELECT column_name, datatype, unit, ucd, description FROM TAP_SCHEMA.columns WHERE table_name = '{table}'")
    about, _ = tap(f"SELECT description FROM TAP_SCHEMA.tables WHERE table_name = '{table}'")
    return {'table': table, 'description': (about[0]['description'] if about else ''),
            'columns': [c for c in cols if c['column_name'] != 'hpx29']}


def start_describe(body, who):
    """Rewrite a served catalog's files with new units and descriptions, into staging."""
    folder = folder_name(body.get('folder'))
    if not keys_under(f'{CATALOG_PREFIX}/{folder}/')[:1]:
        raise Refused(f'There is no served folder named {folder}.')
    rows = body.get('columns') or []
    if not rows:
        raise Refused('No columns were sent.')
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['column_name', 'unit', 'ucd', 'description'])
    for c in rows[:5000]:
        w.writerow([str(c.get('column_name', ''))[:128], str(c.get('unit') or '')[:64],
                    str(c.get('ucd') or '')[:128], str(c.get('description') or '')[:1000]])
    import secrets
    key = f"upload/{time.strftime('%Y%m%dT%H%M%S', time.gmtime())}-{secrets.token_hex(5)}/schema.csv"
    aws('s3').put_object(Bucket=STATE_BUCKET, Key=key, Body=out.getvalue().encode(), ContentType='text/csv')
    return run_ingest(folder, {'MODE': 'describe', 'SRC_BUCKET': CATALOG_BUCKET,
                               'SRC_PREFIX': f'{CATALOG_PREFIX}/{folder}/', 'SCHEMA_BUCKET': STATE_BUCKET,
                               'SCHEMA_KEY': key, 'DESCRIPTION': str(body.get('description') or '')[:500]},
                      who, 'describe')


def view_ingestlog(params):
    task = re.sub(r'[^0-9a-f]', '', params.get('task', ''))[:40]
    if not task:
        return {'lines': []}
    events = aws('logs').filter_log_events(logGroupName=INGEST_LOGS, logStreamNames=[f'ingest/ingest/{task}'],
                                          limit=400).get('events', [])
    return {'lines': [{'at': e['timestamp'] / 1000, 'message': e['message'][:1000]} for e in events][-300:]}


# Moving a table between areas can take minutes for a large one, longer than
# a page waits.  A control starts the move as a second run of this function
# and returns; the page watches "moves" in the datasets view.

def moves():
    return [m for m in load_state('moves', {}).get('list', []) if time.time() - m.get('started', 0) < 86400][-10:]


def start_move(action, folder, who, **more):
    folder = folder_name(folder)
    held = load_state('moves', {}).get('list', [])
    if any(m['folder'] == folder and m['status'] == 'RUNNING' and time.time() - m['started'] < 900 for m in held):
        raise Refused(f'{folder} is already being moved.  Wait for that to finish.')

    here = f'{CATALOG_PREFIX}/{folder}/'
    if action == 'publish':
        report = next((r for r in staged() if r['folder'] == folder), None)
        if not report or not report.get('ok'):
            raise Refused(f'There is no staged table named {folder} that passed its conversion.')
    elif action in ('hide', 'remove'):
        if not keys_under(here)[:1]:
            raise Refused(f'There is no served folder named {folder}.')
        if action == 'remove' and more.get('confirm') != folder:
            raise Refused('To remove a table, type its folder name exactly.')
    elif action == 'unhide':
        if not keys_under(f'hidden/{folder}/')[:1]:
            raise Refused(f'There is no hidden table named {folder}.')
        if keys_under(here)[:1]:
            raise Refused(f'A table named {folder} is being served.  Remove or hide that one first.')
    elif action == 'restore':
        version = str(more.get('version') or '')
        if not re.fullmatch(r'[0-9TZ]+(-[a-z]+)?', version) or not keys_under(f'versions/{folder}/{version}/')[:1]:
            raise Refused('There is no such earlier version.')
    elif action == 'discard':
        pass
    else:
        raise Refused(f'No control named {action!r}.')

    move = {'id': f'{int(time.time() * 1000)}', 'action': action, 'folder': folder, 'who': who,
            'started': time.time(), 'status': 'RUNNING', **{k: v for k, v in more.items() if k == 'version'}}
    held.append(move)
    save_state('moves', {'list': held[-40:]})
    aws('lambda').invoke(FunctionName=os.environ['AWS_LAMBDA_FUNCTION_NAME'], InvocationType='Event',
                         Payload=json.dumps({'move': move}).encode())
    return {'started': action, 'folder': folder}


def copy_folder(src, dst, src_bucket=None):
    """Copy the Parquet files directly under src to dst; returns their names."""
    s3 = boto3.client('s3')
    names = []
    for o in keys_under(src, src_bucket):
        name = o['Key'][len(src):]
        if '/' in name or not name.endswith('.parquet'):
            continue
        s3.copy({'Bucket': src_bucket or CATALOG_BUCKET, 'Key': o['Key']}, CATALOG_BUCKET, dst + name)
        names.append(name)
    return names


def empty_folder(prefix, keep=()):
    for o in keys_under(prefix):
        if o['Key'][len(prefix):] not in keep:
            aws('s3').delete_object(Bucket=CATALOG_BUCKET, Key=o['Key'])


def stamp():
    return time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())


def do_move(move):
    action, folder = move['action'], move['folder']
    here, detail = f'{CATALOG_PREFIX}/{folder}/', {'table': folder}
    try:
        if action == 'publish':
            # Keep what is being replaced, put the new files in, then take
            # away old files the new set does not have.  The service reads
            # this folder throughout; for the seconds the copy takes it may
            # see a mixture of old and new files.
            if keys_under(here)[:1]:
                detail['kept'] = f'versions/{folder}/{stamp()}/'
                copy_folder(here, detail['kept'])
            names = copy_folder(f'staging/{folder}/', here)
            if not names:
                raise RuntimeError('Staging held no files.')
            empty_folder(here, keep=names)
            empty_folder(f'staging/{folder}/')
            detail['files'] = len(names)
        elif action == 'discard':
            empty_folder(f'staging/{folder}/')
        elif action == 'hide':
            empty_folder(f'hidden/{folder}/')
            copy_folder(here, f'hidden/{folder}/')
            empty_folder(here)
        elif action == 'unhide':
            copy_folder(f'hidden/{folder}/', here)
            empty_folder(f'hidden/{folder}/')
        elif action == 'remove':
            detail['kept'] = f'versions/{folder}/{stamp()}-removed/'
            copy_folder(here, detail['kept'])
            empty_folder(here)
        elif action == 'restore':
            if keys_under(here)[:1]:
                detail['kept'] = f'versions/{folder}/{stamp()}/'
                copy_folder(here, detail['kept'])
            names = copy_folder(f"versions/{folder}/{move['version']}/", here)
            empty_folder(here, keep=names)
            detail['restored'] = move['version']
        status, error = 'DONE', None
    except Exception as e:
        print(f'move failed: {e!r}')
        status, error = 'FAILED', str(e)[:300]
        detail['error'] = error

    held = load_state('moves', {}).get('list', [])
    for m in held:
        if m.get('id') == move['id']:
            m.update(status=status, ended=time.time(), error=error)
    save_state('moves', {'list': held[-40:]})
    record(move.get('who', '?'), action, detail)
    return {'status': status}


VIEWS = {'record': view_record, 'jobs': view_jobs, 'columns': view_columns, 'ingestlog': view_ingestlog,
         'status': view_status, 'load': view_load, 'activity': view_activity, 'datasets': view_datasets,
         'cost': view_cost, 'components': view_components, 'logs': view_logs}


# ---- canary ---------------------------------------------------------------

def run_canary():
    """A few queries with known answers; the result is kept and published as metrics."""
    with open(os.path.join(HERE, 'canary.json')) as f:
        checks = json.load(f)['checks']

    svc = ecs_service()
    result = {'at': time.time(), 'ok': True, 'checks': [], 'ms': 0}

    if not SECRET:
        result.update(ok=None, skipped='no way in')
    elif svc['desiredCount'] == 0:
        result.update(ok=None, skipped='standby')
    elif svc['runningCount'] == 0 or is_paused() or just_given_back():
        result.update(ok=None, skipped='starting')
    else:
        for c in checks:
            one = {'name': c['name']}
            try:
                rows, took = tap(c['query'], timeout=40)
                got = int(rows[0][c.get('column', 'n')])
                one.update(ms=int(took * 1000), got=got, expected=c['expected'], ok=got == c['expected'])
            except Exception as e:
                one.update(ok=False, error=str(e)[:200])
            result['checks'].append(one)
            result['ms'] += one.get('ms', 0)
            result['ok'] = result['ok'] and one['ok']

    state = load_state('canary', {})
    history = state.get('history', [])
    history.append({'at': result['at'], 'ok': result['ok'], 'ms': result['ms']})
    save_state('canary', {'last': result, 'history': history[-2016:]})      # a week of five-minute runs

    if result['ok'] is not None:
        aws('cloudwatch').put_metric_data(Namespace='ArchiveInABox/Panel', MetricData=[
            {'MetricName': 'CanaryOk', 'Dimensions': [{'Name': 'Service', 'Value': STACK}],
             'Value': 1 if result['ok'] else 0},
            {'MetricName': 'CanaryMillis', 'Dimensions': [{'Name': 'Service', 'Value': STACK}],
             'Value': result['ms'], 'Unit': 'Milliseconds'}])
    return result


# ---- entry ----------------------------------------------------------------

def respond(code, body):
    return {'statusCode': code,
            'headers': {'Content-Type': 'application/json', 'Cache-Control': 'no-store'},
            'body': json.dumps(body, default=str)}


def tick():
    """The timer: keep the schedule, give the service back once it answers, run the canary."""
    out = {}
    for name, step in (('schedule', keep_schedule), ('settled', settle)):
        try:
            out[name] = step()
        except Exception as e:
            print(f'tick: {name} failed: {e!r}')
            out[name] = f'failed: {e}'[:200]
    out['canary'] = run_canary()
    return out


def handler(event, context):
    if event.get('canary'):
        return tick()
    if event.get('move'):
        return do_move(event['move'])

    http = event.get('requestContext', {}).get('http', {})
    headers = {k.lower(): v for k, v in (event.get('headers') or {}).items()}
    name = event.get('rawPath', '').rstrip('/').split('/')[-1]

    if http.get('method', 'GET') == 'POST' and name == 'control':
        # A page elsewhere cannot add this header to a request it makes the
        # administrator's browser send here.
        if headers.get('x-panel') != '1':
            return respond(403, {'error': 'Not from the panel page.'})
        who = (headers.get('x-forwarded-for') or '?').split(',')[0].strip()
        try:
            raw = event.get('body') or '{}'
            if event.get('isBase64Encoded'):
                import base64
                raw = base64.b64decode(raw).decode()
            out = control(json.loads(raw), who)
            out['asOf'] = time.time()
            return respond(200, out)
        except Refused as e:
            return respond(409, {'error': str(e)})
        except ServiceError as e:
            return respond(502, {'error': str(e)})
        except Exception as e:
            print(f'control failed: {e!r}')
            return respond(500, {'error': f'{type(e).__name__}: {e}'[:500]})

    if http.get('method', 'GET') != 'GET':
        return respond(405, {'error': 'Only the controls take a POST.'})

    view = VIEWS.get(name)
    if not view:
        return respond(404, {'error': f'No view named {name!r}', 'views': sorted(VIEWS)})

    try:
        out = view(event.get('queryStringParameters') or {})
        out['asOf'] = time.time()
        return respond(200, out)
    except Exception as e:
        print(f'view {name} failed: {e!r}')
        return respond(500, {'error': f'{type(e).__name__}: {e}'[:500]})
