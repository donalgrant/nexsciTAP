# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE

"""
The control panel's functions, stage 1:  views only.

One Lambda function answers every "/api/<view>" request of the panel page
and, on a timer, runs the canary.  It changes nothing in the service:  its
role may read the service's records and write only the panel's own state
(the "state/" part of the panel's bucket) and the canary's metrics.

Views:  status, load, activity, datasets, cost, components, logs.

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
    TASK_CAP        most tasks the panel will ever ask for (shown, not used yet)
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
        state = 'starting' if svc.get('pendingCount') else 'down'
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

    return {
        'state': state, 'desired': desired, 'running': running, 'pending': svc.get('pendingCount', 0),
        'taskCap': TASK_CAP, 'serviceUrl': SERVICE_URL,
        'canary': {'last': last, 'history': history[-72:],
                   'day': {'runs': len(day), 'failed': sum(1 for h in day if not h['ok'])},
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
            'totalBytes': sum(f['bytes'] for f in folders.values())}


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


VIEWS = {'status': view_status, 'load': view_load, 'activity': view_activity, 'datasets': view_datasets,
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


def handler(event, context):
    if event.get('canary'):
        return run_canary()

    http = event.get('requestContext', {}).get('http', {})
    if http.get('method', 'GET') != 'GET':
        return respond(405, {'error': 'The panel is read-only at this stage.'})

    name = event.get('rawPath', '').rstrip('/').split('/')[-1]
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
