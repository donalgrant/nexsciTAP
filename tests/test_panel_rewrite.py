"""The control panel's rewrite of a served table: what it will and will not pass to the conversion task."""
import importlib
import sys
import types
from pathlib import Path

import pytest

FUNCTIONS = Path(__file__).resolve().parent.parent / 'deploy' / 'panel' / 'functions'


@pytest.fixture(scope='module')
def panel():
    # The function finds the AWS library in Lambda; stand-ins are enough to load it here.
    saved = {k: sys.modules.get(k) for k in ('boto3', 'botocore', 'botocore.exceptions', 'panel')}
    boto3 = types.ModuleType('boto3')
    boto3.client = lambda *a, **k: None
    botocore = types.ModuleType('botocore')
    exceptions = types.ModuleType('botocore.exceptions')
    exceptions.ClientError = type('ClientError', (Exception,), {})
    sys.modules.update({'boto3': boto3, 'botocore': botocore, 'botocore.exceptions': exceptions})
    sys.modules.pop('panel', None)
    sys.path.insert(0, str(FUNCTIONS))
    try:
        yield importlib.import_module('panel')
    finally:
        sys.path.remove(str(FUNCTIONS))
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v


def test_nothing_asked_changes_nothing(panel):
    assert panel.rewrite_settings({}) == {}
    assert panel.rewrite_settings({'rename': [{'from': 'z', 'to': 'z'}], 'rowGroup': ''}) == {}


def test_renames_and_row_groups_reach_the_task(panel):
    got = panel.rewrite_settings({'rename': [{'from': 'z_in', 'to': 'z'}, {'from': 'Mass (Msun)', 'to': 'mass_msun'}],
                                  'rowGroup': 'auto'})
    assert got == {'RENAME': 'z_in=z,Mass (Msun)=mass_msun', 'ROW_GROUP': 'auto'}
    assert panel.rewrite_settings({'rowGroup': 20480}) == {'ROW_GROUP': '20480'}


@pytest.mark.parametrize('body', [
    {'rename': [{'from': 'ra', 'to': 'alpha'}]},            # the searches depend on it
    {'rename': [{'from': 'hpx29', 'to': 'cell'}]},
    {'rename': [{'from': 'aiab_x', 'to': 'x'}]},
    {'rename': [{'from': 'z_in', 'to': 'dec'}]},            # a name the service uses
    {'rename': [{'from': 'z_in', 'to': 'aiab_z'}]},
    {'rename': [{'from': 'z_in', 'to': 'Z'}]},              # not a name the service can list
    {'rename': [{'from': 'z_in', 'to': '--help'}]},
    {'rename': [{'from': 'a,b', 'to': 'c'}]},               # would read as two renames
    {'rename': [{'from': 'a=b', 'to': 'c'}]},
    {'rename': [{'from': 'a', 'to': 'c'}, {'from': 'b', 'to': 'c'}]},
    {'rowGroup': 'all'},
    {'rowGroup': 5},
    {'rowGroup': 50000000},
])
def test_what_is_refused(panel, body):
    with pytest.raises(panel.Refused):
        panel.rewrite_settings(body)


def test_a_move_whose_ending_was_lost_is_not_in_progress(panel, monkeypatch):
    """The list of moves is shared; a move marked running beyond the function's time limit is not running."""
    import time
    now = time.time()
    held = [{'id': '1', 'action': 'publish', 'folder': 'old', 'started': now - 90000, 'status': 'DONE'},
            {'id': '2', 'action': 'publish', 'folder': 'lost', 'started': now - 7000, 'status': 'RUNNING'},
            {'id': '3', 'action': 'publish', 'folder': 'busy', 'started': now - 30, 'status': 'RUNNING'},
            {'id': '4', 'action': 'hide', 'folder': 'done', 'started': now - 60, 'status': 'DONE'}]
    monkeypatch.setattr(panel, 'load_state', lambda name, default=None: {'list': held})
    got = {m['folder']: m['status'] for m in panel.moves()}
    assert got == {'lost': 'UNKNOWN', 'busy': 'RUNNING', 'done': 'DONE'}
    assert held[1]['status'] == 'RUNNING'           # what is stored is not changed by reading it


class _FakeS3:
    """Enough of S3 to store objects and to refuse a write whose condition no longer holds."""

    def __init__(self, error):
        import threading
        self.objects, self.version, self.lock, self.error, self.refused = {}, 0, threading.Lock(), error, 0

    def _fail(self, code):
        e = self.error(code)
        e.response = {'Error': {'Code': code}}
        return e

    def get_object(self, Bucket, Key):
        import io
        import threading
        with self.lock:
            if Key not in self.objects:
                raise self._fail('NoSuchKey')
            body, tag = self.objects[Key]
        threading.Event().wait(0.002)   # let another thread in between the read and the write
        return {'Body': io.BytesIO(body), 'ETag': tag}

    def put_object(self, Bucket, Key, Body, ContentType=None, IfMatch=None, IfNoneMatch=None):
        with self.lock:
            have = self.objects.get(Key)
            if (IfNoneMatch == '*' and have) or (IfMatch and (not have or have[1] != IfMatch)):
                self.refused += 1
                raise self._fail('PreconditionFailed')
            self.version += 1
            self.objects[Key] = (Body, f'"{self.version}"')


def test_changes_made_at_the_same_moment_are_all_kept(panel, monkeypatch):
    """Forty appends from eight threads to one stored list: every one is there afterward."""
    import threading
    s3 = _FakeS3(panel.ClientError)
    monkeypatch.setattr(panel, 'aws', lambda name, region=None: s3)
    monkeypatch.setattr(panel.time, 'sleep', lambda s: None)

    def work(k):
        for i in range(5):
            panel.change_state('moves', lambda d: {'list': d.get('list', []) + [f'{k}-{i}']}, {}, tries=200)
    threads = [threading.Thread(target=work, args=(k,)) for k in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    kept = panel.load_state('moves')['list']
    assert sorted(kept) == sorted(f'{k}-{i}' for k in range(8) for i in range(5))
    assert s3.refused > 0                # the test did make writes collide

    # A change that decides to write nothing writes nothing, and a refusal inside it comes out.
    before = s3.version
    assert panel.change_state('moves', lambda d: None, {})['list'] == kept
    with pytest.raises(panel.Refused):
        panel.change_state('moves', lambda d: (_ for _ in ()).throw(panel.Refused('no')), {})
    assert s3.version == before


def test_the_firewall_is_found_from_the_service_stack(panel, monkeypatch):
    """Its name comes from the WebAclArn setting; a service without one has no firewall to show."""
    arn = 'arn:aws:wafv2:us-east-1:024489621410:global/webacl/aiab-waf/06cd6333-737e-4f93-b64c-3129b1789224'
    monkeypatch.setattr(panel, 'stack_parameters', lambda: {'WebAclArn': arn})
    assert panel.firewall_name() == 'aiab-waf'
    for none in ({}, {'WebAclArn': ''}, {'WebAclArn': 'not-an-arn'}):
        monkeypatch.setattr(panel, 'stack_parameters', lambda none=none: none)
        assert panel.firewall_name() == ''
        assert panel.firewall(24, 300, panel.now()) is None

    # One that cannot be read says so, and does not stop the rest of the view.
    monkeypatch.setattr(panel, 'stack_parameters', lambda: {'WebAclArn': arn})
    monkeypatch.setattr(panel, 'cached', lambda *a: (_ for _ in ()).throw(RuntimeError('no access')))
    got = panel.firewall(24, 300, panel.now())
    assert got['name'] == 'aiab-waf' and 'no access' in got['error'] and 'series' not in got


def test_what_the_rule_sets_matched(panel):
    """One row for each rule that matched, refused and counted side by side, the busiest first."""
    results = [{'Id': 'refused', 'Label': 'AWSManagedRulesAmazonIpReputationList AWSManagedReconnaissanceList BlockedRequests', 'Values': [10.0, 2.0]},
               {'Id': 'refused', 'Label': 'AWSManagedRulesCommonRuleSet NoUserAgent_HEADER BlockedRequests', 'Values': [3.0]},
               {'Id': 'counted', 'Label': 'AWSManagedRulesCommonRuleSet NoUserAgent_HEADER CountedRequests', 'Values': [40.0]},
               {'Id': 'counted', 'Label': 'AWSManagedRulesCommonRuleSet SizeRestrictions_BODY CountedRequests', 'Values': []},
               {'Id': 'counted', 'Label': '', 'Values': [5.0]}, {'Id': 'other', 'Label': 'a b c', 'Values': [5.0]}]
    assert panel.rule_totals(results) == [
        {'set': 'CommonRuleSet', 'rule': 'NoUserAgent_HEADER', 'refused': 3, 'counted': 40},
        {'set': 'AmazonIpReputationList', 'rule': 'AWSManagedReconnaissanceList', 'refused': 12, 'counted': 0}]
    assert panel.rule_totals([]) == []


def test_the_rule_sets_can_be_set_to_count(panel):
    now = {'service': {}, 'firewall': {'RateLimit': '1000', 'Mode': 'block', 'ManagedRules': 'block'}}
    assert panel.limit_changes({'ManagedRules': 'count'}, now) == {'firewall': {'ManagedRules': ('block', 'count')}}
    with pytest.raises(panel.Refused):
        panel.limit_changes({'ManagedRules': 'off'}, now)


NOW = {'service': {'ClientLimit': '6', 'AsyncLimit': '8', 'AsyncClientLimit': '3', 'QueryMemory': '1GB',
                   'QueryTimeout': '300', 'MaxRecLimit': '2000000', 'DesiredCount': '1', 'PanelSecret': 'x'},
       'firewall': {'RateLimit': '1000', 'OverallLimit': '5000', 'Mode': 'block'}}


def test_a_change_of_limits_goes_to_the_stack_that_holds_it(panel):
    got = panel.limit_changes({'ClientLimit': 8, 'QueryMemory': '1.5gb', 'RateLimit': '2,000', 'Mode': 'count',
                               'AsyncLimit': '8', 'MaxRecLimit': 2000000}, NOW)
    assert got == {'service': {'ClientLimit': ('6', '8'), 'QueryMemory': ('1GB', '1.5GB')},
                   'firewall': {'RateLimit': ('1000', '2000'), 'Mode': ('block', 'count')}}
    assert panel.limit_changes({'ClientLimit': '6', 'Mode': 'block'}, NOW) == {}      # as they are: no change


@pytest.mark.parametrize('asked', [
    {}, None, {'DesiredCount': 3}, {'PanelSecret': 'mine'}, {'TapImage': 'evil'},      # not limits
    {'ClientLimit': -1}, {'ClientLimit': '6; rm'}, {'ClientLimit': 1000}, {'ClientLimit': 2.5},
    {'QueryMemory': '8GB'}, {'QueryMemory': '100MB'}, {'QueryMemory': 'lots'}, {'QueryMemory': '1GB --x'},
    {'QueryTimeout': 5}, {'MaxRecLimit': 10}, {'RateLimit': 5}, {'Mode': 'off'},
    {'AsyncClientLimit': 9},                                # more for one client than for all (8)
    {'AsyncLimit': 2},                                      # fewer in all than one client may have (3)
])
def test_limits_that_are_refused(panel, asked):
    with pytest.raises(panel.Refused):
        panel.limit_changes(asked, NOW)


def test_limits_without_a_firewall(panel):
    with pytest.raises(panel.Refused):
        panel.limit_changes({'RateLimit': 500}, {'service': NOW['service']})
    assert panel.limit_changes({'AsyncLimit': 0, 'AsyncClientLimit': 50}, NOW)       # 0 is no limit in all


def test_the_download_caps_are_limits(panel):
    now = {'service': dict(NOW['service'], DownloadLimit='20GB', FireflyDownloadLimit='50GB'), 'firewall': NOW['firewall']}
    assert panel.limit_changes({'DownloadLimit': '30 gb', 'FireflyDownloadLimit': '50GB'}, now) == \
        {'service': {'DownloadLimit': ('20GB', '30GB')}}
    assert panel.limit_changes({'DownloadLimit': 0, 'FireflyDownloadLimit': '1.5tb'}, now) == \
        {'service': {'DownloadLimit': ('20GB', '0'), 'FireflyDownloadLimit': ('50GB', '1.5TB')}}
    for bad in ('20', 'lots', '20GB; x', '-1GB', '0.0001MB', '20PB'):
        with pytest.raises(panel.Refused):
            panel.limit_changes({'DownloadLimit': bad}, now)


def test_the_request_log_is_read_once_an_hour(panel, monkeypatch):
    """An hour that is over is read from CloudWatch once, kept, and not read again."""
    import json
    import time
    at = time.time()
    this_hour = int(at // 3600)
    lines = [(h * 3600 + 30, {'t': h * 3600 + 30, 'path': '/sync', 'n': h}) for h in range(this_hour - 30, this_hour + 1)
             if h * 3600 + 30 < at]
    asked, kept = [], {}

    class Logs:
        def get_paginator(self, name):
            return self

        def paginate(self, **kw):
            asked.append((kw['startTime'] // 3600000, (kw['endTime'] + 1) // 3600000))
            got = [{'timestamp': int(t * 1000), 'message': 'x TAPQ ' + json.dumps(r)}
                   for t, r in lines if kw['startTime'] <= t * 1000 <= kw['endTime']]
            return [{'events': got[:3]}, {'events': got[3:]}]

    monkeypatch.setattr(panel, 'aws', lambda *a, **k: Logs())
    monkeypatch.setattr(panel, 'load_state', lambda name, default=None: kept.get(name, default))
    monkeypatch.setattr(panel, 'save_state', lambda name, value: kept.__setitem__(name, json.loads(json.dumps(value))))
    monkeypatch.setattr(panel, '_hours', {})
    monkeypatch.setattr(panel, '_live', {})

    first = panel.query_records(24)
    assert [r['n'] for r in first] == sorted(r['n'] for r in first)             # oldest first
    assert {r['n'] for r in first} == {r['n'] for t, r in lines if t >= at - 24 * 3600}
    assert len(asked) > 1 and len(kept) >= 22                                   # the closed hours are kept
    assert f'tapq/{this_hour}' not in kept                                      # the hour in progress is not

    asked.clear()
    assert panel.query_records(24) == first
    assert len(asked) == 1 and asked[0][0] >= this_hour - 1                     # only the open end is read again
    lines.append((at - 1, {'t': at - 1, 'path': '/sync', 'n': this_hour + 1}))  # a request made since
    assert panel.query_records(24) == first + [lines[-1][1]]
    first = first + [lines[-1][1]]

    asked.clear()
    monkeypatch.setattr(panel, '_hours', {})                                    # a new copy of the function
    monkeypatch.setattr(panel, '_live', {})
    assert panel.query_records(24) == first
    assert len(asked) == 1                                                      # the kept hours came from the bucket
    assert len(panel.query_records(3)) < len(first)


def test_a_door_log_file_is_dated_by_its_name(panel):
    """CloudFront's files arrive in no order, so they are told apart by the hour in their names, not by name order."""
    from datetime import datetime, timezone
    now = datetime(2026, 11, 22, 3, 0, tzinfo=timezone.utc).timestamp()
    assert panel.door_log_age('service/E2LMXJ9HJKEQ7L.2026-11-22-01.c40486de.gz', now) == 2 * 3600
    assert panel.door_log_age('service/E2LMXJ9HJKEQ7L.2026-11-19-03.ffd77748.gz', now) == 3 * 86400
    assert panel.door_log_age('service/something-else', now) == float('inf')


def test_a_new_name_needs_no_quotes(panel):
    """A rename from the panel is held to the converter's rule: a letter first, and not a word ADQL reserves."""
    pytest.importorskip('duckdb')
    sys.path.insert(0, str(FUNCTIONS.parent.parent.parent / 'tools'))
    try:
        import make_parquet_catalog
    finally:
        sys.path.pop(0)
    assert panel.ADQL_RESERVED == make_parquet_catalog.ADQL_RESERVED
    for bad in ('_planets', '__row', 'size', 'date'):
        with pytest.raises(panel.Refused):
            panel.rewrite_settings({'rename': [{'from': 'a', 'to': bad}]})
    assert panel.rewrite_settings({'rename': [{'from': '_planets', 'to': 'n_planets'}, {'from': 'size', 'to': 'size_'}]}) \
        == {'RENAME': '_planets=n_planets,size=size_'}


# ---- the notice to users, blocking an address, and what the firewall refused ----

def test_the_paused_page_fits_with_any_notice(panel):
    assert panel.paused_page() == panel.PAUSED_PAGE
    for message in ('Back at 3 pm.', 'x' * 300, '<b>&</b>' * 37, 'é' * 300, '&' * 300):
        page = panel.paused_page(message)
        assert len(page.encode()) <= panel.PAUSED_MOST
        assert 'This archive is paused' in page and '<b>' not in page
        assert not page.rstrip('</p>').endswith('&am')
    assert 'Back at 3 pm.' in panel.paused_page('Back at 3 pm.')
    assert '&lt;script&gt;' in panel.paused_page('<script>')


def test_a_notice_is_short_plain_text(panel):
    assert panel.banner_settings({'message': '  Down   on\nThursday. ', 'level': 'warn'}) == {'message': 'Down on Thursday.', 'level': 'warn'}
    assert panel.banner_settings({}) == {'message': '', 'level': 'info'}
    for body in ({'message': 'x' * 301}, {'message': 'a', 'level': 'loud'}, {'message': 'a\x07b'}):
        with pytest.raises(panel.Refused):
            panel.banner_settings(body)


def test_an_address_to_block(panel):
    assert panel._address('192.0.2.7') == '192.0.2.7/32'
    assert panel._address(' 192.0.2.77/24 ') == '192.0.2.0/24'
    for bad in ('', 'example.org', '2001:db8::1', '10.0.0.0/8', '300.1.1.1', '1.2.3.4/33'):
        with pytest.raises(ValueError):
            panel._address(bad)
    assert panel._addresses('192.0.2.9, 192.0.2.7/32,192.0.2.9') == '192.0.2.7/32,192.0.2.9/32'
    assert panel._addresses('') == ''
    assert panel.LIMITS['BlockedAddresses'][0] == 'firewall'
    got = panel.limit_changes({'BlockedAddresses': '192.0.2.7'}, {'firewall': {'BlockedAddresses': ''}, 'service': {}})
    assert got == {'firewall': {'BlockedAddresses': ('', '192.0.2.7/32')}}


def test_a_refusal_by_the_firewall_in_the_door_log(panel):
    row = {'sc-status': '429', 'x-edge-result-type': 'Error', 'time-taken': '0.001'}
    assert panel.by_firewall(row)
    assert panel.by_firewall(dict(row, **{'sc-status': '403'}))
    assert not panel.by_firewall(dict(row, **{'time-taken': '0.212'}))             # the service's own refusal
    assert not panel.by_firewall(dict(row, **{'x-edge-result-type': 'FunctionGeneratedResponse'}))      # the door's
    assert not panel.by_firewall(dict(row, **{'sc-status': '200'}))
    assert not panel.by_firewall(dict(row, **{'time-taken': '-'}))
    hour = str(int(panel.time.time() // 3600) * 3600)
    watch = {'hours': {hour: {'walled': {'192.0.2.7': {'429': 40, '403': 2}, '192.0.2.9': {'403': 5}}},
                       '0': {'walled': {'192.0.2.1': {'429': 999}}}}}
    assert panel.refused_by_firewall(watch) == [
        {'address': '192.0.2.7', 'refused': 42, 'rate': 40, 'last': int(hour)},
        {'address': '192.0.2.9', 'refused': 5, 'rate': 0, 'last': int(hour)}]


def test_the_firewall_rule(panel):
    assert panel.firewall_verdict({'refused': 0, 'overAll': 0}) == (False, 'nothing refused for its rate')
    bad, said = panel.firewall_verdict({'refused': 31, 'overAll': 0, 'who': ['192.0.2.7']})
    assert bad and '31 requests refused' in said and '192.0.2.7' in said
    bad, said = panel.firewall_verdict({'refused': 0, 'overAll': 12})
    assert bad and 'overall mark' in said
    assert 'firewall' in panel.RULES


def test_what_each_client_was_sent(panel):
    t = panel.time.time()
    records = [{'t': t - 60, 'client': '192.0.2.7', 'bytes': 5_000_000_000}, {'t': t - 90, 'client': '192.0.2.7', 'bytes': 3000},
               {'t': t - 100, 'client': '127.0.0.1', 'bytes': 10_000_000_000}, {'t': t - 90000, 'client': '192.0.2.7', 'bytes': 10 ** 12},
               {'t': t - 5, 'client': '192.0.2.9'}]
    got = panel.volume_sent({'DownloadLimit': '20GB', 'FireflyDownloadLimit': '50GB'}, records)
    assert got['cap'] == 20_000_000_000 and got['sharedCap'] == 50_000_000_000
    assert got['clients'] == [
        {'client': 'Firefly (all its users)', 'bytes': 10_000_000_000, 'cap': 50_000_000_000, 'share': 0.2},
        {'client': '192.0.2.7', 'bytes': 5_000_000_000, 'cap': 20_000_000_000, 'share': 0.25}]
    assert panel.volume_sent({'DownloadLimit': '0'}, records)['clients'][1]['share'] is None
    assert panel.size_bytes('1.5TB') == 1_500_000_000_000 and panel.size_bytes('0') == 0 and panel.size_bytes('x') == 0


def test_only_a_token_of_the_panels_own_pool_signs_in(panel, monkeypatch):
    """Cognito's GetUser answers for a token of any pool; the panel must not."""
    import base64
    import json

    def token(**claims):
        body = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip('=')
        return 'e30.' + body + '.sig'
    monkeypatch.setattr(panel, 'SIGN_IN_POOL', 'us-west-2_Ours')
    monkeypatch.setattr(panel, 'SIGN_IN_CLIENT', 'client1')
    asked = []

    class Cognito:
        def get_user(self, AccessToken):
            asked.append(AccessToken)
            return {'Username': 'u', 'UserAttributes': [{'Name': 'email', 'Value': 'a@example.org'}]}
    monkeypatch.setattr(panel, 'aws', lambda *a, **k: Cognito())
    good = dict(iss='https://cognito-idp.us-west-2.amazonaws.com/us-west-2_Ours', client_id='client1', token_use='access')
    assert panel.signed_in({'x-panel-token': token(**good)}) == 'a@example.org'
    for change in ({'iss': 'https://cognito-idp.us-west-2.amazonaws.com/us-west-2_Theirs'}, {'client_id': 'other'},
                   {'token_use': 'id'}, {'iss': 'https://evil.example/us-west-2_Ours'}):
        panel._known.clear()
        n = len(asked)
        assert panel.signed_in({'x-panel-token': token(**dict(good, **change))}) is None
        assert len(asked) == n, 'a foreign token is refused before Cognito is asked'
    for junk in ('', 'nonsense', 'a.b.c', 'a.' + 'x' * 5000 + '.c'):
        assert panel.signed_in({'x-panel-token': junk}) is None
    monkeypatch.setattr(panel, 'SIGN_IN_POOL', '')
    panel._known.clear()
    assert panel.signed_in({'x-panel-token': token(**good)}) is None, 'no pool named: nobody signs in'


def test_the_limits_of_24_november(panel):
    """The overall volume, a result's size and the firewall's overall mode are limits the panel may change."""
    assert panel.LIMITS['TotalDownloadLimit'][0] == 'service' and panel.LIMITS['OverallMode'][0] == 'firewall'
    assert panel._result_size('4GB') == '4GB' and panel._result_size('0') == '0'
    for bad in ('5MB', '1TB', '200GB', 'lots'):
        try:
            panel._result_size(bad)
        except ValueError:
            continue
        raise AssertionError(bad)
