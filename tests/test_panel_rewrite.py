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
