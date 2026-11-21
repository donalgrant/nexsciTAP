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
