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
