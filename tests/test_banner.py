"""The service's copy of the administrator's notice (deploy/ec2/banner.py)."""
import importlib.util
import io
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('aiab_banner', ROOT / 'deploy' / 'ec2' / 'banner.py')
banner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(banner)


class Bucket:
    def __init__(self, text=None, error=None):
        self.text, self.error = text, error

    def get_object(self, Bucket, Key):
        assert (Bucket, Key) == ('b', 'catalogs/_banner.json')
        if self.error:
            e = Exception(self.error)
            e.response = {'Error': {'Code': self.error}}
            raise e
        return {'Body': io.BytesIO(self.text.encode())}


def test_only_the_notice_is_copied():
    got = banner.notice(json.dumps({'message': ' Down  on\nThursday ', 'level': 'warn', 'set': 5, 'by': 'someone', 'x': 1}))
    assert got == {'message': 'Down on Thursday', 'level': 'warn', 'set': 5}
    assert banner.notice(json.dumps({'message': 'a' * 999, 'level': 'loud'})) == {'message': 'a' * 300, 'level': 'info', 'set': 0}
    for nothing in ('', 'not json', '[]', '{"message": ""}', '{"message": "  "}'):
        assert banner.notice(nothing) is None


def test_where_the_notice_is():
    assert banner.split('s3://b/catalogs/') == ('b', 'catalogs/_banner.json')
    assert banner.split('s3://b') == ('b', '_banner.json')


def test_the_file_follows_the_bucket(tmp_path):
    target = tmp_path / 'banner.json'
    banner.look(Bucket(json.dumps({'message': 'Back at 3', 'set': 1})), 'b', 'catalogs/_banner.json', str(target))
    assert json.loads(target.read_text()) == {'message': 'Back at 3', 'level': 'info', 'set': 1}
    assert target.stat().st_mode & 0o044 == 0o044              # the web server reads it
    banner.look(Bucket(json.dumps({'message': ''})), 'b', 'catalogs/_banner.json', str(target))
    assert not target.exists()
    banner.look(Bucket(json.dumps({'message': 'Again'})), 'b', 'catalogs/_banner.json', str(target))
    banner.look(Bucket(error='NoSuchKey'), 'b', 'catalogs/_banner.json', str(target))
    assert not target.exists()
    assert [p.name for p in tmp_path.iterdir()] == []


def test_a_bucket_that_cannot_be_read_leaves_the_notice(tmp_path):
    target = tmp_path / 'banner.json'
    banner.look(Bucket(json.dumps({'message': 'Stay'})), 'b', 'catalogs/_banner.json', str(target))
    with pytest.raises(Exception):
        banner.look(Bucket(error='AccessDenied'), 'b', 'catalogs/_banner.json', str(target))
    assert json.loads(target.read_text())['message'] == 'Stay'
