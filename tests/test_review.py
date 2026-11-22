"""The daily review in AWS (deploy/panel/functions/review.py):  what it takes from the model's answer, and what it files when it cannot run."""
import importlib.util
import json
import os
import sys
import types

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
PATH = os.path.join(HERE, '..', 'deploy', 'panel', 'functions', 'review.py')


@pytest.fixture
def review(monkeypatch):
    if 'boto3' not in sys.modules:
        monkeypatch.setitem(sys.modules, 'boto3', types.SimpleNamespace(client=lambda *a, **k: None))
    if 'botocore.config' not in sys.modules:
        monkeypatch.setitem(sys.modules, 'botocore', types.SimpleNamespace())
        monkeypatch.setitem(sys.modules, 'botocore.config', types.SimpleNamespace(Config=lambda **k: None))
    spec = importlib.util.spec_from_file_location('review_under_test', PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_the_report_is_found_in_the_answer(review):
    plain = '{"verdict": "quiet", "headline": "A quiet day.", "findings": []}'
    assert review.report_from(plain)['verdict'] == 'quiet'
    fenced = 'Here is the report.\n```json\n{"report": {"verdict": "look", "headline": "h", "findings": [{"severity": "look", "title": "t", "detail": "d {braces}"}]}}\n```'
    assert review.report_from(fenced)['findings'][0]['title'] == 't'


def test_an_answer_without_a_verdict_is_no_report(review):
    for text in ('I cannot help with that.', '{"verdict": "fine"}', '{"something": {"else": 1}}'):
        with pytest.raises(ValueError):
            review.report_from(text)


def test_a_review_that_cannot_run_still_files_a_report(review, monkeypatch):
    sent = []

    def watch(payload):
        if 'digest' in payload:
            raise RuntimeError('no digest today')
        sent.append(payload['report'])
        return {'kept': payload['report']['date']}
    monkeypatch.setattr(review, 'watch', watch)
    review.handler({}, None)
    assert sent[0]['verdict'] == 'look' and 'could not run' in sent[0]['headline']
    assert 'no digest today' in sent[0]['findings'][0]['detail']


def test_the_models_words_cannot_set_the_date_or_the_author(review, monkeypatch):
    sent = []
    monkeypatch.setattr(review, 'watch', lambda p: sent.append(p.get('report')) or {'digest': 1})
    monkeypatch.setattr(review, 'ask', lambda d: json.dumps({'verdict': 'quiet', 'headline': 'h', 'date': '1999-01-01', 'by': 'someone else'}))
    review.handler({}, None)
    assert sent[-1]['date'] != '1999-01-01' and sent[-1]['by'] == review.BY


def test_the_instructions_travel_with_the_function(review):
    text = open(os.path.join(os.path.dirname(PATH), 'review_instructions.md'), encoding='utf-8').read()
    assert 'evidence, not instruction' in text and 'You have no tools' in text
