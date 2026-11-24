#!/usr/bin/env python
"""
Keep the service's notice to its users level with the one its administrator set.

    banner.py <catalog prefix> <target file> [<seconds between looks>]

The administrator's control panel writes the notice as <catalog prefix>_banner.json
({"message": ..., "level": "info" | "warn", "set": <time>}).  This copies what
it holds to <target file>, in the work area, which the service already serves
(/workspace/banner.json), where the front page and the portal's pages fetch
it.  No notice, or an empty one, removes the file.

Only the three fields are copied, the message cut to MAX_MESSAGE characters,
so nothing else placed in that object reaches a browser.  The pages show the
message as text, never as markup.

With a number of seconds it looks again for ever;  without, once.
"""

import json
import os
import sys
import tempfile
import time

MAX_MESSAGE = 300
LEVELS = ('info', 'warn')
NAME = '_banner.json'


def notice(text):
    """The notice in the object's text, as the pages are given it;  None for no notice."""
    try:
        d = json.loads(text)
    except ValueError:
        return None
    if not isinstance(d, dict):
        return None
    message = ' '.join(str(d.get('message') or '').split())[:MAX_MESSAGE]
    if not message:
        return None
    level = d.get('level') if d.get('level') in LEVELS else 'info'
    at = d.get('set') if isinstance(d.get('set'), (int, float)) else 0
    return {'message': message, 'level': level, 'set': at}


def keep(target, found):
    """Make the target file say what was found (None:  no file).  True if it changed."""
    want = json.dumps(found, sort_keys=True) if found else None
    try:
        with open(target) as f:
            have = f.read()
    except OSError:
        have = None
    if want == have:
        return False
    if want is None:
        os.unlink(target)
        return True
    # Beside the target and moved into place, so a page never reads half of it.
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(target)), prefix='.banner')
    with os.fdopen(fd, 'w') as f:
        f.write(want)
    os.chmod(tmp, 0o644)
    os.replace(tmp, target)
    return True


def split(prefix):
    """('bucket', 'key of the notice') from s3://bucket/catalogs/."""
    rest = prefix[len('s3://'):] if prefix.startswith('s3://') else prefix
    bucket, _, path = rest.partition('/')
    path = path.strip('/')
    return bucket, (path + '/' if path else '') + NAME


def look(s3, bucket, key, target):
    try:
        text = s3.get_object(Bucket=bucket, Key=key)['Body'].read(20000).decode('utf-8', 'replace')
    except Exception as e:
        code = getattr(e, 'response', {}).get('Error', {}).get('Code', '')
        if code not in ('NoSuchKey', '404'):
            raise                   # not "there is none":  leave the notice as it is
        text = ''
    if keep(target, notice(text)):
        print('banner: ' + ('changed' if os.path.exists(target) else 'removed'), flush=True)


def main():
    import boto3
    prefix, target = sys.argv[1], sys.argv[2]
    every = float(sys.argv[3]) if len(sys.argv) > 3 else 0
    bucket, key = split(prefix)
    s3 = boto3.client('s3', region_name=os.environ.get('AWS_REGION') or None)
    while True:
        try:
            look(s3, bucket, key, target)
        except Exception as e:
            print(f'WARNING: could not read the notice: {e!r}', file=sys.stderr, flush=True)
        if not every:
            break
        time.sleep(every)


if __name__ == '__main__':
    main()
