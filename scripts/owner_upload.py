"""Runs INSIDE the owner Pod: verify, upload artifacts, then write the manifest last."""
import hashlib
import json
import os
import sys
from pathlib import Path

import boto3
from botocore.config import Config

d, rid = Path(sys.argv[1]), sys.argv[2]
manifest_bytes = (d / 'release_manifest.json').read_bytes()
m = json.loads(manifest_bytes)
if m['release_id'] != rid:
    raise SystemExit('RELEASE_ID_MISMATCH')
c = boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT'], region_name='us-east-1',
                 config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}))
prefix = 'lab2/releases/' + rid + '/'
if c.list_objects_v2(Bucket='research-release', Prefix=prefix, MaxKeys=1).get('KeyCount', 0):
    raise SystemExit('RELEASE_EXISTS: refusing to overwrite ' + prefix)
for key, meta in m['artifacts'].items():
    body = (d / key).read_bytes()
    if len(body) != meta['bytes'] or hashlib.sha256(body).hexdigest() != meta['sha256']:
        raise SystemExit('ARTIFACT_HASH_BEFORE_UPLOAD: ' + key)
for key in m['artifacts']:
    c.put_object(Bucket='research-release', Key=prefix + key, Body=(d / key).read_bytes())
for key, meta in m['artifacts'].items():
    got = c.get_object(Bucket='research-release', Key=prefix + key)['Body'].read()
    if hashlib.sha256(got).hexdigest() != meta['sha256']:
        raise SystemExit('ARTIFACT_HASH_AFTER_UPLOAD: ' + key)
c.put_object(Bucket='research-release', Key=prefix + 'release_manifest.json', Body=manifest_bytes)
back = c.get_object(Bucket='research-release', Key=prefix + 'release_manifest.json')['Body'].read()
if back != manifest_bytes:
    raise SystemExit('MANIFEST_VERIFY_FAILED')
print(json.dumps({'uploaded': len(m['artifacts']) + 1, 'prefix': prefix,
                  'manifest_sha256': hashlib.sha256(back).hexdigest()}))
