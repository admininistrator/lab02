"""Instructor/owner setup only: uploads four trusted input files to existing Lab 1 research-raw.
NEVER run as s3-ingestor unless instructor explicitly grants input-bootstrap permission.
Usage (in owner/authorized setup Pod): python upload_inputs.py /work/input /work/trusted_manifest.json
"""
import hashlib
import json
import os
import sys
from pathlib import Path


def main():
    if len(sys.argv) != 3:
        raise SystemExit('usage: python upload_inputs.py INPUT_DIRECTORY TRUSTED_MANIFEST')
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
    indir = Path(sys.argv[1]); manifest = json.loads(Path(sys.argv[2]).read_text())
    client = boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT'], region_name='us-east-1',
                          config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}))
    for obj in manifest['objects']:
        key = obj['key']; body = (indir / key).read_bytes()
        digest = hashlib.sha256(body).hexdigest()
        if len(body) != obj['bytes'] or digest != obj['sha256']:
            raise RuntimeError('INPUT_INTEGRITY: ' + key)
        dst = 'lab2/inputs/batch-01/' + key
        try:
            response = client.get_object(Bucket='research-raw', Key=dst)
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code', '')
            if code in ('NoSuchKey', '404', 'NotFound'):
                response = None
            else:
                raise
        if response is not None:
            try: remote = response['Body'].read()
            finally: response['Body'].close()
            if remote != body:
                raise RuntimeError('Existing source object does NOT match trusted source; refusing to overwrite: '+dst)
            print('EXISTS & VERIFIED:', dst)
            continue
        client.put_object(Bucket='research-raw', Key=dst, Body=body)
        print('UPLOADED:', dst)


if __name__ == '__main__':
    main()
