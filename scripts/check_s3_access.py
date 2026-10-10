"""Run with curator identity only, from the curate Pod; tests a raw GET and real denied release PUT.
Never run with publisher credentials. A mistakenly allowed PUT is a security failure.
"""
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path


def main():
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError, BotoCoreError
    endpoint = os.environ['S3_ENDPOINT']
    client = boto3.client('s3', endpoint_url=endpoint, region_name='us-east-1',
                          config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}))
    result = {'id': 'I04', 'status': 'BLOCKED', 'identity': 's3-ingestor',
              'timestamp': datetime.now(timezone.utc).isoformat()}
    try:
        r = client.get_object(Bucket='research-raw', Key='lab2/inputs/batch-01/observations_a.csv')
        try: count = len(r['Body'].read())
        finally: r['Body'].close()
        result['raw_get'] = {'status': 'PASS', 'bytes': count}
    except Exception as exc:
        result['raw_get'] = {'status': 'FAIL', 'error_type': type(exc).__name__}
        result['status'] = 'BLOCKED'
    if result['raw_get']['status'] == 'PASS':
        test_key = 'lab2/probes/curator-forbidden-' + str(uuid.uuid4())
        try:
            client.put_object(Bucket='research-release', Key=test_key,
                              Body=b'FORBIDDEN-CURATOR-ACCESS-TEST')
            result['release_put'] = {'status': 'FAIL', 'key': test_key,
                                     'reason': 'WRITE_SUCCEEDED_SECURITY_VIOLATION'}
            result['status'] = 'FAIL'
        except ClientError as err:
            status_code = err.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
            error_code = err.response.get('Error', {}).get('Code')
            ok = status_code == 403 and error_code in ('AccessDenied', 'AllAccessDisabled')
            result['release_put'] = {'status': 'PASS' if ok else 'FAIL',
                                     'http_status': status_code, 'error_code': error_code}
            result['status'] = 'PASS' if ok else 'BLOCKED'
        except (BotoCoreError, Exception) as err:
            result['release_put'] = {'status': 'BLOCKED', 'error_type': type(err).__name__}
            result['status'] = 'BLOCKED'
    out = Path('evidence')
    out.mkdir(parents=True, exist_ok=True)
    (out / 'I04.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    Path('access-test.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps(result, indent=2))
    if result['status'] != 'PASS':
        sys.exit(2)


if __name__ == '__main__':
    main()
