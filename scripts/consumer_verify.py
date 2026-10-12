
import hashlib
import json
import os
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

RID = "rel-89a3ec6e"
BUCKET = "research-release"
PREFIX = f"lab2/releases/{RID}/"

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    region_name="us-east-1",
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    ),
)

manifest_bytes = s3.get_object(
    Bucket=BUCKET,
    Key=PREFIX + "release_manifest.json",
)["Body"].read()

Path("/tmp/consumer-release-manifest.json").write_bytes(manifest_bytes)

manifest = json.loads(manifest_bytes)
assert manifest["release_id"] == RID, "RELEASE_ID_MISMATCH"

tmp = Path("/tmp/lab2-consumer")
tmp.mkdir(parents=True, exist_ok=True)

verified = []

for key, meta in manifest["artifacts"].items():
    body = s3.get_object(
        Bucket=BUCKET,
        Key=PREFIX + key,
    )["Body"].read()

    assert len(body) == meta["bytes"], f"SIZE_MISMATCH: {key}"
    digest = hashlib.sha256(body).hexdigest()
    assert digest == meta["sha256"], f"HASH_MISMATCH: {key}"

    (tmp / key.replace("/", "_")).write_bytes(body)
    verified.append(key)

assert "data/curated.parquet" in verified, "PARQUET_MISSING"

try:
    s3.get_object(
        Bucket="research-raw",
        Key="lab2/inputs/batch-01/sensors.csv",
    )
except ClientError as exc:
    code = exc.response["Error"]["Code"]
    status = exc.response.get("ResponseMetadata", {}).get(
        "HTTPStatusCode"
    )
    if status != 403 and code not in ("AccessDenied", "Forbidden"):
        raise RuntimeError(
            f"Raw access failed for an unverified reason: {code}"
        ) from exc
    raw_denied = True
else:
    raise AssertionError(
        "SECURITY_FAILURE: analyst can read raw data"
    )
parquet_body = s3.get_object(
    Bucket=BUCKET,
    Key=PREFIX + "data/curated.parquet",
)["Body"].read()

Path("/tmp/consumer-curated.parquet").write_bytes(parquet_body)

result = {
    "release_id": RID,
    "manifest_read": True,
    "artifact_hashes_verified": len(verified),
    "artifacts": verified,
    "raw_access_denied": raw_denied,
    "row_count_expected": manifest["curated_row_count"],
    "row_count_verified": False,
    "note": "Parquet row count must be verified separately."
}

Path("/tmp/consumer-check.json").write_text(
    json.dumps(result, indent=2),
    encoding="utf-8",
)

print(json.dumps(result, indent=2))
