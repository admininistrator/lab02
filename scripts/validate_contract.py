"""Validate supplied contract schema and display SHA-256 for peer signoff.
Run BEFORE T2 metrics; if contract changes later, create a new contract version and run.
"""
import hashlib
import json
from pathlib import Path
from jsonschema import Draft202012Validator, FormatChecker

root = Path(__file__).resolve().parent.parent
contract_file = root / 'contract.json'
schema_file = root / 'fixture/schemas/observation_contract_schema.json'
contract = json.loads(contract_file.read_text(encoding='utf-8'))
schema = json.loads(schema_file.read_text(encoding='utf-8'))
Draft202012Validator(schema, format_checker=FormatChecker()).validate(contract)
print('CONTRACT_SCHEMA: PASS')
print('CONTRACT_SHA256:', hashlib.sha256(contract_file.read_bytes()).hexdigest())
print('REVIEW_STATUS:', contract['review_status'])
