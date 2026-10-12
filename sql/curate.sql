-- Student curation pipeline for research-telemetry-v1.
-- Assumes typed.sql already loaded (raw_envelopes, typed, registry, qa_reference).
-- AS_OF and thresholds are read from contract.json via Python bindings when needed;
-- fixed AS_OF below matches contract.

CREATE OR REPLACE TEMP TABLE contract_params AS
SELECT
  TIMESTAMP '2026-02-09 00:00:00' AS as_of,
  900::BIGINT AS late_threshold_seconds,
  -30.0::DOUBLE AS temp_min_c,
  60.0::DOUBLE AS temp_max_c;

CREATE OR REPLACE VIEW registry_norm AS
SELECT
  upper(trim(sensor_id)) AS sensor_id,
  -- site is registry-derived; contract only TRIM_UPPER for record_id/sensor_id/unit
  trim(site) AS site
FROM registry;

-- Physical intake with pre-dedup flags (PARSE / KEY / INGEST_PARSE).
CREATE OR REPLACE VIEW intake AS
SELECT
  t.*,
  (parse_ok IS NOT TRUE) AS is_parse_fail,
  (
    record_id IS NULL
    OR trim(coalesce(record_id, '')) = ''
    OR NOT regexp_full_match(coalesce(record_id, ''), '^R[0-9]{6}$')
  ) AS is_key_fail,
  (ingest_ts IS NULL) AS is_ingest_parse_fail,
  CASE
    WHEN parse_ok IS NOT TRUE THEN 'PARSE'
    WHEN (
      record_id IS NULL
      OR trim(coalesce(record_id, '')) = ''
      OR NOT regexp_full_match(coalesce(record_id, ''), '^R[0-9]{6}$')
    ) THEN 'KEY'
    WHEN ingest_ts IS NULL THEN 'INGEST_PARSE'
    ELSE NULL
  END AS pre_primary_reason,
  list_filter(
    [
      CASE WHEN parse_ok IS NOT TRUE THEN 'PARSE' END,
      CASE WHEN (
        record_id IS NULL
        OR trim(coalesce(record_id, '')) = ''
        OR NOT regexp_full_match(coalesce(record_id, ''), '^R[0-9]{6}$')
      ) THEN 'KEY' END,
      CASE WHEN ingest_ts IS NULL THEN 'INGEST_PARSE' END
    ],
    x -> x IS NOT NULL
  ) AS pre_all_reasons
FROM typed t;

CREATE OR REPLACE VIEW pre_quarantine AS
SELECT
  source_object,
  source_row,
  source_sha256,
  record_id,
  sensor_id,
  event_time,
  ingest_time,
  reading,
  unit,
  pre_primary_reason AS primary_reason,
  array_to_string(pre_all_reasons, '|') AS all_reasons,
  'pre_dedup' AS stage
FROM intake
WHERE pre_primary_reason IS NOT NULL;

CREATE OR REPLACE VIEW dedup_pool AS
SELECT *
FROM intake
WHERE pre_primary_reason IS NULL;

CREATE OR REPLACE VIEW ranked_versions AS
SELECT
  d.*,
  row_number() OVER (
    PARTITION BY record_id
    ORDER BY ingest_ts DESC, source_object ASC, source_row ASC
  ) AS rn
FROM dedup_pool d;

CREATE OR REPLACE VIEW winner_versions AS
SELECT * EXCLUDE (rn)
FROM ranked_versions
WHERE rn = 1;

CREATE OR REPLACE VIEW duplicate_versions AS
SELECT
  source_object,
  source_row,
  source_sha256,
  record_id,
  sensor_id,
  event_time,
  ingest_time,
  reading,
  unit,
  'SUPERSEDED' AS reason,
  first_value(source_object) OVER (
    PARTITION BY record_id
    ORDER BY ingest_ts DESC, source_object ASC, source_row ASC
  ) AS winner_source_object,
  first_value(source_row) OVER (
    PARTITION BY record_id
    ORDER BY ingest_ts DESC, source_object ASC, source_row ASC
  ) AS winner_source_row
FROM ranked_versions
WHERE rn > 1;

-- Winner validity diagnostics (do not use qa_reference to impute/select values).
CREATE OR REPLACE VIEW winner_eval AS
WITH params AS (SELECT * FROM contract_params),
base AS (
  SELECT
    w.*,
    r.site AS registry_site,
    CASE
      WHEN upper(trim(unit)) = 'F' AND value_num IS NOT NULL AND isfinite(value_num)
        THEN (value_num - 32.0) * 5.0 / 9.0
      WHEN upper(trim(unit)) = 'C' AND value_num IS NOT NULL AND isfinite(value_num)
        THEN value_num
      ELSE NULL
    END AS temperature_c_raw,
    date_diff('second', event_ts, ingest_ts) AS lag_seconds
  FROM winner_versions w
  LEFT JOIN registry_norm r ON w.sensor_id = r.sensor_id
  CROSS JOIN params p
)
SELECT
  b.*,
  (
    b.record_id IS NULL OR trim(coalesce(b.record_id, '')) = ''
    OR b.sensor_id IS NULL OR trim(coalesce(b.sensor_id, '')) = ''
    OR b.event_time IS NULL OR trim(coalesce(b.event_time, '')) = ''
    OR b.ingest_time IS NULL OR trim(coalesce(b.ingest_time, '')) = ''
    OR b.reading IS NULL OR trim(coalesce(b.reading, '')) = ''
    OR b.unit IS NULL OR trim(coalesce(b.unit, '')) = ''
  ) AS fail_required,
  (b.value_num IS NULL OR NOT isfinite(b.value_num)) AS fail_numeric,
  (b.unit IS NULL OR upper(trim(b.unit)) NOT IN ('C', 'F')) AS fail_unit,
  (
    b.temperature_c_raw IS NULL
    OR b.temperature_c_raw < (SELECT temp_min_c FROM params)
    OR b.temperature_c_raw > (SELECT temp_max_c FROM params)
  ) AS fail_range,
  (b.event_ts IS NULL) AS fail_time_parse,
  (
    b.event_ts IS NOT NULL
    AND b.ingest_ts IS NOT NULL
    AND (
      b.event_ts > b.ingest_ts
      OR b.ingest_ts > (SELECT as_of FROM params)
    )
  ) AS fail_time_order,
  (b.registry_site IS NULL) AS fail_reference,
  list_filter(
    [
      CASE WHEN (
        b.record_id IS NULL OR trim(coalesce(b.record_id, '')) = ''
        OR b.sensor_id IS NULL OR trim(coalesce(b.sensor_id, '')) = ''
        OR b.event_time IS NULL OR trim(coalesce(b.event_time, '')) = ''
        OR b.ingest_time IS NULL OR trim(coalesce(b.ingest_time, '')) = ''
        OR b.reading IS NULL OR trim(coalesce(b.reading, '')) = ''
        OR b.unit IS NULL OR trim(coalesce(b.unit, '')) = ''
      ) THEN 'REQUIRED' END,
      CASE WHEN (b.value_num IS NULL OR NOT isfinite(b.value_num)) THEN 'NUMERIC' END,
      CASE WHEN (b.unit IS NULL OR upper(trim(b.unit)) NOT IN ('C', 'F')) THEN 'UNIT' END,
      CASE WHEN (
        b.temperature_c_raw IS NULL
        OR b.temperature_c_raw < (SELECT temp_min_c FROM params)
        OR b.temperature_c_raw > (SELECT temp_max_c FROM params)
      ) THEN 'RANGE' END,
      CASE WHEN b.event_ts IS NULL THEN 'TIME_PARSE' END,
      CASE WHEN (
        b.event_ts IS NOT NULL
        AND b.ingest_ts IS NOT NULL
        AND (
          b.event_ts > b.ingest_ts
          OR b.ingest_ts > (SELECT as_of FROM params)
        )
      ) THEN 'TIME_ORDER' END,
      CASE WHEN b.registry_site IS NULL THEN 'REFERENCE' END
    ],
    x -> x IS NOT NULL
  ) AS winner_all_reasons
FROM base b;

CREATE OR REPLACE VIEW winner_reasons AS
SELECT
  *,
  CASE
    WHEN fail_required THEN 'REQUIRED'
    WHEN fail_numeric THEN 'NUMERIC'
    WHEN fail_unit THEN 'UNIT'
    WHEN fail_range THEN 'RANGE'
    WHEN fail_time_parse THEN 'TIME_PARSE'
    WHEN fail_time_order THEN 'TIME_ORDER'
    WHEN fail_reference THEN 'REFERENCE'
    ELSE NULL
  END AS primary_reason
FROM winner_eval;

CREATE OR REPLACE VIEW winner_quarantine AS
SELECT
  source_object,
  source_row,
  source_sha256,
  record_id,
  sensor_id,
  event_time,
  ingest_time,
  reading,
  unit,
  primary_reason,
  array_to_string(winner_all_reasons, '|') AS all_reasons,
  'winner_validation' AS stage
FROM winner_reasons
WHERE primary_reason IS NOT NULL;

CREATE OR REPLACE VIEW curated_rows AS
SELECT
  record_id,
  sensor_id,
  registry_site AS site,
  event_ts AS event_time_utc,
  ingest_ts AS ingest_time_utc,
  cast(round(temperature_c_raw, 2) AS DECIMAL(8, 2)) AS temperature_c,
  (lag_seconds > (SELECT late_threshold_seconds FROM contract_params)) AS is_late,
  source_object,
  source_row,
  source_sha256
FROM winner_reasons
WHERE primary_reason IS NULL;

CREATE OR REPLACE VIEW quarantine_all AS
SELECT * FROM pre_quarantine
UNION ALL BY NAME
SELECT * FROM winner_quarantine;

CREATE OR REPLACE TABLE curated AS
SELECT * FROM curated_rows
ORDER BY record_id, source_object, source_row;

CREATE OR REPLACE TABLE quarantine AS
SELECT * FROM quarantine_all
ORDER BY source_object, source_row;

CREATE OR REPLACE TABLE duplicates AS
SELECT
  source_object,
  source_row,
  source_sha256,
  record_id,
  winner_source_object,
  winner_source_row,
  reason
FROM duplicate_versions
ORDER BY record_id, source_object, source_row;
