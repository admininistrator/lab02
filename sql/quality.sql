-- Quality and reconciliation queries for research-telemetry-v1.
-- Depends on typed.sql + curate.sql relations.

CREATE OR REPLACE VIEW quality_inventory AS
SELECT
  (SELECT count(*) FROM raw_envelopes) AS raw_physical_rows,
  (SELECT count(*) FROM typed WHERE parse_ok IS NOT TRUE) AS parse_failures,
  (SELECT count(*) FROM pre_quarantine WHERE primary_reason = 'KEY') AS key_rejected_rows,
  (SELECT count(*) FROM pre_quarantine WHERE primary_reason = 'INGEST_PARSE') AS ingest_parse_rejected,
  (SELECT count(*) FROM dedup_pool) AS key_eligible_rows,
  (SELECT count(*) FROM winner_versions) AS winner_rows_W,
  (SELECT count(*) FROM duplicates) AS duplicate_rows,
  (SELECT count(*) FROM curated) AS curated_rows,
  (SELECT count(*) FROM quarantine) AS quarantine_rows;

-- Conservation: physical IDs must partition exactly.
CREATE OR REPLACE VIEW conservation_check AS
WITH raw_ids AS (
  SELECT source_object, cast(source_row AS BIGINT) AS source_row FROM typed
),
cur_ids AS (
  SELECT source_object, source_row FROM curated
),
q_ids AS (
  SELECT source_object, source_row FROM quarantine
),
d_ids AS (
  SELECT source_object, source_row FROM duplicates
),
union_ids AS (
  SELECT * FROM cur_ids
  UNION
  SELECT * FROM q_ids
  UNION
  SELECT * FROM d_ids
)
SELECT
  (SELECT count(*) FROM raw_ids) AS n_raw,
  (SELECT count(*) FROM curated) AS n_curated,
  (SELECT count(*) FROM quarantine) AS n_quarantine,
  (SELECT count(*) FROM duplicates) AS n_duplicates,
  (SELECT count(*) FROM curated) + (SELECT count(*) FROM quarantine) + (SELECT count(*) FROM duplicates) AS n_outputs,
  (SELECT count(*) FROM raw_ids) = (
    (SELECT count(*) FROM curated) + (SELECT count(*) FROM quarantine) + (SELECT count(*) FROM duplicates)
  ) AS counts_match,
  (SELECT count(*) FROM (
    SELECT * FROM raw_ids EXCEPT SELECT * FROM union_ids
  )) = 0 AS no_lost_ids,
  (SELECT count(*) FROM (
    SELECT * FROM union_ids EXCEPT SELECT * FROM raw_ids
  )) = 0 AS no_invented_ids,
  (SELECT count(*) FROM (
    SELECT * FROM cur_ids INTERSECT SELECT * FROM q_ids
  )) = 0 AS curated_quarantine_disjoint,
  (SELECT count(*) FROM (
    SELECT * FROM cur_ids INTERSECT SELECT * FROM d_ids
  )) = 0 AS curated_duplicates_disjoint,
  (SELECT count(*) FROM (
    SELECT * FROM q_ids INTERSECT SELECT * FROM d_ids
  )) = 0 AS quarantine_duplicates_disjoint,
  (SELECT count(*) FROM curated) = (SELECT count(DISTINCT record_id) FROM curated) AS curated_business_unique;

-- After metrics on curated candidate.
CREATE OR REPLACE VIEW metric_after AS
WITH c AS (SELECT * FROM curated),
chrono AS (
  SELECT *
  FROM c
  WHERE event_time_utc IS NOT NULL
    AND ingest_time_utc IS NOT NULL
    AND event_time_utc <= ingest_time_utc
    AND ingest_time_utc <= TIMESTAMP '2026-02-09 00:00:00'
),
matched AS (
  SELECT c.record_id, c.temperature_c, q.reference_c
  FROM c
  INNER JOIN qa_reference q
    ON upper(trim(c.record_id)) = upper(trim(q.record_id))
)
SELECT 'Q_REQUIRED' AS rule_id,
       'curated rows' AS cohort,
       count(*) FILTER (
         WHERE record_id IS NOT NULL AND sensor_id IS NOT NULL AND site IS NOT NULL
           AND event_time_utc IS NOT NULL AND ingest_time_utc IS NOT NULL
           AND temperature_c IS NOT NULL AND source_object IS NOT NULL
           AND source_row IS NOT NULL AND source_sha256 IS NOT NULL
       ) AS numerator,
       count(*) AS denominator,
       1.0 AS threshold
FROM c
UNION ALL
SELECT 'Q_KEY_UNIQUENESS', 'curated rows',
       count(DISTINCT record_id), count(*), 1.0 FROM c
UNION ALL
SELECT 'Q_VALUE', 'curated rows',
       count(*) FILTER (
         WHERE temperature_c IS NOT NULL
           AND temperature_c BETWEEN -30 AND 60
       ), count(*), 1.0 FROM c
UNION ALL
SELECT 'Q_SENSOR', 'curated rows',
       count(*) FILTER (WHERE site IS NOT NULL), count(*), 1.0 FROM c
UNION ALL
SELECT 'Q_CHRONOLOGY', 'curated rows',
       (SELECT count(*) FROM chrono), count(*), 1.0 FROM c
UNION ALL
SELECT 'Q_TIMELINESS', 'chronologically valid curated rows',
       count(*) FILTER (WHERE is_late IS FALSE),
       count(*),
       0.95
FROM chrono
UNION ALL
SELECT 'Q_QA_COVERAGE', 'all 100 reference IDs',
       (SELECT count(*) FROM matched),
       (SELECT count(*) FROM qa_reference),
       1.0
UNION ALL
SELECT 'Q_QA_AGREEMENT', 'matched reference IDs',
       count(*) FILTER (WHERE abs(temperature_c - reference_c) <= 0.05),
       count(*),
       0.95
FROM matched;
