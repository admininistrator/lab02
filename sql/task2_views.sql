-- Eligible records before deduplication.
CREATE OR REPLACE VIEW key_eligible AS
SELECT *
FROM typed
WHERE parse_ok IS TRUE
  AND record_id IS NOT NULL
  AND regexp_matches(record_id, '^R[0-9]{6}$')
  AND ingest_ts IS NOT NULL;

-- Select one deterministic winner per normalized record_id.
CREATE OR REPLACE VIEW winners AS
SELECT * EXCLUDE (rn)
FROM (
    SELECT *,
           row_number() OVER (
               PARTITION BY record_id
               ORDER BY ingest_ts DESC,
                        source_object ASC,
                        source_row ASC
           ) AS rn
    FROM key_eligible
) AS ranked
WHERE rn = 1;
