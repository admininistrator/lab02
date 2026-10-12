import os
import duckdb

# Đảm bảo tạo thư mục work/staging và work/candidate đúng chỗ
os.makedirs("../work/staging", exist_ok=True)
os.makedirs("../work/candidate", exist_ok=True)

con = duckdb.connect(database=':memory:')

print("Đang khởi chạy pipeline Task 3 với các quy tắc quản trị nâng cao...")

# 1. Đọc dữ liệu thô (Source A: CSV và Source B: JSON Lines đọc dạng text an toàn tránh crash)
con.execute("""
CREATE OR REPLACE VIEW raw_envelopes AS 
-- Nguồn A: observations_a.csv
SELECT 
    'observations_a.csv' AS source_object,
    ROW_NUMBER() OVER () AS source_row,
    'sha256_mock_a' AS source_sha256,
    TRY_CAST(record_id AS VARCHAR) AS record_id,
    TRY_CAST(sensor_id AS VARCHAR) AS sensor_id,
    TRY_CAST(event_time AS VARCHAR) AS event_time,
    TRY_CAST(ingest_time AS VARCHAR) AS ingest_time,
    TRY_CAST(reading AS VARCHAR) AS reading,
    TRY_CAST(unit AS VARCHAR) AS unit,
    TRY_CAST(operator_email AS VARCHAR) AS operator_email,
    TRUE AS parse_ok
FROM read_csv_auto('../input/observations_a.csv')

UNION ALL

-- Nguồn B: observations_b.jsonl (Dùng cột 'content' và bắt lỗi json_valid an toàn)
SELECT 
    'observations_b.jsonl' AS source_object,
    ROW_NUMBER() OVER () AS source_row,
    'sha256_mock_b' AS source_sha256,
    CASE WHEN TRY_CAST(json_valid(content) AS BOOLEAN) THEN TRY_CAST(json_extract(content, '$.id') AS VARCHAR) ELSE NULL END AS record_id,
    CASE WHEN TRY_CAST(json_valid(content) AS BOOLEAN) THEN TRY_CAST(json_extract(content, '$.sensor') AS VARCHAR) ELSE NULL END AS sensor_id,
    CASE WHEN TRY_CAST(json_valid(content) AS BOOLEAN) THEN TRY_CAST(json_extract(content, '$.timestamp') AS VARCHAR) ELSE NULL END AS event_time,
    CASE WHEN TRY_CAST(json_valid(content) AS BOOLEAN) THEN TRY_CAST(json_extract(content, '$.arrived_at') AS VARCHAR) ELSE NULL END AS ingest_time,
    CASE WHEN TRY_CAST(json_valid(content) AS BOOLEAN) THEN TRY_CAST(json_extract(content, '$.temperature') AS VARCHAR) ELSE NULL END AS reading,
    CASE WHEN TRY_CAST(json_valid(content) AS BOOLEAN) THEN TRY_CAST(json_extract(content, '$.temperature_unit') AS VARCHAR) ELSE NULL END AS unit,
    CASE WHEN TRY_CAST(json_valid(content) AS BOOLEAN) THEN TRY_CAST(json_extract(content, '$.operator') AS VARCHAR) ELSE NULL END AS operator_email,
    TRY_CAST(json_valid(content) AS BOOLEAN) AS parse_ok
FROM read_text('../input/observations_b.jsonl');
""")

# 2. Đọc bảng sensors để lấy thông tin site
con.execute("""
CREATE OR REPLACE VIEW sensors_registry AS 
SELECT UPPER(TRIM(sensor_id)) AS sensor_id, UPPER(TRIM(site)) AS site 
FROM read_csv_auto('../input/sensors.csv');
""")

# 3. Phân tích cấu trúc, định dạng thời gian và giá trị sơ bộ
con.execute("""
CREATE OR REPLACE VIEW parsed_data AS 
SELECT 
    source_object,
    source_row,
    source_sha256,
    record_id,
    UPPER(TRIM(record_id)) AS raw_record_id,
    UPPER(TRIM(sensor_id)) AS raw_sensor_id,
    UPPER(TRIM(unit)) AS raw_unit,
    event_time,
    ingest_time,
    reading,
    operator_email,
    parse_ok,
    TRY_STRPTIME(event_time, '%Y-%m-%dT%H:%M:%SZ') AS event_time_utc,
    TRY_STRPTIME(ingest_time, '%Y-%m-%dT%H:%M:%SZ') AS ingest_time_utc,
    TRY_CAST(reading AS DECIMAL(10,4)) AS reading_num
FROM raw_envelopes;
""")

# 4. Gắn nhãn lỗi chi tiết theo thứ tự ưu tiên (Precedence Rule) cho toàn bộ dữ liệu trước khử trùng lặp
con.execute("""
CREATE OR REPLACE VIEW evaluated_records AS 
SELECT 
    *,
    CASE 
        WHEN NOT parse_ok OR event_time IS NULL OR ingest_time IS NULL OR reading IS NULL THEN 'PARSE'
        WHEN record_id IS NULL OR TRIM(record_id) = '' OR NOT regexp_matches(raw_record_id, '^R[0-9]{6}$') THEN 'KEY'
        WHEN event_time_utc IS NULL OR ingest_time_utc IS NULL THEN 'TIME_PARSE'
        WHEN event_time_utc > ingest_time_utc OR ingest_time_utc > TIMESTAMP '2026-02-09 00:00:00' THEN 'TIME_ORDER'
        WHEN raw_unit NOT IN ('C', 'F') OR raw_unit IS NULL THEN 'UNIT'
        WHEN reading_num IS NULL OR NOT ISFINITE(reading_num) THEN 'NUMERIC'
        -- Kiểm tra dải nhiệt độ sau khi quy đổi về độ C (-30 đến 60 độ C)
        WHEN (CASE WHEN raw_unit = 'F' THEN (reading_num - 32.0) * 5.0 / 9.0 ELSE reading_num END) < -30.0 
          OR (CASE WHEN raw_unit = 'F' THEN (reading_num - 32.0) * 5.0 / 9.0 ELSE reading_num END) > 60.0 THEN 'RANGE'
        ELSE 'VALID'
    END AS error_reason
FROM parsed_data;
""")

# 5. Tách các bản ghi lỗi format/key/thời gian/đơn vị ban đầu vào quarantine.csv
con.execute("""
COPY (
    SELECT 
        source_object, 
        source_row, 
        COALESCE(record_id, 'UNKNOWN') AS record_id,
        error_reason AS primary_reason
    FROM evaluated_records
    WHERE error_reason != 'VALID'
) TO '../work/staging/quarantine.csv' (HEADER, FORMAT CSV);
""")

# 6. Lọc các bản ghi hợp lệ về mặt cấu trúc/đơn vị/giá trị để đưa vào tiến hành khử trùng lặp
con.execute("""
CREATE OR REPLACE VIEW valid_for_dedup AS 
SELECT * 
FROM evaluated_records 
WHERE error_reason = 'VALID';
""")

# 7. Khử trùng lặp (Deduplication) bằng cửa sổ ROW_NUMBER()
# Ưu tiên: ingest_time_utc mới nhất, phá băng bằng source_object ASC, source_row ASC
con.execute("""
CREATE OR REPLACE VIEW ranked_records AS 
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY raw_record_id 
           ORDER BY ingest_time_utc DESC, source_object ASC, source_row ASC
       ) as rn
FROM valid_for_dedup;
""")

# 8. Xuất các phiên bản phụ bị loại (duplicates) vào duplicates.csv
con.execute("""
COPY (
    SELECT 
        source_object, 
        source_row, 
        raw_record_id AS record_id,
        'SUPERSEDED' AS reason
    FROM ranked_records
    WHERE rn > 1
) TO '../work/staging/duplicates.csv' (HEADER, FORMAT CSV);
""")

# 9. Xử lý bản ghi chiến thắng (Winner), chuẩn hóa chuyển đổi đơn vị nhiệt độ sang C
con.execute("""
CREATE OR REPLACE VIEW winning_candidates AS 
SELECT 
    r.raw_record_id AS record_id,
    r.raw_sensor_id AS sensor_id,
    s.site,
    r.event_time_utc,
    r.ingest_time_utc,
    r.reading_num,
    r.raw_unit AS unit,
    r.source_object,
    r.source_row,
    r.source_sha256,
    EXTRACT(EPOCH FROM (r.ingest_time_utc - r.event_time_utc)) AS lag_seconds,
    CASE 
        WHEN r.raw_unit = 'F' THEN ROUND((r.reading_num - 32.0) * 5.0 / 9.0, 2)
        WHEN r.raw_unit = 'C' THEN ROUND(r.reading_num, 2)
        ELSE NULL
    END AS temperature_c
FROM ranked_records r
LEFT JOIN sensors_registry s ON r.raw_sensor_id = s.sensor_id
WHERE r.rn = 1;
""")

# 10. Xuất dữ liệu sạch chính thức ra tệp Parquet (Chuẩn Allowlist 10 cột)
con.execute("""
COPY (
    SELECT 
        record_id,
        sensor_id,
        site,
        event_time_utc,
        ingest_time_utc,
        temperature_c,
        CASE WHEN lag_seconds > 900 THEN TRUE ELSE FALSE END AS is_late,
        source_object,
        source_row,
        source_sha256
    FROM winning_candidates
) TO '../work/candidate/curated.parquet' (FORMAT PARQUET);
""")

print("✨ Pipeline Task 3 đã chạy hoàn tất thành công!")

# --- KHỐI TỰ ĐỘNG ĐỐI SOÁT BẢO TOÀN DỮ LIỆU (CONSERVATION CHECK C01) ---
raw_count = con.execute("SELECT COUNT(*) FROM raw_envelopes").fetchone()[0]
curated_count = con.execute("SELECT COUNT(*) FROM read_parquet('../work/candidate/curated.parquet')").fetchone()[0]
quarantine_count = con.execute("SELECT COUNT(*) FROM read_csv_auto('../work/staging/quarantine.csv')").fetchone()[0]
duplicate_count = con.execute("SELECT COUNT(*) FROM read_csv_auto('../work/staging/duplicates.csv')").fetchone()[0]

total_output = curated_count + quarantine_count + duplicate_count

print("\n" + "="*50)
print("       BẢNG ĐỐI SOÁT BẢO TOÀN DỮ LIỆU (CONSERVATION C01)")
print("="*50)
print(f"🔹 Tổng số bản ghi thô đầu vào (N_raw)      : {raw_count}")
print(f"🔹 Bản ghi sạch chính thức (Curated)       : {curated_count}")
print(f"🔹 Bản ghi lỗi kiểm duyệt (Quarantine)     : {quarantine_count}")
print(f"🔹 Bản ghi trùng lặp bị loại (Duplicates)  : {duplicate_count}")
print("-" * 50)
print(f"🔸 Tổng các đầu ra (Curated + Q + D)       : {total_output}")

if raw_count == total_output:
    print("✅ KẾT QUẢ: PASS! Định luật bảo toàn tập hợp thỏa mãn tuyệt đối (Không mất mát ngầm).")
else:
    print(f"❌ KẾT QUẢ: FAIL! Lệch số lượng bản ghi: {raw_count - total_output}")
print("="*50)
print(" - work/candidate/curated.parquet")
print(" - work/staging/quarantine.csv")
print(" - work/staging/duplicates.csv")