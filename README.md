# Lab 2 — Data Curation, Quality & Metadata Governance

Repo riêng cho **Lab 2**, kế thừa Kubernetes namespace `bd-g06`, S3 object storage (`objects:8333`), buckets `research-raw` / `research-release` và ba role từ Lab 1. Không cần triển khai AWS hay đặt code trong repo Lab 1.

## Quy trình nhóm

- **T1 / Người A:** xác thực nguồn dữ liệu, tạo physical-row envelopes và `contract.json`.
- **T2 / Người B:** dùng DuckDB SQL đo chất lượng dữ liệu trước/sau.
- **T3 / Người C:** curated Parquet, quarantine và duplicate ledger, đảm bảo reconciliation.
- **T4 / Người D:** metadata catalog, OpenLineage, README và retention.
- **T5 / Người E:** release gate, mutation tests, owner review và analyst verification.

## Dữ liệu

- `observations_a.csv`: 5.000 bản ghi; `observations_b.jsonl`: 5.205 bản ghi (có 5 dòng JSON bị lỗi có chủ đích).
- `sensors.csv`: 20 sensor, dùng để kiểm tra registry và suy ra `site`.
- `qa_reference.csv`: 100 bản ghi tham chiếu, **chỉ để đánh giá**, không dùng để sửa số đo.
- `trusted_manifest.json` do giảng viên cấp riêng: là cơ sở tin cậy cho byte-size/SHA-256, không dùng bản manifest đi kèm trong ZIP để tự xác thực.
- `contract.json`: `research-telemetry-v1`; fixed AS_OF `2026-02-09T00:00:00Z`.

## Chạy T1 trên local

Đặt file `Lab2-datasets.zip` và `trusted_manifest.json` ngoài repo (ví dụ ở thư mục cha) rồi chạy từ thư mục gốc repo:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python scripts/prepare_fixture.py ../Lab2-datasets.zip ../trusted_manifest.json
python scripts/stage_inputs.py local fixture/trusted_manifest.json --output-dir work
python scripts/verify_t1.py
python scripts/validate_contract.py
```

Nếu không có Python 3.11, cài Python 3.11 hoặc chạy thử bằng phiên bản đang có; môi trường chuẩn của lab là 3.11. Chỉnh đường dẫn ZIP/manifest theo vị trí thực tế. Phải có người thứ hai review `contract.json` rồi chốt SHA-256 **trước T2**. Đổi contract phải tạo version/run mới.

**I01–I03** có thể kiểm tra local; **I04** cần curator Pod dùng identity `s3-ingestor` truy cập S3 thật: raw GET thành công và release PUT bị `403/AccessDenied`. Nếu Lab 1 chưa chạy, I04 là `BLOCKED`, không phải `PASS`.

## Bàn giao từ T1 (Người A) — thông tin những gì đã làm

### Kết quả kiểm tra

| Hạng mục | Trạng thái |
|---|---|
| I01–I03: SHA-256/byte-size khớp `trusted_manifest.json` độc lập, envelopes, parse failures | PASS (local) |
| I04: curator `s3-ingestor` GET raw OK, PUT release bị `403 AccessDenied` | PASS, chạy trong Pod `curate` ns `bd-g06` ngày 2026-10-10 |
| `contract.json` qua schema | PASS |
| Review độc lập `contract.json` | Chưa có — `review_status` vẫn `PENDING_INDEPENDENT_REVIEW` |

Contract SHA-256 hiện tại: `e0aa69b0287f817fa50f6ddc770bf20f3f74ebf807b9b7362a91e5c41637d1d8`. Theo yêu cầu lab, hash được chốt sau review trước T2; đổi contract thì tạo version/run mới.

### Dữ liệu đã chuẩn bị (`work/`, tạo bằng các lệnh ở trên, không commit)

- `envelopes.csv`: 10.205 physical-row envelopes (5.000 từ CSV + 5.205 từ JSONL), giữ cả 5 dòng JSONL parse lỗi.
- `snapshot/sensors.csv` (20 sensor) và `snapshot/qa_reference.csv` (100 cặp; tài liệu quy định chỉ để đánh giá, không dùng sửa số đo).
- `evidence/I01–I04.json`, `T1_STATUS.json`, `input_inventory.json`.
- `sql/typed.sql`: scaffold normalization do tài liệu cung cấp, chạy DuckDB với working directory là `work/`.

### Quy tắc đã ghi trong `contract.json` / tài liệu lab (để tham khảo)

- Contract `research-telemetry-v1`, AS_OF cố định `2026-02-09T00:00:00Z`.
- Before-quality population W là winner theo ID trước lọc row-validity, không phải toàn bộ raw.
- Winner theo `ingest_time` mới nhất, tie-break `source_object`/`source_row` tăng dần.
- Nhiệt độ có thể ở C hoặc F; dải hợp lệ [-30, 60]°C. Chronology `event <= ingest <= AS_OF`; late nếu > 900s.
- Mẫu số 0 là `NOT_EVALUATED`; metric có cohort, numerator, denominator, threshold.
- `raw_payload` và `operator_email` là restricted, không đưa vào report phát hành.

### Môi trường Lab 1 / S3 đã dựng lại

- Cụm Docker Desktop, ns `bd-g06`: SeaweedFS `objects:8333` (image ghim digest từ evidence Lab 1), bucket `research-raw` / `research-release`, Secret `s3-owner|ingestor|analyst|config`, client pods owner/ingestor/analyst/blocked. Credential sinh mới, nằm ở `lab/private/`, không commit.
- Bốn file input đã upload (có kiểm SHA-256) vào `research-raw/lab2/inputs/batch-01/`.
- Pod `curate` (`k8s/curate-pod.yaml`) dùng image Lab 1 `bd-g06-s3-client:lab1`, chưa ghim digest vì chưa có image giảng viên duyệt.
- Script: `prepare_fixture.py`, `stage_inputs.py`, `verify_t1.py`, `validate_contract.py`, `upload_inputs.py`, `check_s3_access.py`.

### Lưu ý

`fixture/`, `work/` và mọi credential bị `.gitignore`. Mỗi thành viên tự tải ZIP + trusted manifest từ nguồn được phép và chạy lại script. Evidence Lab 1 cũ không còn khớp credential mới.
