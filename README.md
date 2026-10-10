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

## Trạng thái T1 (bàn giao cho T2 — Người B)

| Hạng mục | Trạng thái |
|---|---|
| I01–I03 (SHA-256/byte-size với trusted manifest, envelopes, parse failures) | PASS (local) |
| I04 (curator `s3-ingestor`: GET raw OK, PUT release → 403 AccessDenied) | PASS, chạy trong Pod `curate` ns `bd-g06` ngày 2026-10-10 |
| `contract.json` schema | PASS |
| Review độc lập `contract.json` | **PENDING** — `review_status` vẫn `PENDING_INDEPENDENT_REVIEW` |

- Contract SHA-256 cần chốt: `e0aa69b0287f817fa50f6ddc770bf20f3f74ebf807b9b7362a91e5c41637d1d8`.
- **Chỉ bắt đầu triển khai T2 sau khi người thứ hai review contract và chốt hash này.** Đổi contract = version/run mới.
- Lab 1 đã được dựng lại trên cụm Docker Desktop (SeaweedFS `objects:8333`, bucket `research-raw`/`research-release`, Secret `s3-*`, client pods). Bốn file input đã nằm ở `research-raw/lab2/inputs/batch-01/`. Pod `curate` dùng image Lab 1 `bd-g06-s3-client:lab1` (chưa ghim digest; thay bằng image giảng viên duyệt nếu có yêu cầu). Credential nằm ở `lab/private/`, không commit.
- `fixture/` và `work/` bị `.gitignore`: mỗi người tự tải ZIP + trusted manifest rồi chạy lại các lệnh ở phần "Chạy T1 trên local" để tạo `work/` (kèm `work/evidence/I01–I03.json`).

### Đầu vào cho T2

- `work/envelopes.csv`: 10.205 physical records (5.000 CSV + 5.205 JSONL, gồm 5 dòng JSONL parse lỗi).
- `work/snapshot/sensors.csv` (20 sensor), `work/snapshot/qa_reference.csv` (100 cặp, **chỉ để đánh giá sau curate**, không dùng sửa nhiệt độ).
- `contract.json` và scaffold `sql/typed.sql` (chạy DuckDB với working directory là `work/`).

### Việc của T2

Viết `quality.sql` với các metric: required completeness, key uniqueness, value validity, registry consistency, chronology, timeliness; sau T3 bổ sung reference coverage/agreement. Mỗi metric ghi cohort, numerator, denominator, threshold, status; mẫu số 0 = `NOT_EVALUATED`.

Quy tắc quan trọng:
- Before-quality population **W** = winner theo ID **trước** lọc row-validity, không phải mọi dòng raw.
- Winner: `ingest_time` mới nhất, tie-break `source_object`/`source_row` tăng dần.
- Nhiệt độ có thể C hoặc F; hợp lệ trong [-30, 60]°C.
- Chronology: `event <= ingest <= 2026-02-09T00:00:00Z`; late nếu trễ > 900s.
- Báo cáo riêng: parse failures, key-rejected, duplicates, late.

Đầu ra: `quality.sql`, `quality_before.json`, sau T3 `quality_after.json`, biểu đồ before/after, và ít nhất 3 nhóm lỗi có nguồn tham chiếu. `raw_payload` và `operator_email` là restricted, không đưa vào report.

`fixture/`, `work/`, các file credential/Secret không được commit. Dữ liệu gốc và manifest độc lập được mỗi thành viên tải từ nguồn được phép, rồi tự chạy lại script.
