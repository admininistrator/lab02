# Lab 2 — Task 4: Metadata, lineage và retention

> **Trạng thái:** bản nháp chỉ dành cho Task 4. Các kết quả ghi `local` là bằng chứng chạy trên snapshot cục bộ. Không đổi `PENDING` hoặc `BLOCKED` thành PASS nếu chưa có người/runtime thực hiện xác nhận.

## Mục tiêu

Task 4 biến kết quả curation thành một handover có thể tìm kiếm và kiểm chứng: catalog mô tả raw observations, quarantine và curated candidate; README giải thích cách dùng; OpenLineage nối job với input/output; hai trace lần theo record; retention plan thể hiện quyết định giữ/xóa theo thời hạn, hold và dependency.

## Vai trò và phần việc

Vai trò kỹ thuật tương ứng Task 4 trong phân công là **Role D — Metadata**: catalog, README, lineage, retention và kiểm tra consumer. Run record hiện ghi operator là `Role-C-Curation`, tức là lần chạy curation được lưu không phải bằng chứng ai thực hiện Task 4. Điền tên người nộp và vai trò thật của bạn trước khi nộp; không tự nhận thay phần peer review.

## Kết quả hiện có

- `catalog.json` có 3 dataset record: raw observations, quarantine và curated candidate. Catalog mô tả nguồn CSV/JSONL, schema/columns, AS_OF, owner/steward, allowed use, retention và provenance.
- Đã tạo `task4/draft-manifest.json` cho peer handover. Đây là candidate draft, ghi `DRAFT_NOT_APPROVED`; không phải release manifest hay phê duyệt xuất bản.
- M01 trả về 1 curated candidate với 9.610 rows, owner/steward, vị trí local và schema version.
- M02 trả raw và quarantine là restricted; candidate có allowlist 10 cột và không có email.
- README ghi mục đích synthetic, files/keys, unit/time, chính sách dedup/curation, cohort/chất lượng, access boundary, phiên bản phần mềm và cách reproduce.
- OpenLineage curate START/COMPLETE dùng cùng run UUID; `validate_lineage.py` báo PASS cho schema và các kiểm tra ngữ nghĩa cơ bản.
- Đã lưu ví dụ L02 cho một record được chấp nhận và L03 cho một record quarantine, cùng câu SQL và giá trị provenance/reason.
- Retention plan có đủ bốn dry-run decisions: eligible scratch; giữ do legal hold; giữ do active dependency; giữ vì chưa hết hạn. Đây là dry-run, không có dữ liệu nào bị xóa.
- Bộ kiểm tra catalog đối chiếu file tồn tại và hash artifact với quality report/run record; kiểm tra được 10.205 raw envelopes và 395 quarantine rows. Kết quả tổng thể hiện là `PARTIAL`.

## Việc còn thiếu để chốt Task 4

1. **Schema/count Parquet:** chưa đo trực tiếp schema và row count từ `curated.parquet`, vì DuckDB không khả dụng trong runtime hiện tại. Script kiểm tra đã chuẩn bị tại `task4/inspect_candidate.py`. Chạy `python task4/inspect_candidate.py work/candidate/curated.parquet` trong môi trường có DuckDB; yêu cầu output `PASS`, 10 cột đúng kiểu và 9.610 rows.
2. **M03:** mời một thành viên chỉ xem `catalog.json`, `DATA_README.md` và `task4/draft-manifest.json`, trả lời năm câu trong `task4/catalog_queries.json`, ghi câu trả lời/số câu đúng/người trả lời. Hiện trạng là `PENDING_INDEPENDENT_PEER`.
3. **Trace peer check:** một người khác chạy lại L02/L03 query từ artifacts, xác nhận source object, SHA-256, source row, selected version hoặc primary/all reasons trùng kết quả đã lưu; ghi truy vấn và người kiểm tra.
4. **Địa điểm thật:** catalog hiện trỏ đến các file local. Nếu nhóm đã chạy trên object store thì thay bằng URI thực và xác minh object tồn tại/hash; nếu chưa, giữ nguyên nhãn local và không mô tả như đã publish trên S3.
5. **Thông tin người phụ trách:** xác nhận tên/role thực của owner, steward, operator và reviewer nếu rubric yêu cầu người thật thay vì nhãn role.

## Nội dung peer cần trả lời (M03)

1. `temperature_c` dùng đơn vị nào?
2. Reference population có bao nhiêu ID và được dùng với mục đích gì?
3. AS_OF là thời điểm nào?
4. Nêu hai giới hạn đã được README ghi nhận.
5. Ai là steward liên hệ và ai là owner?

Peer chỉ xem `catalog.json` và `DATA_README.md`; không mở notebook/code trong khi trả lời. Đính kèm bảng câu trả lời và số câu đúng sau khi peer hoàn tất.

## Checklist ảnh minh chứng Task 4

| Ảnh | Chụp gì | Điều kiện |
|---|---|---|
| T4-1 Catalog | `catalog.json` hiển thị đủ 3 dataset, location, row_count, owner/steward và schema version | Chụp bản cuối sau khi kiểm tra location/count. |
| T4-2 M01/M02 | Kết quả M01 có 1 candidate; M02 có raw/quarantine restricted và `release_has_email: false` | Có thể chụp `task4/catalog_queries.json` hoặc output lệnh tạo truy vấn. |
| T4-3 README handover | README với các mục purpose, units/time, quality limitations, allowed use/access và reproduce | Chụp toàn bộ các mục liên quan, không cần chụp dài dòng từng đoạn. |
| T4-4 Schema/count/hash | Output `python task4/inspect_candidate.py work/candidate/curated.parquet` và kết quả validator | Chưa đủ điều kiện chụp PASS; cần DuckDB chạy thành công trước. |
| T4-5 Lineage L01 | Output validator `PASS`, hai event START/COMPLETE và cùng run UUID | Không chụp giả lập; giữ output thực của validator. |
| T4-6 Trace L02/L03 | Peer chạy hai query, kết quả khớp artifact và ghi người kiểm tra | Chụp sau khi peer thực hiện độc lập. |
| T4-7 M03 | Năm câu trả lời của peer, số câu đúng, tên/role và ngày xác nhận | Chụp sau khi peer hoàn tất; không đưa đáp án vào tài liệu phát cho peer trước đó. |
| T4-8 Retention | Bốn dry-run cases cùng decision/reason | Thể hiện rõ đây là dry-run; không có thao tác xóa thật. |

Không chụp credential, token, Secret, kubeconfig hoặc raw email. Các artifact chính của Task 4: [catalog.json](catalog.json), [DATA_README.md](DATA_README.md), [draft-manifest.json](task4/draft-manifest.json), [catalog_queries.json](task4/catalog_queries.json), [lineage.jsonl](lineage.jsonl), [lineage_traces.json](work/evidence/lineage_traces.json), [retention-plan.json](retention-plan.json), [inspect_candidate.py](task4/inspect_candidate.py).
