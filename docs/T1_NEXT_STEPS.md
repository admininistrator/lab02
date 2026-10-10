# Lab 2 / T1 — tiếp tục từ trạng thái thực tế

## Những gì log của bạn đã xác nhận

- Context là `docker-desktop`; node `desktop-control-plane` là `Ready`.
- Namespace `bd-g06` và `team-budget` quota tồn tại; không cần tạo lại cluster.
- Namespace có `private-object-store` NetworkPolicy; tuy nhiên **không có Service hoặc Secret**.
- Pod `quota-positive` là `Unknown`; Pod `curate` mới tạo ở trạng thái `Pending`.
- `kubectl cp` lỗi `container not found ("client")` là **hệ quả** của container chưa chạy.
- `.venv` đã tạo với Python 3.12, DuckDB 1.4.5, boto3 và jsonschema.
- Local I01, I02, I03 đều PASS: 4 hash đúng, 10.205 envelopes, 5 parse failures giữ lại, byte-tamper bị chặn.
- `CONTRACT_SCHEMA: PASS` nhưng `review_status=PENDING_INDEPENDENT_REVIEW`.

## A. Chẩn đoán blocker Kubernetes (chỉ đọc)

Từ `C:\Users\Admin\Documents\GitHub\lab02`:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\diagnose_lab1.ps1 -Namespace bd-g06 -Lab1Path ..\lab
```

Xem `work/evidence/k8s-diagnostic.txt` và nhất là phần Events từ `kubectl describe pod curate`.

Phân loại các trường hợp:

| Tín hiệu | Hành động an toàn |
|---|---|
| `FailedMount` / `secret "s3-ingestor" not found` | Khôi phục Secret từ quy trình Lab 1; không copy plain-text credential vào repo. |
| `InvalidImageName` / image placeholder | Sửa `k8s/curate-pod.yaml` bằng instructor-approved image có digest thật. |
| `ImagePullBackOff` | Kiểm tra image tồn tại, quyền pull, registry và digest. |
| `CreateContainerConfigError` | Xem events: thường do Secret/config không tồn tại. |
| `FailedScheduling` / quota | Xem `kubectl describe pod`, quota và Pod request; không tự nâng quota. |
| `objects` Service missing | Khôi phục deployment/Service object storage từ repo Lab 1, kiểm tra PVC và network policy. |

**Quan trọng:** Không `kubectl apply -f` tất cả manifest Lab 1 một cách mù quáng. Dùng file nguồn thật và thứ tự áp dụng của Lab 1; do Kubernetes object storage có dữ liệu/credential stateful. Kiểm tra `kubectl get pvc -n bd-g06`, tránh xóa PVC. `quota-positive` Unknown có thể xóa khi đã xác minh không cần dùng, nhưng không phải nguyên nhân duy nhất làm `curate` Pending.

Nếu manifest Lab 1 đã mất, cần lấy lại từ repo/bài nộp/giảng viên. Không thể tái tạo Secret policy chính xác hoặc chọn một approved image khi chưa biết cấu hình gốc.

## B. Chốt T1 local và bàn giao T2

```powershell
python scripts/verify_t1.py
python scripts/validate_contract.py
python scripts/write_t1_evidence_index.py
```

Lưu `work/evidence/T1_STATUS.json`. Yêu cầu một người khác review contract và ghi nhận người duyệt; **không tự nhận đã được review**. Nếu họ thay `contract.json`, hash thay đổi, cần dùng phiên bản contract mới cho T2 trở đi.

T2 có thể bắt đầu phát triển trên `work/envelopes.csv`, snapshot và `contract.json` sau khi contract được duyệt; nhưng phải ghi chú dataset hiện được lấy qua `local://`, **chưa có bằng chứng S3**. Lần chạy chính thức sau khi phục hồi storage phải lấy đầu vào thông qua curator role.

## C. Sau khi phục hồi Lab 1

1. Phải có `objects` Service, bucket `research-raw`/`research-release`, role `s3-ingestor`, và image đã được phê duyệt.
2. Tạo Pod theo đúng `k8s/curate-pod.yaml`, **không để image placeholder**. Nếu Pod cũ cần thay image, xem `kubectl describe` trước; chỉ xóa/recreate đúng `curate` sau khi đã lưu evidence trong emptyDir (nếu có).
3. Đợi Pod chạy và `client` Ready.
4. Xác nhận bốn dữ liệu đã được preload ở `research-raw/lab2/inputs/batch-01/`. Nếu thiếu, người được cấp quyền setup phải dùng `upload_inputs.py`, không dùng curator để đổi chính sách.
5. Chạy:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_s3_t1.ps1 -Namespace bd-g06
```

Script này chặn chạy khi Pod chưa Ready, Service/Secret chưa có; copy source, chạy staging S3, thực hiện GET raw và PUT release bị từ chối, rồi kéo evidence về `work/s3/`. Sau đó chạy lại:

```powershell
python scripts/write_t1_evidence_index.py
```

Đừng commit `work/`, `fixture/`, hay bất kỳ credential/raw email; repo `.gitignore` đã loại trừ những thư mục này.

## D. Chưa thể tự làm gì từ transcript

Các lệnh `kubectl` chạy ở máy Windows của bạn. Bộ script này **không thể tự thao tác với cluster Docker Desktop** trong phiên ChatGPT. Tôi cũng không có image digest hoặc Secret của Lab 1. Vì vậy I04 và `curate` Ready vẫn phải được thực thi trên cluster của bạn; không thể báo chúng đã PASS từ các log hiện có.
