# Nhóm 3 — Checkpoint 2 Buổi 6

## Đề tài và thành viên

**Hệ thống phát hiện gian lận tài chính theo thời gian thực.**

- Hoàng Thị Diệu Hương
- Nguyễn Thị Hằng My
- Nguyễn Hoàng Dương
- Thân Văn Dũng

## Mục tiêu Buổi 6

Hoàn thành luồng **Environment → Ingestion → Raw Storage**:

- Chạy được Spark và lưu bằng chứng kiểm tra môi trường.
- Truy cập được nguồn dữ liệu PaySim.
- Chạy script ingestion, giữ nguyên dữ liệu gốc.
- Lưu raw theo dataset, ngày ingest và run ID.
- Có manifest, log và kết quả Spark đọc raw.

## Cấu trúc repo

```text
BIGDATA_NHOM_3/
├── README.md
├── .gitignore
├── src/
├── data/
│   └── raw/
├── logs/
├── metadata/
└── notebooks/
    └── 01_Checkpoint_Buoi6_Kaggle.ipynb
```

Raw lớn được lưu ở Kaggle Notebook Outputs và bản dự phòng của nhóm. Git chỉ lưu file giữ chỗ trong data/raw/, code, notebook, log nhỏ và metadata.

## Nguồn dữ liệu

- Dataset: **Synthetic Financial Datasets For Fraud Detection — PaySim**.
- Nguồn: https://www.kaggle.com/datasets/ealaxi/paysim1
- Loại dữ liệu: giao dịch tài chính mô phỏng.
- Giấy phép trên data card: CC BY-SA 4.0; giữ thông tin nguồn và điều kiện giấy phép khi chia sẻ lại.
- Snapshot được nhận diện bằng URL nguồn, version nếu biết, dung lượng và SHA-256 trong `metadata/source.json`.

## Môi trường và cách chạy

Nhóm sử dụng **Kaggle Notebook, CPU, Spark local[2]**. Notebook cấu hình PySpark 4.1.1 và JDK 17/21; version thực tế được ghi sau khi chạy.

1. Import `notebooks/01_Checkpoint_Buoi6_Kaggle.ipynb` vào Kaggle.
2. Chọn Accelerator None/CPU và bật Internet để cài dependency.
3. Add Input dataset `ealaxi/paysim1`; không cần upload lại CSV.
4. Chạy các ô theo thứ tự: setup → smoke test → ingest sample → ingest full → kiểm rerun → xuất hồ sơ.
5. Chọn Save Version → Save & Run All, chờ hoàn tất và kiểm Outputs.
6. Tải hồ sơ về, đưa script/log/metadata vào repo và ghi commit/tag thật.

Các lệnh dưới đây chạy tại `/kaggle/working/nhom3_buoi6` sau khi notebook hoàn tất setup và tạo script. Thay các giá trị `<...>` bằng đường dẫn thực tế. Notebook có sẵn các bước gọi script và lưu log.

Kiểm tra môi trường:

```bash
python src/env_check.py
```

PASS khi có Spark version, count bằng `1000000` và tiến trình kết thúc thành công.

Ingest dữ liệu:

```bash
# Lệnh đơn (tự động phát hiện file nguồn có sẵn trong repo):
python src/ingest_paysim.py

# Hoặc chỉ định rõ file nguồn / URL:
python src/ingest_paysim.py --source "<CSV_nguon_hoac_URL>" --dataset paysim --raw-root data/raw
```

Nguồn `--source` có thể là đường dẫn file local hoặc URL HTTP(S). Mỗi lần chạy tạo
một thư mục run mới trong `data/raw/`, cập nhật manifest mới nhất tại
`metadata/manifest.json` và ghi log tại `logs/ingest_<timestamp>_<run_id>.log`.
Manifest chứa `source`, `ingest_time`, `bytes`, `hash` (SHA-256), `destination` và `status` (`SUCCESS`
hoặc `FAILED`), cùng thông tin dataset và run ID.

Kiểm tra manifest và đọc raw bằng Spark:

```bash
python src/verify_manifest.py --manifest "<manifest.json>"
python src/inspect_raw.py --manifest "<manifest.json>"
```

## Raw storage và truy vết

```text
data/raw/paysim/ingest_date=YYYY-MM-DD/run_<UTC_TIMESTAMP>_<id>/
├── <file_nguon>.csv
└── manifest.json
```

Script kiểm nguồn không rỗng, đúng header và đọc được sample; sao chép nguyên byte, đối chiếu dung lượng và SHA-256. Không sửa tay hoặc làm sạch trong raw. Spark đọc file đã ingest để kiểm số dòng, schema và sample.

Manifest ghi nguồn, run ID, thời gian ingest UTC, dung lượng, SHA-256, đường dẫn đích và trạng thái. Log lỗi được lưu nếu ingestion thất bại.

**Policy chạy lại:** mỗi lần thành công tạo một run mới, không ghi đè run cũ. Chạy 3 lần cùng snapshot tạo 3 run có cùng hash. Chỉ đọc một run được chọn khi xử lý tiếp, tránh nhân bản dữ liệu. Kết quả thử thực tế được lưu trong hồ sơ checkpoint.

## Kết quả và bằng chứng

Điền các mục sau từ lần chạy thực tế trước khi nộp.

| Nội dung | Kết quả hoặc vị trí bằng chứng |
|---|---|
| Python / Java / Spark version | Python 3.12.10 / Java 17.0.12 / Spark 4.2.0 |
| Smoke test | PASS (Minh chứng: `logs/env_check.txt`) |
| Nguồn và snapshot | metadata/source.json |
| Số dòng raw | 6,362,620 dòng (xem `metadata/source.json`) |
| Raw location | `data/raw/paysim/ingest_date=YYYY-MM-DD/run_<timestamp>_<id>/` (chi tiết tại `metadata/manifest.json`) |
| Manifest location | `metadata/manifest.json` (và bản sao trong thư mục run raw) |
| Ingestion log | `logs/ingest_<timestamp>_<run_id>.log` |
| Spark count/schema/sample | [Điền logs/spark_full_<run_id>.txt] |
| Kiểm chạy lại 3 lần | [Điền kết quả và đường dẫn bằng chứng] |
| Kaggle Notebook version | [Điền URL version đã chạy thành công] |
| Git commit/tag | [Điền commit hash hoặc tag checkpoint-2 thực tế] |
| Blocker còn lại | [Điền vấn đề thực tế; nếu không còn thì ghi rõ] |
| Owner và deadline | [Điền người xử lý và hạn nếu còn blocker] |

## Exit ticket

1. **Lệnh kiểm tra môi trường:** `python src/env_check.py`, sau khi setup.
2. **Xác định snapshot:** URL nguồn, version nếu biết, dung lượng và SHA-256.
3. **Chạy ingestion 3 lần:** tạo 3 run riêng; cùng hash nếu nguồn không thay đổi; không ghi đè.
4. **Rủi ro trước Buổi 7:** [Nhóm điền rủi ro, người phụ trách và hạn xử lý thực tế].
