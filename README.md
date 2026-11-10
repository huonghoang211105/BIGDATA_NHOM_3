# Nhóm 3 — Hệ Thống Phát Hiện Gian Lận Tài Chính Theo Thời Gian Thực

## Đề tài và thành viên

- **Đề tài:** Hệ thống phát hiện gian lận tài chính theo thời gian thực (PaySim Financial Fraud Detection).
- **Thành viên Nhóm 3:**
  1. Hoàng Thị Diệu Hương
  2. Nguyễn Thị Hằng My
  3. Nguyễn Hoàng Dương
  4. Thân Văn Dũng

---

## 1. Cấu trúc Repository

```text
BIGDATA_NHOM_3/
├── README.md                           # Tài liệu hướng dẫn đồ án và cách chạy
├── requirements.txt                    # Danh sách thư viện phụ thuộc (PySpark)
├── .gitignore                          # Quy tắc loại trừ tệp lớn, venv, output
├── config/
│   ├── config.json                     # Cấu hình pipeline (đường dẫn, hash, kỳ vọng)
│   └── dim_transaction_type.csv        # Dimension loại giao dịch (5 categories)
├── data/
│   └── raw/
│       ├── .gitkeep
│       ├── paysim_data.csv             # Snapshot raw PaySim (493.5MB, SHA-256 xác thực)
│       └── checkpoint3_input.json      # Hồ sơ truy vết relocation snapshot CP3
├── evidence/
│   └── checkpoint3/
│       └── 20261110T095859Z_ec6b3d33/  # Bằng chứng run SUCCESS (metrics, explain, samples)
├── metadata/
│   ├── source.json                     # Thông tin nguồn dữ liệu Kaggle PaySim
│   ├── manifest.json                   # Manifest ingestion Checkpoint 2
│   └── spark_read_*.json               # Bằng chứng Spark đọc raw ban đầu
├── reports/
│   ├── checkpoint3_template.md         # Template báo cáo Checkpoint 3
│   └── checkpoint3.md                  # Báo cáo kết quả chính thức Checkpoint 3
├── src/
│   ├── env_check.py                    # Kiểm tra môi trường Python/Java/Spark
│   ├── ingest_paysim.py                # Script ingestion PaySim (CP2)
│   ├── inspect_raw.py                  # Kiểm tra raw data bằng Spark (CP2)
│   ├── verify_manifest.py              # Xác thực manifest và checksum SHA-256
│   ├── prepare_raw.py                  # Chuẩn bị và kiểm tra 1 snapshot raw duy nhất (CP3)
│   ├── quality_report.py               # Thư viện kiểm tra quality gates và profile dữ liệu
│   ├── pipeline.py                     # Entry point Spark Batch Processing Pipeline (CP3)
│   └── compare_runs.py                 # So sánh độc lập 2 lần chạy để kiểm chứng rerun
└── notebooks/
    ├── 01_Checkpoint_Buoi6_Kaggle_v2.ipynb
    └── 02_Train_Baseline_PaySim_Kaggle_v2.ipynb
```

---

## 2. Nguồn dữ liệu (Dataset Contract)

- **Dataset:** Synthetic Financial Datasets For Fraud Detection (PaySim).
- **Nguồn:** [Kaggle ealaxi/paysim1](https://www.kaggle.com/datasets/ealaxi/paysim1).
- **Snapshot duy nhất được sử dụng:**
  - File: `data/raw/paysim_data.csv` (1 file CSV duy nhất).
  - Dung lượng: 493.534.783 bytes.
  - Số dòng: 6.362.620 dòng, 11 cột.
  - SHA-256: `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b`.
  - Giấy phép: CC BY-SA 4.0.

---

## 3. Checkpoint 3 — Spark Batch Processing Pipeline

### 3.1. Kiến trúc luồng xử lý

```text
1 Snapshot PaySim CSV (493.5MB, 6.36M rows)
    │
    ▼
[Spark Read với Explicit Schema + FAILFAST]
    │
    ▼
[T1: Normalize Strings] ── upper(trim(type)), trim tài khoản
    │
    ▼
[T2: Validate & Quality Filter] ── kiểm null 11 cột, step >= 1, amount >= 0 (giữ 16 dòng amount=0)
    │                             tách invalid vào rejected/
    ▼
[T3: Derive Features] ── day_index (1..31), hour_index (0..23), log_amount (log1p)
    │
    ▼
[T4: Projection] ── chọn rõ 11 cột gốc + 3 cột phái sinh
    │
    ▼
[Left Join Dimension] ── kết nối config/dim_transaction_type.csv trên khóa type (Many-to-One)
    │                   BroadcastHashJoin không shuffle Fact; kiểm tra unmatched = 0
    ▼
[Aggregation & KPI Summary] ── groupBy(day_index, type, type_name_vi, flow_group)
    │                          tính txn_count, total_amount, avg_amount, fraud_count, fraud_amount, fraud_rate_pct
    ▼
[Write Output Parquet + Preview CSV] ── lưu theo snapshot/run_id riêng với errorifexists
    │
    ▼
[Readback Verification] ── đọc lại Parquet, kiểm tra đối soát totals, ghi _SUCCESS.json & metrics.json
```

### 3.2. Cài đặt và chuẩn bị môi trường

Yêu cầu hệ thống:
- Python >= 3.10 (khuyến nghị Python 3.12).
- Java JDK >= 17 (OpenJDK Temurin 17).
- Hadoop winutils (đối với Windows: lưu tại `D:\hadoop` hoặc `C:\hadoop`).

Cài đặt thư viện:
```bash
pip install -r requirements.txt
```

Kiểm tra môi trường:
```bash
python src/env_check.py
```

### 3.3. Một lệnh chạy toàn bộ Pipeline

Thực thi một lệnh duy nhất từ đầu đến cuối:
```bash
python src/pipeline.py --config config/config.json
```

Kết quả hợp lệ sẽ hiển thị:
- `[INPUT] rows=6362620 bytes=493534783`
- `[CLEAN] valid=6362620 rejected=0`
- `[JOIN] before=6362620 after=6362620 unmatched=0`
- `[OUTPUT] groups=152 path=output/snapshot_16910f90577b/<run_id>/daily_type_summary`
- `[BASELINE] elapsed_sec=...`
- `SUCCESS: output/snapshot_16910f90577b/<run_id>`

### 3.4. Kiểm tra và xác thực khả năng tái tạo (Rerun Verification)

Chạy pipeline lần 2 để tạo run thứ hai, sau đó so sánh kết quả 2 chiều:
```powershell
$cp3First = (Get-Content "output/latest_success.json" -Raw | ConvertFrom-Json).run_dir
python src/pipeline.py --config config/config.json
$cp3Second = (Get-Content "output/latest_success.json" -Raw | ConvertFrom-Json).run_dir
python src/compare_runs.py $cp3First $cp3Second
```

Kỳ vọng: **`RERUN PASS: identical schema and business rows in both directions`**.

---

## 4. Bảng số liệu đối soát chính thức (Run PASS)

Từ kết quả thực tế của run `20261110T095859Z_ec6b3d33` (lưu tại `evidence/checkpoint3/`):

| Chỉ số kiểm tra | Kỳ vọng đối soát | Kết quả Spark đo được | Trạng thái |
|---|---:|---:|:---:|
| Tổng số giao dịch (`txn_count`) | 6.362.620 | 6.362.620 | **PASS** |
| Số dòng mang nhãn gian lận (`fraud_count`) | 8.213 | 8.213 | **PASS** |
| Tổng giá trị giao dịch (`total_amount`) | 1.144.392.944.759,77 | 1.144.392.944.759,77 | **PASS** |
| Số nhóm `(day_index, type)` | 152 | 152 | **PASS** |
| Unmatched rows khi join Dimension | 0 | 0 | **PASS** |
| Số dòng rác / invalid (`rejected`) | 0 | 0 | **PASS** |
| Số giao dịch 0 đồng (được giữ lại) | 16 | 16 (cả 16 đều có isFraud=1) | **PASS** |
| Số dòng duplicate toàn bộ 11 cột | 0 | 0 | **PASS** |
| **Baseline Runtime (toàn bộ pipeline)** | — | **191,432 giây** (~3,19 phút) | **PASS** |
| **Rerun Verification (compare_runs.py)** | — | **RERUN PASS** | **PASS** |

---

## 5. Báo cáo và Hồ sơ nộp

- Báo cáo chi tiết Checkpoint 3: [`reports/checkpoint3.md`](file:///reports/checkpoint3.md).
- Minh chứng nhẹ đã lưu trong Git: [`evidence/checkpoint3/20261110T095859Z_ec6b3d33/`](file:///evidence/checkpoint3/20261110T095859Z_ec6b3d33/).
- Báo cáo 6 slide: Cập nhật các số đo từ run PASS vào template PowerPoint buổi học.
