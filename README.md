# Nhóm 3 — Hệ Thống Phát Hiện Gian Lận Tài Chính Theo Thời Gian Thực

## Đề tài và Thành viên Nhóm 3

- **Đề tài:** Hệ thống phát hiện gian lận tài chính theo thời gian thực (Financial Fraud Detection on PaySim).
- **Thành viên nhóm:**
  1. **Hoàng Thị Diệu Hương** (Nhóm trưởng) — Thu thập evidence, tổng hợp báo cáo slide/markdown, hoàn thiện tài liệu dự án.
  2. **Nguyễn Thị Hằng My** — Quản lý schema, thiết kế các phép biến đổi T1–T4, quy tắc data quality và invariants.
  3. **Nguyễn Hoàng Dương** — Thiết lập môi trường Spark/Hadoop, tối ưu entry point, quản lý snapshot raw và rerun verification.
  4. **Thân Văn Dũng** — Thiết kế bảng Dimension, xử lý phép Join (Many-to-One), xây dựng công thức KPI và đối soát số liệu.

---

## 1. Cấu trúc Repository

```text
BIGDATA_NHOM_3/
├── README.md                           # Tài liệu tổng thể hướng dẫn đồ án (CP2 & CP3)
├── requirements.txt                    # Thư viện phụ thuộc (PySpark)
├── .gitignore                          # Cấu hình loại trừ raw lớn, venv, output
│
├── config/
│   ├── config.json                     # Cấu hình pipeline CP3 (path, SHA-256, số đối soát)
│   └── dim_transaction_type.csv        # Dimension danh mục loại giao dịch (5 dòng)
│
├── data/
│   └── raw/
│       ├── .gitkeep
│       ├── paysim_data.csv             # Snapshot raw PaySim chuẩn (493.5MB, SHA-256 xác thực)
│       ├── checkpoint3_input.json      # Hồ sơ di chuyển và xác thực snapshot cho CP3
│       └── paysim/                     # Lưu trữ raw theo phân vùng ngày ingest từ CP2
│           └── ingest_date=2026-10-23/
│               └── run_20261023T180622Z_c45ac87c/
│                   ├── paysim_data.csv
│                   └── manifest.json
│
├── evidence/
│   └── checkpoint3/
│       └── 20261110T095859Z_ec6b3d33/  # Bằng chứng run SUCCESS (metrics, explain, samples)
│           ├── _SUCCESS.json           # Dấu mốc chạy thành công
│           ├── config_used.json        # Snapshot cấu hình đã chạy
│           ├── explain_join.txt        # Physical plan của phép join
│           ├── explain_summary.txt     # Physical plan của toàn bộ pipeline aggregation
│           ├── metrics.json            # Toàn bộ thông số đo runtime, quality gates, counts
│           ├── quality_progress.json   # Tiến độ kiểm tra chất lượng từng chặng
│           ├── run.log                 # Log chi tiết quá trình chạy Spark
│           ├── sample_output.json      # Top 50 dòng kết quả tổng hợp KPI
│           ├── sample_transformed.json # Mẫu 5 dòng sau khi hoàn tất T1–T4
│           ├── schema_input.txt        # Schema đọc từ raw CSV
│           ├── schema_output.txt       # Schema kết quả tổng hợp Parquet
│           └── unmatched_sample.json   # Mẫu các dòng unmatched (rỗng = 0 lỗi)
│
├── metadata/
│   ├── source.json                     # Thông tin data card, nguồn gốc Kaggle PaySim
│   ├── manifest.json                   # Manifest chứng nhận Ingestion CP2
│   ├── spark_read_20261022T122337Z_ec39637e.json # Log Spark đọc raw CP2
│   └── spark_read_20261023T181354Z_f09cca3e.json # Log Spark đọc raw CP2
│
├── reports/
│   ├── checkpoint3_template.md         # Template báo cáo Checkpoint 3 theo yêu cầu
│   └── checkpoint3.md                  # Báo cáo kết quả chính thức Checkpoint 3 (Run PASS)
│
├── src/
│   ├── env_check.py                    # [CP2] Kiểm tra môi trường Python/Java/Spark
│   ├── ingest_paysim.py                # [CP2] Script nạp dữ liệu PaySim và sinh manifest
│   ├── inspect_raw.py                  # [CP2] Script kiểm tra cấu trúc raw data bằng Spark
│   ├── verify_manifest.py              # [CP2] Xác thực tính toàn vẹn manifest và mã SHA-256
│   ├── prepare_raw.py                  # [CP3] Chuẩn bị snapshot độc lập từ archive ZIP
│   ├── quality_report.py               # [CP3] Thư viện kiểm tra quality gates và profile dữ liệu
│   ├── pipeline.py                     # [CP3] Entry point Spark Batch Processing Pipeline
│   └── compare_runs.py                 # [CP3] So sánh độc lập 2 lần chạy (Rerun Verification)
│
└── notebooks/
    ├── 01_Checkpoint_Buoi6_Kaggle_v2.ipynb    # Notebook ingestion & environment trên Kaggle
    └── 02_Train_Baseline_PaySim_Kaggle_v2.ipynb # Notebook baseline machine learning tham chiếu
```

---

## 2. Nguồn dữ liệu & Snapshot Contract

- **Bộ dữ liệu:** Synthetic Financial Datasets For Fraud Detection (PaySim).
- **Nguồn gốc:** [Kaggle ealaxi/paysim1](https://www.kaggle.com/datasets/ealaxi/paysim1).
- **Ý nghĩa nghiệp vụ:** Dữ liệu mô phỏng các giao dịch chuyển tiền/thanh toán trên thiết bị di động, được tạo bởi trình mô phỏng PaySim dựa trên dữ liệu tài chính thực tế tại châu Phi.
- **Giấy phép:** CC BY-SA 4.0.
- **Snapshot duy nhất được nhóm chốt và kiểm chứng:**
  - Định dạng: 1 tệp CSV UTF-8 có tiêu đề.
  - Kích thước: **493.534.783 bytes** (~470.67 MB).
  - Số dòng: **6.362.620 dòng**, 11 cột thuộc tính.
  - Mã băm SHA-256: `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b`.
  - Grain dữ liệu: Mỗi dòng là một giao dịch mô phỏng tài chính đơn lẻ. Không có thuộc tính `transaction_id` nghiệp vụ gốc duy nhất.

---

## 3. Checkpoint 2 — Environment, Ingestion & Raw Storage

Checkpoint 2 hoàn thành nền tảng môi trường và lưu trữ dữ liệu thô bất biến:

### 3.1. Các công việc đã đạt được
1. **Kiểm tra môi trường (Environment Validation):**
   - Xác nhận sự tương thích của Python 3.12, OpenJDK 17 và PySpark.
   - Lệnh thực thi:
     ```bash
     python src/env_check.py
     ```
   - Kết quả: `ENV PASS` (Minh chứng: `logs/env_check.txt`).

2. **Nạp dữ liệu gốc (Data Ingestion):**
   - Nạp tệp PaySim gốc từ nguồn local hoặc remote, đảm bảo nguyên vẹn từng byte (byte-identical), không thực hiện làm sạch hay biến đổi trong tầng raw.
   - Phân vùng lưu trữ theo định dạng: `data/raw/paysim/ingest_date=YYYY-MM-DD/run_<UTC_TIMESTAMP>_<id>/`.
   - Sinh `manifest.json` ghi nhận metadata: dung lượng, ngày giờ UTC, mã SHA-256, đường dẫn đích và trạng thái `SUCCESS`.
   - Lệnh thực thi:
     ```bash
     python src/ingest_paysim.py --source "data/raw/paysim_data.csv" --dataset paysim --raw-root data/raw
     ```

3. **Xác thực Manifest & Kiểm tra Raw bằng Spark:**
   - Script xác thực manifest hỗ trợ truyền cờ `--manifest`:
     ```bash
     python src/verify_manifest.py --manifest metadata/manifest.json
     ```
     -> Kết quả: `XÁC THỰC MANIFEST: PASS` (khớp dung lượng 493.534.783 bytes và mã SHA-256).
   - Kiểm tra Spark đọc raw:
     ```bash
     python src/inspect_raw.py --manifest metadata/manifest.json
     ```
     -> Kết quả: Đọc đủ 6.362.620 dòng trên 12 partitions ban đầu, ghi nhận tại `metadata/spark_read_*.json`.

---

## 4. Checkpoint 3 — Spark Batch Processing Pipeline

Checkpoint 3 xây dựng và kiểm chứng toàn diện một pipeline xử lý dữ liệu theo lô (Batch Processing Pipeline) trên nền tảng Apache Spark.

### 4.1. Câu hỏi nghiệp vụ giải quyết
> **"Mỗi ngày mô phỏng và mỗi loại giao dịch có bao nhiêu giao dịch, tổng giá trị bao nhiêu, bao nhiêu giao dịch mang nhãn gian lận và tỷ lệ gian lận là bao nhiêu?"**

Output phục vụ báo cáo trực quan phân bố gian lận và làm đầu vào chuẩn cho các mô hình phát hiện bất thường ở các giai đoạn sau.

### 4.2. Kiến trúc luồng xử lý (Data Flow)

```text
1 Snapshot PaySim CSV (493.5MB, 6.362.620 dòng)
    │
    ▼
[Spark Read với Explicit Schema + FAILFAST]
    │── Chặn đứng dữ liệu lỗi định dạng ngay từ tầng đọc
    ▼
[T1: Normalize Strings]
    │── upper(trim(type)), trim(nameOrig), trim(nameDest)
    ▼
[T2: Validate & Quality Gates]
    │── Kiểm null 11 cột, step >= 1, amount >= 0, isFraud IN (0,1), isFlaggedFraud IN (0,1)
    │── Tách invalid vào rejected/ (Đạt: valid = 6.362.620, invalid = 0)
    │── GIỮ NGUYÊN 16 giao dịch amount = 0 vì cả 16 giao dịch này đều là gian lận (isFraud = 1)
    ▼
[T3: Derive Business Features]
    │── day_index = floor((step - 1) / 24) + 1  (Ngày mô phỏng 1..31)
    │── hour_index = (step - 1) % 24            (Giờ trong ngày mô phỏng 0..23)
    │── log_amount = log1p(amount)              (Biến đổi log chuẩn hóa khoảng giá trị)
    ▼
[T4: Explicit Projection]
    │── Chiếu rõ ràng 11 cột gốc + 3 cột phái sinh, giữ nguyên grain 1 dòng = 1 giao dịch
    ▼
[Left Join Dimension]
    │── Join config/dim_transaction_type.csv trên khóa type (Many-to-One)
    │── Kiểm tra Dimension unique (5 types), Broadcast Hash Join loại bỏ shuffle Fact
    │── Đo lường: unmatched = 0, fact rows trước và sau join giữ nguyên 6.362.620 dòng
    ▼
[Aggregation & KPI Calculation]
    │── GroupBy(day_index, type, type_name_vi, flow_group)
    │── Tính: txn_count, total_amount, avg_amount, fraud_count, fraud_amount, fraud_rate_pct
    ▼
[Ghi Output Parquet + Preview CSV]
    │── Ghi Parquet vào thư mục run versioned độc lập với chế độ errorifexists
    │── Ghi CSV preview (Top 50 nhóm có nhiều gian lận nhất)
    ▼
[Readback Verification & Baseline Recording]
    │── Đọc lại Parquet từ đĩa, kiểm tra đối soát totals (tổng dòng, tổng tiền, gian lận, số nhóm)
    │── Ghi nhận _SUCCESS.json, metrics.json, explain physical plan và thời gian thực thi
```

### 4.3. Bảng danh mục loại giao dịch (Dimension Table)

Tệp quản lý nội bộ [`config/dim_transaction_type.csv`](file:///config/dim_transaction_type.csv) bổ sung ngữ nghĩa tiếng Việt và nhóm luồng tài chính:

| type | type_name_vi | flow_group |
|---|---|---|
| CASH_IN | Nạp tiền | CASH_SERVICE |
| CASH_OUT | Rút tiền | CASH_SERVICE |
| DEBIT | Ghi nợ | ACCOUNT_DEBIT |
| PAYMENT | Thanh toán | MERCHANT_PAYMENT |
| TRANSFER | Chuyển tiền | ACCOUNT_TRANSFER |

- **Tính toàn vẹn:** Khóa `type` duy nhất tuyệt đối (0 duplicate keys), không rỗng/null, bao phủ 100% các loại giao dịch trong dữ liệu thực tế.

---

## 5. Hướng dẫn thực thi & Tái tạo (How-to-Run)

### 5.1. Thiết lập môi trường chạy trên Windows

Môi trường yêu cầu:
- Python 3.12 (đã có sẵn `sys.executable`).
- OpenJDK 17 (Temurin).
- Hadoop winutils: Đặt tại `D:\hadoop` (chứa `bin/winutils.exe` và `bin/hadoop.dll`).

Cài đặt thư viện:
```powershell
python -m pip install -r requirements.txt
```

### 5.2. Chạy Pipeline bằng một lệnh duy nhất

```powershell
python src/pipeline.py --config config/config.json
```

Pipeline sẽ tự động:
1. Xác thực SHA-256 của snapshot raw.
2. Khởi động Spark và smoke test cả JVM lẫn Python worker.
3. Đọc dữ liệu, đo lường toàn bộ chất lượng (Quality Profile).
4. Thực thi biến đổi T1–T4, join danh mục và tổng hợp KPI.
5. Ghi kết quả Parquet, đọc lại từ đĩa để đối soát tính toàn vẹn.
6. Cập nhật con trỏ run thành công tại `output/latest_success.json`.

Log màn hình khi hoàn thành thành công:
```text
[INPUT] rows=6362620 bytes=493534783
[CLEAN] valid=6362620 rejected=0
[JOIN] before=6362620 after=6362620 unmatched=0
[OUTPUT] groups=152 path=D:\BIGDATA_NHOM_3\output\snapshot_16910f90577b\<run_id>\daily_type_summary
[BASELINE] elapsed_sec=...
SUCCESS: D:\BIGDATA_NHOM_3\output\snapshot_16910f90577b\<run_id>
```

### 5.3. Kiểm chứng chạy lại độc lập (Rerun Verification)

Để chứng minh tính xác định (deterministic) và khả năng tái tạo kết quả:
```powershell
# Chạy lần 1 và lưu đường dẫn
$cp3First = (Get-Content "output/latest_success.json" -Raw | ConvertFrom-Json).run_dir

# Chạy lần 2
python src/pipeline.py --config config/config.json
$cp3Second = (Get-Content "output/latest_success.json" -Raw | ConvertFrom-Json).run_dir

# So sánh 2 chiều bằng exceptAll trên Spark
python src/compare_runs.py $cp3First $cp3Second
```

Kỳ vọng: **`RERUN PASS: identical schema and business rows in both directions`**.

---

## 6. Kết quả kiểm chứng thực tế (Run PASS Official Evidence)

Dữ liệu được trích xuất từ lượt chạy chính thức `20261110T095859Z_ec6b3d33` được lưu trong [`evidence/checkpoint3/`](file:///evidence/checkpoint3/20261110T095859Z_ec6b3d33/):

### 6.1. Bảng đối soát số liệu (Invariants Reconciliation)

| Chỉ số kiểm tra | Kỳ vọng đối soát độc lập | Kết quả Spark đo được | Kết luận |
|---|---:|---:|:---:|
| **Tổng số giao dịch (`txn_count`)** | 6.362.620 | **6.362.620** | **PASS** |
| **Tổng số nhãn gian lận (`fraud_count`)** | 8.213 | **8.213** | **PASS** |
| **Tỷ lệ gian lận toàn bộ** | ~0,129082% | **0,129082%** | **PASS** |
| **Tổng giá trị tiền (`total_amount`)** | 1.144.392.944.759,77 | **1.144.392.944.759,77** | **PASS** |
| **Số nhóm `(day_index, type)` quan sát** | 152 | **152** | **PASS** |
| **Tính duy nhất của khóa nhóm output** | Unique (1 dòng/nhóm) | **Unique (100%)** | **PASS** |
| **Số dòng null (trên cả 11 cột gốc)** | 0 | **0** | **PASS** |
| **Số dòng duplicate toàn bộ 11 cột** | 0 | **0** | **PASS** |
| **Số giao dịch 0 đồng (`amount = 0`)** | 16 | **16 (được bảo toàn, cả 16 đều isFraud=1)** | **PASS** |
| **Số dòng bị loại bỏ (`rejected`)** | 0 | **0** | **PASS** |
| **Unmatched rows khi join Dimension** | 0 | **0** | **PASS** |
| **Fact rows trước và sau Join** | 6.362.620 / 6.362.620 | **6.362.620 / 6.362.620** | **PASS** |

### 6.2. Baseline Thời gian & Môi trường thực thi

- **Môi trường chạy:**
  - OS: Windows 11 Home Single Language (64-bit, 8 logical CPUs, 16 GB RAM).
  - Python: 3.12.10 | Java: OpenJDK 17.0.20.1 (Temurin) | Spark: 3.5.9 (Master: `local[*]`).
  - Hadoop: `D:\hadoop` (winutils 3.3.5, hadoop.dll).
  - Partitions: Input 8 partitions, Output 1 partition.
- **Thời gian thực thi (Baseline Runtime):**
  - **Run 1:** `20261110T095859Z_ec6b3d33` — **191,432 giây** (~3,19 phút).
  - **Run 2:** `20261110T100253Z_30d5dc85` — **227,951 giây** (~3,80 phút).
  - *Phạm vi đo:* Toàn bộ pipeline đầu-cuối (kiểm tra băm, khởi động Spark, đọc schema FAILFAST, profile chất lượng dữ liệu, T1–T4, join, aggregate, ghi Parquet, đọc lại xác thực và xuất preview CSV).
- **Phân tích Physical Plan (`explain_summary.txt`):**
  - **Join:** `BroadcastHashJoin LeftOuter BuildRight` kết hợp `BroadcastExchange`. Dimension table (5 dòng) được broadcast trực tiếp sang các worker, loại bỏ hoàn toàn chi phí shuffle cho bảng Fact 6,36 triệu dòng.
  - **Shuffle Exchange:** Duy nhất 1 node `Exchange hashpartitioning(day_index, type, type_name_vi, flow_group, 200)` tại bước `HashAggregate` để gom nhóm tính KPI.
  - **AQE:** Kích hoạt cơ chế `AdaptiveSparkPlan` tự động tối ưu hóa việc phân chia partition trong runtime.

---

## 7. Tài liệu và Báo cáo Checkpoint 3

1. **Báo cáo Markdown đầy đủ:** [`reports/checkpoint3.md`](file:///reports/checkpoint3.md) — Điền đầy đủ các mục A–G, Exit Ticket và bảng phân công.
2. **Minh chứng nhẹ trong Git:** [`evidence/checkpoint3/20261110T095859Z_ec6b3d33/`](file:///evidence/checkpoint3/20261110T095859Z_ec6b3d33/).
3. **Template Slide 6 trang:** Cập nhật các số liệu chính thức từ Run PASS vào bộ slide thuyết trình của nhóm.
