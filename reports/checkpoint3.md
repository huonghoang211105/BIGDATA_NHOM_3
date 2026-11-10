# Checkpoint 3 — Nhóm 3: Spark Processing Pipeline trên PaySim

## A. Nhóm và trạng thái

- **Thành viên:** Hoàng Thị Diệu Hương, Nguyễn Thị Hằng My, Nguyễn Hoàng Dương, Thân Văn Dũng.
- **Repo / Commit:** `https://github.com/huonghoang211105/BIGDATA_NHOM_3` — commit: `43220c1c34dfe66ad1cb46d983b22b53704601c9`.
- **Ngày demo / máy chạy:** 10/11/2026, Windows 11 (AMD64, 8 logical CPUs, JDK 17 Temurin, PySpark 3.5.9).
- **Run ID / Path:** `20261110T095859Z_ec6b3d33` tại `output/snapshot_16910f90577b/20261110T095859Z_ec6b3d33`.
- **Trạng thái:** **PASS** (100% test và quality gates đạt, đã kiểm chứng chạy lại 2 lần độc lập đạt `RERUN PASS`).
- **Minh chứng:** `evidence/checkpoint3/20261110T095859Z_ec6b3d33/`.

---

## B. Input contract

- **Dataset:** PaySim (Synthetic Financial Datasets For Fraud Detection), nguồn Kaggle `ealaxi/paysim1`.
- **Grain:** Một dòng là một giao dịch mô phỏng tài chính; không có business transaction ID gốc duy nhất.
- **Input path:** `data/raw/paysim_data.csv` (ghi nhận di chuyển từ snapshot CP2 `run_20261023T180622Z_c45ac87c`).
- **Thông số kỹ thuật:**
  - Files: 1 file CSV.
  - Bytes: 493.534.783 bytes.
  - SHA-256: `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b`.
  - Số dòng raw: 6.362.620 dòng.
- **Schema input:** [schema_input.txt](file:///evidence/checkpoint3/20261110T095859Z_ec6b3d33/schema_input.txt) (11 cột: `step` INT, `type` STRING, `amount` DECIMAL(20,2), `nameOrig` STRING, `oldbalanceOrg` DECIMAL(20,2), `newbalanceOrig` DECIMAL(20,2), `nameDest` STRING, `oldbalanceDest` DECIMAL(20,2), `newbalanceDest` DECIMAL(20,2), `isFraud` INT, `isFlaggedFraud` INT).
- **Bảng danh mục loại giao dịch (Dimension):** `config/dim_transaction_type.csv` (5 dòng, 3 cột: `type`, `type_name_vi`, `flow_group`), SHA-256: `e30610723992fe6fedc51a6b5886e68f7c9f46e0c3b30598828f5d0556680534`.

---

## C. Transformations & Business Reasons

Pipeline thực hiện 4 bước biến đổi rõ ràng:
1. **T1 (Normalize):** `upper(trim(type))`, `trim(nameOrig)`, `trim(nameDest)` nhằm chuẩn hóa chuỗi, ngăn ngừa lỗi mismatch khi thực hiện phép Join. Số dòng giữ nguyên: 6.362.620.
2. **T2 (Validate & Filter):** Kiểm tra toàn bộ 11 cột null, `step >= 1`, `amount >= 0` (giữ lại 16 giao dịch 0 đồng vì cả 16 đều mang nhãn gian lận `isFraud = 1`), `isFraud IN (0,1)`, `isFlaggedFraud IN (0,1)`, tài khoản và type không rỗng. Tách các dòng invalid sang nhánh rejected.
   - Kết quả: `valid = 6.362.620`, `invalid = 0`.
3. **T3 (Derive features):**
   - `day_index = floor((step - 1) / 24) + 1` (ngày mô phỏng tương đối, 1..31).
   - `hour_index = (step - 1) % 24` (giờ trong ngày mô phỏng, 0..23).
   - `log_amount = log1p(amount)` (biến đổi log phục vụ phân tích độ lệch giá trị).
4. **T4 (Projection):** Chiếu rõ ràng 11 cột gốc và 3 cột suy diễn, giữ đúng grain giao dịch đơn lẻ.

**Số lượng dòng qua từng bước:**
- Raw: 6.362.620
- Normalized: 6.362.620
- Valid: 6.362.620 (Invalid: 0)
- Derived: 6.362.620

---

## D. Join Validation

- **Loại join:** `LEFT JOIN` giữa Fact (`enriched`) và Dimension (`dim_transaction_type`) trên khóa `type`.
- **Grain & Quan hệ:** Many-to-One (nhiều giao dịch thuộc về một loại giao dịch).
- **Dimension check:** 5 dòng, 0 duplicate keys, không null/empty key.
- **Fact before join:** 6.362.620 dòng.
- **Unmatched fact rows:** 0 dòng (`left_anti` cho ra 0 dòng, `unmatched_rate = 0.0`).
- **Fact after join:** 6.362.620 dòng (grain hoàn toàn được bảo toàn, không bị mất dòng hay bùng nổ dữ liệu).
- **Policy:** Nếu xuất hiện unmatched rows hoặc dimension bị duplicate, pipeline lập tức fail fast, lưu `unmatched_sample.json` và chặn xuất bản output.

---

## E. Aggregation và Output Contract

- **Câu hỏi nghiệp vụ:** *Mỗi ngày mô phỏng và mỗi loại giao dịch có bao nhiêu giao dịch, tổng giá trị bao nhiêu, bao nhiêu giao dịch mang nhãn gian lận và tỷ lệ bao nhiêu?*
- **Grain output:** Một dòng cho mỗi cặp `(day_index, type)` có dữ liệu thực tế.
- **Công thức KPI:**
  - `txn_count = count(*)`
  - `total_amount = sum(amount)`
  - `avg_amount = round(avg(amount), 2)`
  - `fraud_count = sum(isFraud)`
  - `fraud_amount = sum(amount khi isFraud=1)`
  - `fraud_rate_pct = round(100.0 * fraud_count / txn_count, 6)`
- **Format & Đường dẫn:** Parquet tại `output/snapshot_16910f90577b/<run_id>/daily_type_summary` + CSV preview tại `preview_csv/`.
- **Đối soát độc lập với số liệu CSV:**
  - Tổng số nhóm `(day_index, type)`: **152** (kỳ vọng: 152).
  - Khóa `(day_index, type)` trong output là **duy nhất** (count = 1 với mọi nhóm).
  - Tổng số giao dịch (`txn_count`): **6.362.620** (khớp 100%).
  - Tổng số nhãn gian lận (`fraud_count`): **8.213** (khớp 100%).
  - Tổng giá trị giao dịch (`total_amount`): **1.144.392.944.759,77** (khớp 100% đến từng cent).

---

## F. Quality, Baseline và Khả năng tái tạo (Reproducibility)

- **Chất lượng dữ liệu:**
  - 0 null trên cả 11 cột gốc.
  - 0 dòng duplicate trên 11 cột gốc.
  - Step range: min = 1, max = 743, distinct = 743.
  - Amount range: min = 0.00, max = 92.445.516,64.
  - Giao dịch 0 đồng: 16 dòng (được giữ nguyên theo thiết kế vì toàn bộ 16 dòng đều là gian lận).
- **Baseline xử lý Spark:**
  - **Lần chạy 1 (Run 1):** `20261110T095859Z_ec6b3d33` — Thời gian: **191,432 giây** (~3,19 phút).
  - **Lần chạy 2 (Run 2):** `20261110T100253Z_30d5dc85` — Thời gian: **227,951 giây** (~3,80 phút).
  - Phạm vi đo thời gian: Đo bằng `time.perf_counter()`, bao gồm kiểm hash, khởi động Spark, đọc schema FAILFAST, profile chất lượng, T1-T4, join, aggregation, ghi Parquet, đọc lại xác thực (readback verification) và xuất preview CSV.
- **Môi trường:**
  - Python: 3.12.10
  - Spark: 3.5.9 (Master: `local[*]`)
  - Java: OpenJDK 17.0.20.1 Temurin
  - OS: Windows 11 Home Single Language (64-bit, 8 logical CPUs, 16 GB RAM)
  - Hadoop Home: `D:\hadoop` (winutils 3.3.5)
  - Input Partitions: 8 partitions; Output Partitions: 1 partition.
- **Phân tích Execution Plan (`explain_summary.txt`):**
  - **Join Node:** `BroadcastHashJoin LeftOuter BuildRight` kết hợp với `BroadcastExchange` cho bảng dimension 5 dòng. Nhờ kích thước nhỏ, Spark tự động broadcast dimension sang các worker, loại bỏ hoàn toàn shuffle cho bảng Fact 6,36 triệu dòng.
  - **Shuffle Node:** Xuất hiện duy nhất 1 lần `Exchange hashpartitioning(day_index, type, type_name_vi, flow_group, 200)` tại bước `HashAggregate` phục vụ tính toán KPI theo nhóm `(day_index, type)`.
  - **AQE:** `AdaptiveSparkPlan` kích hoạt tối ưu runtime.
- **Kiểm chứng chạy lại (`compare_runs.py`):**
  - Kết quả so sánh 2 chiều qua `exceptAll`: **`RERUN PASS: identical schema and business rows in both directions`**.
  - Output đảm bảo tính xác định (deterministic) 100%.

---

## G. Blocker và Next Action

| Việc cần làm tiếp theo | Người phụ trách | Hạn chốt | Điều kiện hoàn thành |
|---|---|---|---|
| Khảo sát tối ưu Shuffle Partitions (Buổi 9) | Nguyễn Hoàng Dương | Buổi 9 | Đo lường số partition shuffle tối ưu thay vì mặc định 200 |
| Benchmark Cache/Persist và Skew Join (Buổi 9-10) | Thân Văn Dũng | Buổi 10 | Đo baseline trước/sau tối ưu hóa tài nguyên |
| Báo cáo Slide trình bày Checkpoint 3 (6 slide) | Hoàng Thị Diệu Hương | Buổi 8 | Hoàn thành slide theo template và số liệu run PASS |
| Chuẩn bị phần hỏi đáp kiến trúc & data quality | Nguyễn Thị Hằng My | Buổi 8 | Cả 4 thành viên giải thích trôi chảy 10 câu hỏi Checkpoint 3 |

---

## Exit Ticket

1. **End-to-end PASS bằng chứng nào?**
   - Run ID `20261110T095859Z_ec6b3d33` hoàn thành với exit code 0, có `_SUCCESS.json`, `metrics.json` ghi nhận `status: SUCCESS`, log xác nhận đủ 6.362.620 dòng và đối soát KPI khớp 100%.
2. **Join grain/key và nguy cơ shuffle/data explosion?**
   - Join key là `type`, quan hệ Fact (many) sang Dim (one, unique 5 rows). Vì dimension đã kiểm tra unique tuyệt đối nên không gây nhân đôi dòng. Phép join sử dụng Broadcast Hash Join nên không phát sinh shuffle trên Fact.
3. **Quality issue lớn nhất thực tế?**
   - Không có null/duplicate toàn dòng. Issue nghiệp vụ quan trọng nhất là 16 giao dịch có `amount = 0` đều mang nhãn `isFraud = 1`. Nhóm giữ nguyên điều kiện `amount >= 0` để không vô tình loại bỏ 16 gian lận thực tế này.
4. **Baseline/commit dùng so với Buổi 10?**
   - Baseline runtime toàn pipeline: **191,432s** với PySpark 3.5.9 local[8], 8 input partitions, AQE enabled, trên commit hash của Checkpoint 3.
