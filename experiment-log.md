# Experiment log — Checkpoint 4: Benchmark & tối ưu pipeline (Nhóm 3)

> Trạng thái: **NHÁP — hết Phase 1** (baseline + bottleneck evidence + hypothesis). Mục 7–10 điền ở Phase 2.
> Mọi con số dưới đây lấy từ `benchmark.csv` và `evidence/checkpoint4/baseline/<run_id>/`. Không có số ước lượng.

## 1. Workload, môi trường, control

| Mục | Giá trị |
|---|---|
| Input | PaySim CSV, 1 file, 493.534.783 bytes (471,4 MiB), 6.362.620 dòng, SHA-256 `16910f90…4eea6b`, đọc qua `metadata/manifest.json` |
| Pipeline | `src/pipeline.py`: đọc schema 11 cột FAILFAST → profile null + đếm trùng 11 cột → T1–T4 → kiểm dim + left_anti → broadcast left join → groupBy(day_index, type, …) → Parquet 152 dòng → đọc lại đối soát |
| Máy | Windows 11, 8 logical CPU, RAM 7,8 GiB (8.344.559.616 bytes) |
| Spark | PySpark 3.5.9, `local[*]` (default parallelism 8), Java 17.0.20.1, Python 3.12.10 |
| Driver memory | không đặt → mặc định `1g`; JVM max heap đo được 1.073.741.824 bytes |
| SQL conf (đọc lúc chạy) | `adaptive.enabled=true`, `shuffle.partitions=200`, `autoBroadcastJoinThreshold=10485760b`, `adaptive.coalescePartitions.enabled=true` |
| Conf khác mặc định | `spark.eventLog.enabled=true`, `spark.eventLog.dir=logs/spark-events`, `spark.ui.showConsoleProgress=false` (giống nhau cho baseline và after) |
| Baseline code | tag **`checkpoint-4-baseline`** = commit **`ae84592`** (tooling CP4; logic xử lý giống CP3 `eee6567`) |
| Cách chạy | `python src/benchmark.py --label baseline --runs 5 --warmup 1` — chạy từ git worktree `.bench/ae845921d9ca` check-out đúng tag, CSV là hard-link của cùng file vật lý |

**Control (giữ cố định cho cả hai phía):** cùng file input (cùng SHA-256, cùng file vật lý), cùng dimension, cùng
`config/config.json`, cùng `local[*]`, driver memory 1g, cùng Spark/Java/Python, cùng mọi Spark conf ở trên, cùng
mã đo (`benchmark.py`, `stage_metrics.py`), cùng thứ tự các bước kiểm chất lượng; 1 run warm-up bị loại cho mỗi phía;
Phase 2 chạy xen kẽ B, A, B, A… bằng `--label baseline optimized`.

## 2. Bản đồ Spark job (từ event log, giống nhau ở cả 5 run)

Mỗi run sinh **46 job / 46 stage được thực thi**, trong đó **12 stage quét file CSV fact** (tổng input
5.185,4 MiB = 11,0 lần kích thước file; lần quét của `unmatched.limit(20).collect()` bị chia thành nhiều job nhỏ).

| Phase | Job | Stage quét CSV | Input (MiB) | Shuffle write (MiB) | Spill mem / disk (MiB) | Job wall median (s) |
|---|---:|---:|---:|---:|---:|---:|
| startup (smoke test) | 3 | 0 | 0 | 0 | 0 / 0 | 2,746 |
| profile_null | 3 | 1 | 471,4 | 0,075 | 0 / 0 | 10,947 |
| **profile_dup** | 3 | 1 | 471,4 | **413,735** | **3.512,753 / 815,206** | **42,530** |
| clean (invalid/valid/normalized/enriched count + sample) | 9 | 5 | 2.357,0 | 0 | 0 / 0 | 41,118 |
| join (dim checks, left_anti count + limit, joined count) | 17 | 4 | 1.414,2 | 0 | 0 / 0 | 35,546 |
| output (aggregate + write + readback) | 11 | 1 | 471,4 | 0,039 | 0 / 0 | 13,801 |

Nguồn: `evidence/checkpoint4/baseline/<run_id>/phase_metrics.csv`, `job_metrics.csv`.

## 3. Baseline — từng run

Warm-up (run_no 0) được ghi lại nhưng **không** tính vào median. Thời gian tính bằng giây.

| run_no | run_id | trạng thái | runtime | hash | spark_startup | read_schema | profile_null | profile_dup | clean | join | output | max task | RAM trống lúc bắt đầu (MiB) | checks |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 0 | 20261117T151451Z_aca3e358 | warm-up (bỏ) | 171,367 | 0,588 | 23,285 | 0,406 | 10,180 | 35,420 | 47,035 | 38,493 | 15,449 | 26,180 | 1.122 | PASS |
| 1 | 20261117T151749Z_a28eaadb | warm | 158,230 | 0,599 | 11,049 | 0,356 | 10,430 | 42,195 | 40,241 | 38,312 | 14,623 | 32,558 | 1.704 | PASS |
| 2 | 20261117T152034Z_e003171c | warm | 177,613 | 0,579 | 11,360 | 0,401 | 13,845 | 43,743 | 47,972 | 44,488 | 14,797 | 32,281 | 2.130 | PASS |
| 3 | 20261117T152338Z_baf24beb | warm | 156,453 | 0,657 | 11,028 | 0,407 | 12,602 | 42,960 | 40,941 | 33,071 | 14,201 | 32,878 | 1.985 | PASS |
| 4 | 20261117T152622Z_864399ec | warm | 160,142 | 0,631 | 11,218 | 0,409 | 11,170 | 42,970 | 42,009 | 36,298 | 15,032 | 32,931 | 2.311 | PASS |
| 5 | 20261117T152909Z_fc83fe80 | warm | 163,214 | 0,563 | 10,942 | 0,379 | 11,868 | 43,038 | 44,045 | 36,621 | 15,383 | 32,944 | 2.094 | PASS |

### Tổng hợp 5 run đo (n = 5)

| Metric | Median | Min | Max | Max − Min | Stdev | % runtime (median) |
|---|---:|---:|---:|---:|---:|---:|
| **runtime_sec** | **160,142** | 156,453 | 177,613 | **21,160** | **8,475** | 100 % |
| sec_hash (SHA-256 input + dim) | 0,599 | 0,563 | 0,657 | 0,094 | 0,038 | 0,4 % |
| sec_spark_startup | 11,049 | 10,942 | 11,360 | 0,418 | 0,168 | 6,9 % |
| sec_read_schema | 0,401 | 0,356 | 0,409 | 0,053 | 0,023 | 0,3 % |
| sec_profile_null | 11,868 | 10,430 | 13,845 | 3,415 | 1,317 | 7,4 % |
| **sec_profile_dup** | **42,970** | 42,195 | 43,743 | 1,548 | **0,548** | **26,8 %** |
| sec_clean | 42,009 | 40,241 | 47,972 | 7,731 | 3,108 | 26,2 % |
| sec_join | 36,621 | 33,071 | 44,488 | 11,417 | 4,213 | 22,9 % |
| sec_output | 14,797 | 14,201 | 15,383 | 1,182 | 0,443 | 9,2 % |
| max_task_sec | 32,878 | 32,281 | 32,944 | 0,663 | 0,291 | — |
| shuffle_write_mb (cả run) | 413,849 | 413,849 | 413,849 | 0 | 0 | — |
| spill disk / memory (MiB) | 815,206 / 3.512,753 | — | — | 0 | 0 | — |

Business metrics ở cả 6 run (kể cả warm-up), `checks = PASS`: 6.362.620 dòng vào = valid = sau join; unmatched 0;
null 0; trùng 0; invalid 0; amount = 0 có 16 dòng; fraud 8.213; tổng tiền 1.144.392.944.759,77; 152 nhóm.

**Warm-up:** run warm-up có `spark_startup` = 23,285 s, so với 10,9–11,4 s ở các run sau (JVM/thư viện chưa nằm
trong OS cache). Vì vậy loại warm-up là cần thiết, và phía after cũng phải bỏ 1 run warm-up như vậy.
**Variance:** runtime chênh 21,160 s giữa run nhanh nhất và chậm nhất (13,2 % median); run 2 (177,6 s) chậm chủ yếu
ở clean và join, là các chặng chỉ quét CSV. Riêng `profile_dup` rất ổn định (stdev 0,548 s).

## 4. Tách mốc LINEAGE→INPUT (71,9 s ở CP3)

Ở run median (run 4), phần tương ứng `[LINEAGE]→[INPUT]` = hash 0,631 + startup 11,218 + read 0,409
+ profile_null 11,170 + profile_dup 42,970 = **66,398 s**, trong đó:

- **profile_dup: 42,970 s (64,7 %)**: phần lớn nhất.
- spark_startup: 11,218 s (16,9 %): chi phí cố định khi khởi động JVM/SparkSession (gồm 2 smoke test, 2,7 s job).
- profile_null: 11,170 s (16,8 %), tương đương một lần quét CSV.
- hash SHA-256: 0,631 s (1,0 %): **không phải bottleneck** (file đã nằm trong OS cache).

## 5. Top stage tốn thời gian (median duration qua 5 run)

| # | Stage | Phase / action | Duration median (s) | Task | Task median / max (s) | Input | Shuffle write | Spill mem / disk (MiB) | Peak exec mem/task |
|---|---:|---|---:|---:|---|---|---|---|---|
| 1 | **10** | profile_dup: `HashAggregate(partial, key 11 cột)` + Exchange (map) | **32,881** (32,296–32,944) | 8 | 32,065 / 32,878 | 471,4 MiB, 6.362.620 rec | **413,735 MiB, 6.362.620 rec** | **1.768,3 / 406,2** | 52,2 MiB |
| 2 | 54 | output: quét CSV + broadcast join + partial agg → write parquet | 12,086 (11,594–12,390) | 8 | 11,843 / 12,086 | 471,4 MiB | 0,031 MiB, 187 rec | 0 / 0 | 4,3 MiB |
| 3 | 28 | clean: `enriched.orderBy(...).limit(5).collect()` | 11,274 (9,976–12,723) | 8 | 10,991 / 11,258 | 471,4 MiB | 0 | 0 / 0 | 0 |
| (tham chiếu) | 12 | profile_dup: final aggregate (reduce) | 9,568 | 9 | 9,120 / 9,491 | — (shuffle read 413,735 MiB) | ~0 | 1.744,4 / 409,0 | 54,2 MiB |
| (tham chiếu) | 4 | profile_null: quét CSV + agg | 10,676 | 8 | 10,479 / 10,661 | 471,4 MiB | 0,075 MiB | 0 / 0 | 2,3 MiB |

Mọi cột là median của 5 run đo (`stage_metrics.csv` từng run); riêng spill bytes thì 5 run giống hệt nhau.

**Plan profile trùng** (`explain_profile_dup.txt`, final plan trong `sql_final_plans.txt`):
`Scan csv → HashAggregate(partial, keys = 11 cột) → Exchange hashpartitioning(step, type, amount, nameOrig,
oldbalanceOrg, newbalanceOrig, nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud, 200)
→ AQEShuffleRead coalesced → HashAggregate(final) → Filter(count > 1) → HashAggregate → Exchange SinglePartition → HashAggregate`.

**Quan sát:**

1. Stage 10 đọc cùng 471,4 MiB như stage 4 (profile_null) nhưng mất 32,9 s so với 10,7 s, tức **khoảng 22 s thêm do
   aggregate + shuffle**. Cộng thêm stage reduce 12 (9,6 s), profile_dup tốn khoảng 32 s so với một lần quét CSV thường.
2. Partial aggregate **không giảm được bản ghi nào**: shuffle write = 6.362.620 records = số dòng input, vì mọi dòng có
   khóa 11 cột khác nhau. Cả dataset (413,7 MiB) đi qua Exchange.
3. Đây là **stage duy nhất có spill** của cả pipeline: 3.512,8 MiB memory / 815,2 MiB disk (map + reduce). Peak
   execution memory dừng ở khoảng 52–54 MiB/task, khớp với vùng execution memory của heap 1g chia cho 8 task. Toàn bộ 62
   WARN `RowBasedKeyValueBatch: Calling spill()` trong console rơi vào khung giờ của profile_dup (ví dụ run 4:
   22:26:50–22:27:09, trong khi phase này chạy 22:26:45 → 22:27:28).
4. 11 stage quét CSV còn lại mỗi stage mất khoảng 9–12 s, không shuffle và không spill: chi phí của chúng là **parse CSV
   lặp lại**. Tổng duration của 12 stage quét CSV (median) là 126,24 s.

## 6. Bottleneck và hypothesis

### Đánh giá các ứng viên

| Ứng viên | Evidence | Kết luận |
|---|---|---|
| Hash SHA-256 | 0,599 s (0,4 %) | Bác bỏ |
| Khởi động Spark | 11,049 s (6,9 %), chi phí cố định, không phụ thuộc dữ liệu | Không chọn: không phải execution bottleneck |
| **A. Đếm trùng groupBy 11 cột** | Stage nặng nhất (32,9 s, task max 32,9 s), shuffle 413,7 MiB = 100 % số dòng, spill 3,5 GiB mem / 815 MiB disk (stage duy nhất có spill), phase ổn định (stdev 0,55 s) | **Xác nhận**, đề xuất cho experiment này |
| B. CSV bị parse lại (persist) | 12 stage quét CSV, tổng khoảng 126 s; clean + join + output là 90,5 s job wall (median). Không có shuffle/spill ở các stage này | Xác nhận là chi phí lớn **về tổng**, nhưng chia đều qua 10+ job. Cache dataset 471 MiB với heap 1g phải dựa nhiều vào disk, rủi ro áp lực bộ nhớ. Nguyên nhân gốc (parse text) hợp với Buổi 11 (đổi CSV → Parquet) hơn → để làm experiment sau |

### Bottleneck claim

Chúng tôi cho rằng **bước đếm trùng toàn dòng (`profile_dup`) là bottleneck của chặng profile**, vì nó là stage nặng
nhất pipeline (stage 10: 32,9 s, gấp khoảng 3 lần một lần quét CSV cùng input), đẩy toàn bộ 6.362.620 dòng × 11 cột
(413,7 MiB) qua Exchange do partial aggregate không gộp được dòng nào, và là nơi duy nhất trong pipeline spill
(3.512,8 MiB memory / 815,2 MiB disk) khi execution memory mỗi task chỉ khoảng 52–54 MiB (driver 1g, 8 task).

### Hypothesis

> **Nếu** chặng `profile_dup` chậm **vì** aggregate và shuffle trên khóa 11 cột rộng (6 decimal(20,2) + 2 chuỗi ID),
> làm toàn bộ dữ liệu đi qua Exchange và vượt execution memory nên phải spill, **thì khi** thay khóa groupBy bằng
> `xxhash64(11 cột)` để tìm ứng viên trùng (count > 1), rồi chỉ kiểm chính xác 11 cột trên các dòng có hash ứng viên,
> **chúng tôi kỳ vọng** shuffle write của stage map profile_dup giảm rõ so với 413,7 MiB, spill (memory và disk) giảm mạnh
> (lý tưởng về 0), task max của stage này và `sec_profile_dup` giảm, **trong khi** `duplicate_groups = 0`,
> `duplicate_extra_rows = 0` cùng mọi business metric (6.362.620 dòng, 8.213 fraud, 1.144.392.944.759,77, 152 nhóm,
> 16 dòng amount = 0) giữ nguyên.

**Vì sao vẫn chính xác:** hai dòng giống hệt nhau (kể cả null) luôn có cùng `xxhash64`, nên mọi nhóm trùng thật đều
nằm trong tập hash có count > 1. Va chạm hash chỉ làm tăng số ứng viên, và bước 2 sẽ đếm lại chính xác trên 11 cột.
Nếu không có hash nào có count > 1 thì chắc chắn không có dòng trùng.
Lưu ý: mỗi bản ghi shuffle vẫn lớn hơn 8 byte (UnsafeRow gồm null bitset + key + count), và số record shuffle vẫn là
6.362.620 vì hash của mọi dòng khác nhau. Thứ giảm là **kích thước** record, không phải **số lượng** record.

### Expected signal (kiểm ở Phase 2)

- Plan: vẫn có `Exchange hashpartitioning(...)`, nhưng trên **một cột hash (bigint)** thay vì 11 cột.
- `stage_metrics.csv` của stage map profile_dup: `shuffle_write_mb` < 413,735; `spill_disk_mb` và `spill_memory_mb` giảm
  mạnh; `task_max_sec` < 32,9; WARN `RowBasedKeyValueBatch` giảm.
- `sec_profile_dup` median giảm. Ngưỡng kết luận: vì stdev của profile_dup chỉ 0,548 s, mức giảm ở phase này dễ thấy.
  Với **runtime tổng**, chỉ kết luận "nhanh hơn" nếu Δ median > biên độ baseline (max − min = 21,160 s). Nếu nhỏ hơn
  thì ghi là không kết luận được ở cấp toàn pipeline.
- Không đổi: số job/stage quét CSV ở clean/join/output; mọi business metric; schema output 10 cột.

### Kế hoạch Phase 2 (chờ duyệt)

1. Nhánh `opt/hash-dedup-check` từ `checkpoint-4-baseline`; chỉ đổi `duplicate_frame()` / bước profile_dup.
   Không đổi config Spark.
2. Test: chèn cố ý 1 dòng trùng vào mẫu nhỏ, cách mới phải báo `duplicate_groups = 1`, `duplicate_extra_rows = 1`,
   giống cách cũ.
3. Commit, gắn tag `checkpoint-4-optimized`, rồi chạy `python src/benchmark.py --label baseline optimized --runs 5 --warmup 1`
   (xen kẽ B/A, cả hai phía cùng 1 warm-up).
4. Kiểm correctness: assert các mốc row count, profile, business metric, schema, và so output với `compare_runs.py`.

## 7. Change — *(Phase 2)*
## 8. Before / After — *(Phase 2)*
## 9. Correctness — *(Phase 2)*
## 10. Decision — *(Phase 2)*
