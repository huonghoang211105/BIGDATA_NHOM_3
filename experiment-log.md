# Experiment log — Checkpoint 4: Benchmark & tối ưu pipeline (Nhóm 3)

> Trạng thái: **HOÀN TẤT**. Baseline `ae84592` (tag `checkpoint-4-baseline`) → optimized `d2b15a5` (tag
> `checkpoint-4-optimized`). Kết quả: **KEEP**. Runtime median 154,751 → 135,565 s (−12,4 %), correctness PASS 283/283.
> Mục 1–6 là Phase 1 (session `phase1`, ngày 17/11). Mục 7–10 là Phase 2 (session `exp1`, ngày 18/11, chạy xen kẽ).
> Mọi con số lấy từ `benchmark.csv` và `evidence/checkpoint4/`. Không có số ước lượng, trừ chỗ ghi rõ "suy luận, chưa đo".

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

### Kế hoạch Phase 2 (đã duyệt ngày 17/11, đã thực hiện như dưới đây)

1. Nhánh `opt/hash-dedup-check` từ `checkpoint-4-baseline`; chỉ đổi `duplicate_frame()` / bước profile_dup.
   Không đổi config Spark.
2. Test: chèn cố ý 1 dòng trùng vào mẫu nhỏ, cách mới phải báo `duplicate_groups = 1`, `duplicate_extra_rows = 1`,
   giống cách cũ.
3. Commit, gắn tag `checkpoint-4-optimized`, rồi chạy `python src/benchmark.py --label baseline optimized --runs 5 --warmup 1`
   (xen kẽ B/A, cả hai phía cùng 1 warm-up).
4. Kiểm correctness: assert các mốc row count, profile, business metric, schema, và so output với `compare_runs.py`.

## 7. Change (một thay đổi duy nhất)

Nhánh `opt/hash-dedup-check`, commit **`d2b15a5`** (tag `checkpoint-4-optimized`). Diff so với tag baseline chỉ ở
`src/quality_report.py` (phần kiểm trùng) và chỗ gọi trong `src/pipeline.py`
(`git diff checkpoint-4-baseline checkpoint-4-optimized -- src/pipeline.py src/quality_report.py`).
**Config Spark không đổi**: driver 1g mặc định, `shuffle.partitions=200`, AQE bật.

| | Code |
|---|---|
| **Before** | `df.groupBy(11 cột).count().filter(count > 1).agg(duplicate_groups, duplicate_extra_rows)` |
| **After, bước 1** | `df.groupBy(xxhash64(11 cột)).count().filter(count > 1)` → collect các hash ứng viên |
| **After, rỗng** | `duplicate_groups = duplicate_extra_rows = 0`. Kết quả chính xác vì hai dòng giống hệt nhau luôn cùng hash; bước 2 không chạy |
| **After, khác rỗng** | broadcast các hash ứng viên → `left_semi` lọc input theo hash → groupBy đủ 11 cột trên tập con (giống hệt cách cũ). Va chạm hash chỉ thêm ứng viên và bị loại ở bước này |

Tên field output giữ nguyên. `metrics.json` có thêm `duplicate_check = {hash_candidates, exact_step_ran}`. Ở cả 6 run
optimized: `hash_candidates = 0`, `exact_step_ran = false`.

**Test** `tests/test_dup_check.py`: so cách cũ và cách mới, **5/5 PASS** (`evidence/checkpoint4/optimized/_test_dup_check.txt`):
1 cặp trùng + 1 cặp chỉ khác 1 cột + 1 dòng lặp 3 lần → (2, 3); không trùng → (0, 0); **va chạm xxhash64 thật** (hoán
vị null giữa `isFraud`/`isFlaggedFraud`): hash ứng viên = 1, trùng thật = 0; dòng trùng có null → (1, 1); mẫu 1.000 dòng
chèn thêm 1 dòng trùng → (1, 1).

## 8. Before / After (session `exp1`, chạy xen kẽ)

`python src/benchmark.py --pair baseline=checkpoint-4-baseline optimized=checkpoint-4-optimized --runs 5 --warmup 1 --session exp1`,
chạy trong một cửa sổ PowerShell riêng ngày 18/11 (11:05–11:38). Máy vừa khởi động lại lúc 10:55, nên OS cache lạnh ở
round warm-up. Thứ tự: B0, A0 (warm-up), B1, A1, …, B5, A5. Kết quả tổng hợp ở `evidence/checkpoint4/exp1/summary_exp1.md`.

### Từng run (giây)

| Round | Baseline run_id | Baseline runtime | profile_dup | Optimized run_id | Optimized runtime | profile_dup |
|---:|---|---:|---:|---|---:|---:|
| 1 (warm-up, bỏ) | 20261118T040532Z_586e9368 | 242,080 | 55,208 | 20261118T040940Z_98eff953 | 181,561 | 26,674 |
| 2 | 20261118T041248Z_c0be2d08 | 156,697 | 38,364 | 20261118T041531Z_c704e241 | 136,780 | 17,794 |
| 3 | 20261118T041753Z_a30dd096 | 151,506 | 37,857 | 20261118T042031Z_62e87044 | 130,144 | 16,666 |
| 4 | 20261118T042247Z_4036d808 | 149,459 | 36,195 | 20261118T042523Z_b9736199 | 134,236 | 16,873 |
| 5 | 20261118T042743Z_6722e467 | 162,640 | 40,126 | 20261118T043032Z_f3b6f273 | 138,536 | 17,366 |
| 6 | 20261118T043257Z_126578d1 | 154,751 | 38,906 | 20261118T043538Z_5f0e4b92 | 135,565 | 17,883 |

RAM trống lúc bắt đầu các run đo: 2.264–3.010 MiB (warm-up: 1.057 / 1.338 MiB). `checks = PASS` ở cả 12 run.

### Bảng before/after (median của 5 run đo; nhiễu = max − min và stdev của baseline trong exp1)

| Metric | Baseline | Optimized | Δ | Δ % | Nhiễu baseline (max−min / stdev) | Vượt nhiễu? |
|---|---:|---:|---:|---:|---:|---|
| **Runtime** (min–max) | **154,751** (149,459–162,640) | **135,565** (130,144–138,536) | **−19,186 s** | **−12,4 %** | 13,181 / 5,104 | **Có** (> max−min) |
| sec_profile_dup | 38,364 | 17,366 | −20,998 | −54,7 % | 3,931 / 1,443 | **Có** (≈ 14,5 × stdev) |
| sec_profile (null + dup) | 51,599 | 30,626 | −20,973 | −40,6 % | 4,192 / 1,596 | Có |
| sec_profile_null | 12,831 | 13,327 | +0,496 | +3,9 % | 0,665 / 0,274 | Không |
| sec_spark_startup | 8,968 | 9,785 | +0,817 | +9,1 % | 1,267 / 0,619 | Không |
| sec_clean | 41,265 | 40,214 | −1,051 | −2,5 % | 5,548 / 2,196 | Không |
| sec_join | 35,519 | 37,018 | +1,499 | +4,2 % | 3,208 / 1,331 | Không |
| sec_output | 15,257 | 15,818 | +0,561 | +3,7 % | 0,855 / 0,348 | Không |
| Max task time (cả run = stage 10) | 28,502 s | 14,609 s | −13,893 | −48,7 % | 1,896 / 0,746 | Có |
| Shuffle write / read (cả run, MiB) | 413,849 / 413,849 | 67,579 / 67,579 | −346,270 | −83,7 % | 0 (byte cố định) | Có |
| Spill memory / disk (cả run, MiB) | 3.512,753 / 815,204 | 1.049,996 / 217,010 | −2.462,8 / −598,2 | −70,1 % / −73,4 % | 0 | Có |
| Số Spark job / run | 46 | 45 | −1 (bước 2 không chạy) | | | |
| WARN `RowBasedKeyValueBatch: Calling spill()` / run | 55–62 | 8 | | | | |
| Stage quét toàn bộ CSV / run | 12 | 12 | 0 (ngoài phạm vi experiment) | | | |
| Output rows | 152 | 152 | 0 | | | |
| fraud_count / total_amount | 8.213 / 1.144.392.944.759,77 | 8.213 / 1.144.392.944.759,77 | 0 | | | |
| *Tham chiếu: baseline Phase 1 (17/11)* | *160,142 (156,453–177,613)* | | | | *21,160 / 8,475* | |

### Stage profile_dup (median 5 run đo, `stage_metrics.csv`)

| Stage | Metric | Baseline | Optimized |
|---|---|---:|---:|
| 10 (map: scan + partial agg + Exchange) | duration / task median / task max | 28,502 / 27,642 / 28,502 s | 14,609 / 14,343 / 14,609 s |
| | shuffle write | 413,735 MiB, **6.362.620 records** | 67,465 MiB, **6.362.620 records** |
| | spill memory / disk | 1.768,340 / 406,176 MiB | 491,998 / 104,870 MiB |
| | peak execution memory / task | 52,205 MiB | 42,000 MiB |
| 12 (reduce: final agg) | duration | 9,062 s | 2,191 s |
| | shuffle read; spill memory / disk | 413,735 MiB; 1.744,412 / 409,028 MiB | 67,465 MiB; 557,998 / 112,140 MiB |

### Plan trước–sau (`explain_profile_dup.txt`, `sql_final_plans.txt`)

| | Baseline | Optimized |
|---|---|---|
| Khóa Exchange | `hashpartitioning(step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig, nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud, 200)` | `hashpartitioning(_groupingexpression#…L, 200)`, với `Project xxhash64(11 cột, 42) AS _groupingexpression` (bigint) |
| AQE ShuffleQueryStage | sizeInBytes = 1.358,7 MiB, rowCount = 6,36E+6 | sizeInBytes = 145,6 MiB, rowCount = 6,36E+6 |
| Exchange thứ hai | Có (SinglePartition cho agg cuối) | Không (chỉ collect các hash có count > 1) |

### Đánh giá hypothesis

- **Đúng:** Exchange chuyển sang một cột bigint. Shuffle bytes −83,7 % trong khi **số record giữ nguyên 6.362.620**,
  khớp điều chỉnh đã nêu ở Phase 1. Task max −48,7 %. `sec_profile_dup` −54,7 %. Các chặng không đổi code (profile_null,
  clean, join, output, startup) đều dao động trong nhiễu.
- **Chỉ đúng một phần:** spill **chưa về 0**. Còn 1.050,0 MiB memory / 217,0 MiB disk (giảm 70–73 %). Peak execution
  memory ở stage map vẫn chạm khoảng 42 MiB/task. *Suy luận (chưa đo riêng):* partial aggregate vẫn phải giữ khoảng 795 nghìn
  khóa hash khác nhau mỗi task (6,36 triệu / 8), vượt vùng execution memory của heap 1g nên phải spill. Bản ghi nhỏ hơn
  thì spill ít hơn nhưng không hết.

## 9. Correctness

`python src/check_correctness.py --session exp1` → **OVERALL PASS 283/283**
(`evidence/checkpoint4/exp1/correctness.json`, `correctness_console.txt`). Script kiểm tất cả 12 run exp1 (kể cả warm-up),
so với run tham chiếu baseline #1 (`20261118T041248Z_c0be2d08`):

| Nhóm kiểm | Kết quả |
|---|---|
| Session: mỗi bên 5 run đo + 1 warm-up, mỗi bên một commit (`ae84592` / `d2b15a5`) | PASS |
| Row count: input 6.362.620 → valid 6.362.620 → after join 6.362.620; unmatched 0; invalid 0 | PASS × 12 |
| Profile: null 0 (11 cột), duplicate_groups 0, duplicate_extra_rows 0, amount = 0 là 16 dòng | PASS × 12 |
| Business: fraud 8.213; tổng tiền 1.144.392.944.759,77; 6.362.620 giao dịch; 152 nhóm | PASS × 12 |
| Input SHA-256 `16910f90…` và dim SHA-256 `9f570105…` giống run tham chiếu | PASS × 12 |
| Output Parquet: 10 cột, schema = tham chiếu, `schema_output.txt` giống hệt, khóa (day_index, type) duy nhất | PASS × 12 |
| Checksum: sort theo (day_index, type), so **từng ô**: 0 khác biệt; SHA-256 các dòng đã sort `dfe6a12120f6e554…` giống nhau ở 12/12 run; exceptAll 2 chiều 0/0 | PASS × 12 |
| `tests/test_dup_check.py` | PASS (exit 0) |

Ngoài ra `compare_runs.py` baseline #1 vs optimized #1 → **RERUN PASS** (`evidence/checkpoint4/exp1/compare_runs_b1_vs_a1.txt`).

## 10. Decision: **KEEP**

**Lý do:**
1. Δ runtime median −19,186 s (−12,4 %; nhanh 1,14×) **lớn hơn** biên độ max − min của baseline trong cùng session
   (13,181 s), và gấp khoảng 3,8 lần stdev baseline (5,104 s). Hai khoảng min–max không chồng nhau: run optimized chậm nhất
   (138,536 s) vẫn nhanh hơn run baseline nhanh nhất (149,459 s).
2. Mức giảm tập trung đúng ở chặng bị nhắm tới: `sec_profile_dup` −20,998 s, tương đương khoảng 14,5 lần stdev của chặng
   này. Toàn bộ Δ runtime giải thích được bằng Δ profile_dup, các chặng khác không đổi trong nhiễu.
3. Cơ chế được xác nhận bằng metric cố định theo byte (không phụ thuộc nhiễu thời gian): shuffle −83,7 %, spill −70–73 %,
   plan có Exchange trên cột bigint.
4. Correctness PASS 283/283, output giống hệt từng ô; bước kiểm trùng vẫn chính xác (đã test cả ca va chạm hash và ca chèn
   dòng trùng).

**So với Phase 1:** Δ −19,186 s **nhỏ hơn** biên độ của Phase 1 (21,160 s). Phase 1 đo ngày khác, lúc máy có ít RAM trống hơn
(1,1–2,3 GiB), và median baseline hôm đó là 160,142 s so với 154,751 s ở exp1. Vì vậy kết luận before/after **chỉ dựa trên
exp1**, nơi hai phía chạy xen kẽ trong cùng điều kiện. So chéo giữa hai ngày không đủ để kết luận về runtime tổng, nhưng
Δ ở chặng profile_dup (−21 s so với stdev khoảng 0,5–1,4 s) vẫn rõ ở cả hai ngày.

### Hạn chế

- 1 máy Windows 11, `local[*]` (driver kiêm executor), driver heap mặc định 1g. Kết quả không đại diện cho cluster.
- 1 file input 471 MiB. Không đo với kích thước khác.
- OS cache: máy khởi động lại trước exp1. Warm-up đầu chậm (baseline 242,1 s, optimized 181,6 s) và đã bị loại cho cả hai
  phía; các run đo coi là warm cache.
- RAM trống lúc đo 2,2–2,9 GiB, IDE (Antigravity) mở trong suốt lúc đo, như nhau cho cả hai phía nhờ chạy xen kẽ.
- Dim CRLF: worktree benchmark check-out `dim_transaction_type.csv` dạng CRLF (SHA `9f570105…`), khác bản LF của CP3
  (`e3061072…`). Hai phía cùng byte, và đã chứng minh output không đổi (`evidence/checkpoint4/optimized/_smoke/dim_crlf_data_check.txt`).
- Phase 1 và exp1 đo ở hai ngày khác nhau, nên bảng before/after chỉ lấy từ exp1.
- Mỗi phía chỉ có 5 run, nên median và biên độ còn thô; chưa làm kiểm định thống kê.

### Câu hỏi giảng viên

**"Nếu data tăng 10× thì giải pháp còn hợp lý không?"** *(suy luận, chưa đo)*
Vẫn hợp lý, và lợi ích có thể lớn hơn. Với khoảng 63,6 triệu dòng, cách cũ phải shuffle cả 11 cột cho mọi dòng (theo tỉ lệ
hiện tại là vài GiB) và spill nhiều hơn với heap 1g. Cách mới chỉ shuffle một bigint cộng count mỗi dòng. Xác suất có va chạm
xxhash64 vẫn rất nhỏ (cỡ n²/2⁶⁵ ≈ 10⁻⁴), và nếu có thì bước 2 vẫn cho kết quả đúng. Hai giới hạn cần biết:
(1) số record shuffle vẫn tăng tuyến tính theo số dòng, spill vẫn còn nếu không tăng bộ nhớ;
(2) nếu dữ liệu có **nhiều** dòng trùng thật, danh sách hash ứng viên được collect về driver có thể lớn. Khi đó nên giữ
ứng viên ở dạng DataFrame (join, không collect). Ngoài ra, chi phí parse CSV (12 stage quét mỗi run) cũng tăng tuyến tính
và sẽ thành phần trội. Đó là lý do cho Buổi 11.

**"Nếu speedup < variance thì kết luận được gì?"**
Không kết luận được rằng pipeline nhanh hơn. Δ khi đó không phân biệt được với dao động giữa các run, nên chỉ được ghi là
"không phát hiện khác biệt ở cấp runtime". Vẫn có thể báo cáo các metric cố định theo byte (shuffle, spill, số record, plan)
như bằng chứng về cơ chế, nhưng không được quy chúng thành speedup. Muốn kết luận thì phải tăng số run, giảm nhiễu (đóng ứng
dụng, cố định nguồn điện) hoặc đo ở cấp chặng có stdev nhỏ. Ở exp1, Δ runtime đã vượt max − min của baseline trong cùng
session nên kết luận được. Nếu chỉ so với biên độ của Phase 1 (21,16 s) thì riêng runtime tổng sẽ là chưa đủ.

### Next action

- **Buổi 11:** chuyển input CSV → Parquet (Data Lake). Hiện mỗi run vẫn có 12 stage parse CSV (tổng input khoảng 11 lần kích
  thước file); clean + join + output chiếm khoảng 93 s ở bản tối ưu. Đánh giá bằng đúng quy trình này: tag baseline/after,
  worktree, chạy xen kẽ, ≥ 5 run, correctness gate.
- **Experiment riêng (không gộp):** tăng driver memory (1g → lớn hơn), để xem spill còn lại ở profile_dup có hết không.
- Ứng viên B (persist sau khi đọc) **không** làm trong đồ án này, theo quyết định chỉ làm một experiment.
