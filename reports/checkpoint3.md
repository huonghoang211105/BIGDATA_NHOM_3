# Checkpoint 3 — Nhóm 3

Spark batch pipeline trên PaySim, chạy lại với input lấy từ raw zone Checkpoint 2. Mọi số liệu dưới đây lấy từ `metrics.json` của run A/B và output lệnh trong `evidence/checkpoint3/_runs_20261112T181247Z/`.

## A. Nhóm và trạng thái

- Thành viên: Hoàng Thị Diệu Hương, Nguyễn Thị Hằng My, Nguyễn Hoàng Dương, Thân Văn Dũng.
- Repo/commit: `https://github.com/huonghoang211105/BIGDATA_NHOM_3`, branch `Jake-branch`. Code chạy run A/B: commit `eee6567` (refactor(cp3): read raw via CP2 manifest, relative paths, spark conf evidence — ghi trong `_runs_20261112T181247Z/00_commit.txt`). Commit nộp: tag **`checkpoint-3`**.
- Ngày demo, máy chạy: Ngày demo: TODO. Máy chạy: Windows 11 (AMD64), 8 logical CPUs, RAM 7,8 GiB, Python 3.12.10, Java Temurin 17.0.20.1, Spark 3.5.9 `local[*]`.
- Run ID/path: run A `20261112T181257Z_65a10a1a` tại `output/snapshot_16910f90577b/20261112T181257Z_65a10a1a`; run B `20261112T181605Z_058d2196` tại `output/snapshot_16910f90577b/20261112T181605Z_058d2196` (`output/latest_success.json` trỏ run B).
- Trạng thái: **PASS** — run A và B đều `status: SUCCESS`, exit 0; `compare_runs` A↔B và run cũ↔A đều `RERUN PASS`. Evidence: `evidence/checkpoint3/20261112T181257Z_65a10a1a/`, `evidence/checkpoint3/20261112T181605Z_058d2196/`, `evidence/checkpoint3/_runs_20261112T181247Z/`.

## B. Input contract

- PaySim CSV, grain một giao dịch, không có transaction ID gốc.
- Input path/link truy cập được: đọc qua manifest Checkpoint 2 `metadata/manifest.json` (`config.json` → `"input_manifest"`) → `data/raw/paysim/ingest_date=2026-11-12/run_20261112T175026Z_822219b0/paysim_data.csv`. Raw zone không nằm trong Git (471 MB); nguồn gốc Kaggle `ealaxi/paysim1`, CC BY-SA 4.0 (`metadata/source.json`); mẫu 1.000 dòng trong Git: `data/sample/paysim_sample_1000.csv`.
  - **Lineage:** raw run Checkpoint 2 `20261112T175026Z_822219b0` (manifest run: `data/raw/paysim/ingest_date=2026-11-12/run_20261112T175026Z_822219b0/manifest.json`) → run Checkpoint 3 `20261112T181257Z_65a10a1a` (A) và `20261112T181605Z_058d2196` (B). Ghi trong `metrics.json` (`raw_run_id`, `raw_manifest`, `raw_run_manifest`, `input_path`), `_SUCCESS.json` và `output/latest_success.json` (`raw_run_id`), và dòng `[LINEAGE]` của `run.log`.
  - Pipeline yêu cầu manifest `status = SUCCESS` và SHA-256 file = `manifest.sha256` = `config.expected_sha256`, nếu không thì dừng trước khi đọc Spark.
- Rows/files/bytes/SHA-256: 6.362.620 rows / 1 file / 493.534.783 bytes / `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b`; 8 input partitions.
- Schema evidence: [`schema_input.txt`](../evidence/checkpoint3/20261112T181257Z_65a10a1a/schema_input.txt) — schema khai báo, FAILFAST, `enforceSchema=false`: `step` int, `type` string, `amount` decimal(20,2), `nameOrig` string, `oldbalanceOrg` decimal(20,2), `newbalanceOrig` decimal(20,2), `nameDest` string, `oldbalanceDest` decimal(20,2), `newbalanceDest` decimal(20,2), `isFraud` int, `isFlaggedFraud` int.
- Dimension type, một dòng/type; file/hash: `config/dim_transaction_type.csv` (5 dòng: CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER; cột `type`, `type_name_vi`, `flow_group`), SHA-256 `e30610723992fe6fedc51a6b5886e68f7c9f46e0c3b30598828f5d0556680534`.

## C. Transformations

1. Normalize type và trim account IDs; lý do tránh sai join. (`upper(trim(type))`, `trim(nameOrig)`, `trim(nameDest)` — khoá join `type` phải khớp đúng chữ hoa với dimension.)
2. Validate/filter, giữ amount=0; invalid policy dừng publish và lưu rejected. (Invalid = null ở bất kỳ cột nào trong 11 cột, `step < 1`, `amount < 0`, `isFraud`/`isFlaggedFraud` ngoài {0,1}, `type`/`nameOrig`/`nameDest` rỗng. Có invalid → ghi `rejected/` Parquet và dừng trước join. 16 giao dịch `amount = 0` được giữ vì cả 16 đều `isFraud = 1`.)
3. Derive ngày/giờ mô phỏng, log1p(amount). (`day_index = floor((step-1)/24)+1`, `hour_index = (step-1) mod 24`, `log_amount = log1p(amount)`; không bịa timestamp lịch.)
4. Select schema rõ ràng. (11 cột gốc + `day_index`, `hour_index`, `log_amount`, giữ grain giao dịch.)

Counts raw/normalized/valid/invalid/derived: **6.362.620 / 6.362.620 / 6.362.620 / 0 / 6.362.620** (run A và B giống nhau; `metrics.json` → `row_counts`).

## D. Join validation

- Left join trên type, fact many-to-one dimension.
- Left/right rows: 6.362.620 / 5.
- Dimension duplicate keys/null: 0 duplicate key; 0 key hoặc thuộc tính null/rỗng (pipeline `require` cả hai trước khi join).
- After rows: 6.362.620 (= left rows, grain không đổi).
- Matched/unmatched/rate: 6.362.620 / 0 / 0.0.
- Policy: type thiếu → giữ evidence, dừng publish; review dimension rồi rerun.
- Evidence: `metrics.json` → `join` và [`unmatched_sample.json`](../evidence/checkpoint3/20261112T181257Z_65a10a1a/unmatched_sample.json) (`[]`), dòng `[JOIN] before=6362620 after=6362620 unmatched=0` trong `run.log`.

## E. Aggregation và output

- Câu hỏi: quy mô và tỷ lệ nhãn gian lận theo ngày mô phỏng/type.
- Grain: một dòng/(day_index,type) có dữ liệu.
- KPI: count, sum/avg amount, fraud count/amount/rate. (`txn_count = count(*)`, `total_amount = sum(amount)`, `avg_amount = round(avg(amount), 2)`, `fraud_count = sum(isFraud)`, `fraud_amount = sum(amount khi isFraud = 1)`, `fraud_rate_pct = round(fraud_count*100/txn_count, 6)`.)
- Path/format/schema: `output/snapshot_16910f90577b/<run_id>/daily_type_summary/` — Parquet (snappy, 1 file); preview CSV `preview_csv/` (top 50 theo `fraud_count`). Schema [`schema_output.txt`](../evidence/checkpoint3/20261112T181257Z_65a10a1a/schema_output.txt): `day_index` int, `type` string, `type_name_vi` string, `flow_group` string, `txn_count` long, `total_amount` decimal(30,2), `avg_amount` decimal(21,2), `fraud_count` long, `fraud_amount` decimal(30,2), `fraud_rate_pct` double.
- Mode: snapshot/run riêng, errorifexists, latest_success chỉ trỏ run PASS.
- Rows/sample: 152 dòng; mẫu 50 dòng [`sample_output.json`](../evidence/checkpoint3/20261112T181257Z_65a10a1a/sample_output.json), mẫu 5 dòng sau T1–T4 [`sample_transformed.json`](../evidence/checkpoint3/20261112T181257Z_65a10a1a/sample_transformed.json).
- Đối soát tổng giao dịch/gian lận/tiền, uniqueness group key: đọc lại Parquet từ đĩa: tổng `txn_count` = **6.362.620** (= valid), tổng `fraud_count` = **8.213** (= config), tổng `total_amount` = **1.144.392.944.759,77** (= config, khớp đến cent), **152** nhóm (= config); `(day_index, type)` duy nhất (0 nhóm có count ≠ 1). Tỷ lệ gian lận toàn bộ 8.213 / 6.362.620 ≈ 0,129082%.

## F. Quality, baseline và tái tạo

- Null/duplicate/invalid/amount zero/step range: 0 null trên cả 11 cột; 0 dòng trùng toàn bộ 11 cột (`duplicate_groups = 0`, `duplicate_extra_rows = 0`); 0 invalid; 16 dòng `amount = 0` (giữ lại); `step` 1..743 (743 giá trị phân biệt); `amount` 0,00..92.445.516,64.
- Runtime và phạm vi đo: run A **175,063 s**, run B **171,039 s** (`time.perf_counter()`, phạm vi `hash + startup + read + quality + join + aggregate + writes + readback; excludes shutdown`; không gồm chép evidence).

  Thời gian từng chặng, tính từ mốc `run.log`:

  | Chặng (mốc run.log) | Công việc trong chặng | Run A (s) | % run A | Run B (s) |
  |---|---|---:|---:|---:|
  | bắt đầu → `[LINEAGE]` | import PySpark / quality_report | 0,642 | 0,4% | 0,509 |
  | `[LINEAGE]` → `[INPUT]` | hash input + dimension, kiểm header, khởi động Spark + smoke JVM/Python worker, đọc schema, **profile** (null 11 cột, min/max, distinct step, **đếm duplicate bằng groupBy cả 11 cột**) | **71,886** | **41,1%** | **68,901** |
  | `[INPUT]` → `[CLEAN]` | T1–T2: count normalized/invalid/valid; T3–T4: count derived, sample_transformed | 46,703 | 26,7% | 44,942 |
  | `[CLEAN]` → `[JOIN]` | đọc + kiểm dimension, left_anti đếm unmatched, count sau join | 39,288 | 22,4% | 40,359 |
  | `[JOIN]` → `[OUTPUT]` | aggregate, explain, ghi Parquet, đọc lại đối soát, uniqueness, preview CSV | 16,544 | 9,4% | 16,328 |
  | **Tổng** | | **175,063** | 100% | **171,039** |

  Chặng chậm nhất là `[LINEAGE]→[INPUT]`. Hash và khởi động Spark chỉ chiếm vài giây; phần lớn là **profile**, đặc biệt bước đếm duplicate `groupBy` trên cả 11 cột. Đây là lúc Spark in hàng loạt `WARN RowBasedKeyValueBatch: Calling spill()` (run A từ 01:13:34 đến 01:13:51 giờ máy, `01_pipeline_runA.txt`): bộ đệm hash aggregate đầy và Spark phải chuyển sang đường dự phòng. Đây là **ứng viên tối ưu số 1 cho Buổi 10**. Các chặng sau mỗi lần `count()` lại quét lại CSV vì pipeline không cache; đây là ứng viên thứ hai.
- Input bytes/hash/partitions: 493.534.783 bytes / `16910f90…4eea6b` / 8 partitions.
- Python/Java/Spark/master/OS/CPU/RAM/ổ đĩa: Python 3.12.10 (Microsoft Store) / Eclipse Temurin OpenJDK 17.0.20.1 / Spark 3.5.9 / `local[*]` / Windows 11 (AMD64) / 8 logical CPUs / **7,8 GiB** (`total_ram_bytes = 8.344.559.616`) / ổ chứa output: tổng 126.121.668.608 bytes, trống 41.945.563.136 bytes (~41,9 GB) lúc chạy run A.
- Config AQE/shuffle partitions/auto broadcast: giá trị thực tế đọc bằng `spark.conf.get()` (`metrics.json` → `environment.effective_sql_conf`): `spark.sql.adaptive.enabled = true`, `spark.sql.shuffle.partitions = 200`, `spark.sql.autoBroadcastJoinThreshold = 10485760b` (10 MiB), `spark.sql.adaptive.coalescePartitions.enabled = true`. Nhóm không chỉnh tham số nào, tất cả là mặc định của Spark 3.5.9.
- Explain saved: [`explain_join.txt`](../evidence/checkpoint3/20261112T181257Z_65a10a1a/explain_join.txt), [`explain_summary.txt`](../evidence/checkpoint3/20261112T181257Z_65a10a1a/explain_summary.txt).
- Exchange/shuffle/join thấy ở node nào:
  - Theo `explain_summary.txt`: `Scan csv (1)` → `Filter (2)` → `Project (3)` ở nhánh fact. Nhánh dimension là `Scan csv (4)` → `Filter (5)` → `Project (6)` → **`BroadcastExchange (7)`** (`HashedRelationBroadcastMode`).
  - Hai nhánh gặp nhau ở **`BroadcastHashJoin LeftOuter BuildRight (8)`** (left keys `type#372`, right keys `type#616`): dimension 5 dòng nhỏ hơn ngưỡng 10 MiB nên được broadcast, **fact 6,36 triệu dòng không bị shuffle cho join**.
  - Sau đó là `HashAggregate (10)` (partial) → **`Exchange (11)` `hashpartitioning(day_index, type, type_name_vi, flow_group, 200)`** → `HashAggregate (12)` (final) → `Project (13)` → `AdaptiveSparkPlan (14)`. Đây là shuffle duy nhất của pipeline, nằm ở bước group-by.
  - `explain_join.txt` chỉ có join: `BroadcastExchange (7)` + `BroadcastHashJoin LeftOuter BuildRight (8)`, không có `Exchange` hash.
  - **Vì sao `shuffle.partitions = 200` nhưng output chỉ 1 partition:** plan lưu trước khi chạy (`isFinalPlan=false`) nên vẫn ghi 200. Lúc chạy, AQE (`adaptive.coalescePartitions.enabled = true`) thấy 152 nhóm sau partial aggregate chỉ vài KB nên gộp 200 shuffle partition thành 1. Kết quả là ghi ra đúng 1 file Parquet (`part-00000-…snappy.parquet`) và `metrics.output.partitions = 1` khi đọc lại.
- Lệnh chạy: `python src/pipeline.py --config config/config.json` (mặc định `--evidence-dir evidence/checkpoint3`).
- Rerun hai lần: run A `20261112T181257Z_65a10a1a` và run B `20261112T181605Z_058d2196`. `python src/compare_runs.py <A> <B>` → `Rows: A=152 B=152`, `Rows only in A=0, only in B=0`, **`RERUN PASS: identical schema and business rows in both directions`**, exit 0 (`03_compare_A_vs_B.txt`).
  - **So với run cũ trước refactor:** `python src/compare_runs.py output/snapshot_16910f90577b/20261110T095859Z_ec6b3d33 <A>` → 152/152, chênh lệch 0/0, **RERUN PASS**, exit 0 (`04_compare_old_vs_A.txt`). Refactor (đọc qua manifest, đường dẫn tương đối, thêm evidence môi trường) không đổi kết quả nghiệp vụ.
  - Run cũ `20261110T095859Z_ec6b3d33` và `20261110T100253Z_30d5dc85` **chỉ bị xoá sau khi lệnh so sánh trên PASS**. Evidence nhẹ của run cũ vẫn xem được qua tag `backup/before-cp3-rerun` (`git show backup/before-cp3-rerun:evidence/checkpoint3/20261110T095859Z_ec6b3d33/metrics.json`).
- Evidence location và quyền truy cập: trong Git tại `evidence/checkpoint3/20261112T181257Z_65a10a1a/`, `evidence/checkpoint3/20261112T181605Z_058d2196/` (12 file nhẹ mỗi run, do pipeline tự chép) và `evidence/checkpoint3/_runs_20261112T181247Z/` (output từng lệnh, có dòng lệnh và `EXIT_CODE`). Parquet/CSV và raw zone ở `output/`, `data/raw/` ngoài Git, chỉ có trên máy chạy. Quyền truy cập repo cho giảng viên: TODO xác nhận (repo public hoặc đã mời).

## G. Blocker và next action

| Việc | Owner | Deadline nhóm chốt | Điều kiện hoàn thành |
|---|---|---|---|
| Tối ưu chặng profile (đếm duplicate groupBy 11 cột, WARN spill) — chặng chậm nhất 71,9 s / 41% runtime | TODO | TODO | Runtime chặng `[LINEAGE]→[INPUT]` giảm so với 71,886 s, `compare_runs` vẫn RERUN PASS, số đối soát giữ nguyên |
| Giảm quét lại CSV giữa các `count()` (cache/persist sau T4 hoặc đọc lại có kiểm soát) và thử `spark.sql.shuffle.partitions` nhỏ hơn 200 | TODO | TODO | Baseline trước/sau cùng commit, cùng máy, đo bằng `metrics.runtime_sec` + bảng chặng |
| Snapshot 471 MB không nằm trong Git — máy khác phải tự đặt vào `data/raw/` và chạy ingest CP2 trước | TODO | TODO | Hướng dẫn trong README mục 3.2 được thành viên khác chạy lại thành công |
| Chuẩn bị demo và trả lời câu hỏi phần mình | TODO | TODO | Mỗi thành viên giải thích được mục được phân công |

## Exit ticket

1. End-to-end PASS bằng chứng nào? Run A `20261112T181257Z_65a10a1a` và run B `20261112T181605Z_058d2196` đều exit 0, có `_SUCCESS.json` và `metrics.json` `status: SUCCESS`. `run.log` ghi `[INPUT] rows=6362620`, `[CLEAN] valid=6362620 rejected=0`, `[JOIN] … unmatched=0`, `[OUTPUT] groups=152`. Đối soát đọc lại Parquet khớp config (6.362.620 giao dịch, 8.213 gian lận, 1.144.392.944.759,77, 152 nhóm). `compare_runs` A↔B và run cũ↔A đều RERUN PASS (`_runs_20261112T181247Z/01`–`04`).
2. Join grain/key và nguy cơ shuffle/data explosion? Fact grain một giao dịch, left join many-to-one trên `type` sang dimension 5 dòng. Không có data explosion vì dimension được `require` 0 key trùng và 0 null; số dòng sau join = trước join = 6.362.620, unmatched = 0. Không shuffle fact cho join vì plan dùng `BroadcastExchange` + `BroadcastHashJoin LeftOuter BuildRight` (dimension < `autoBroadcastJoinThreshold` 10 MiB). Shuffle duy nhất là `Exchange hashpartitioning(…, 200)` ở group-by, AQE gộp còn 1 partition.
3. Quality issue lớn nhất? Dữ liệu sạch về kỹ thuật (0 null, 0 duplicate, 0 invalid). Issue nghiệp vụ lớn nhất là 16 giao dịch `amount = 0` đều mang `isFraud = 1`: rule `amount >= 0` giữ lại chúng để không làm mất gian lận thật. Về hiệu năng, bước kiểm duplicate trên cả 11 cột chiếm phần lớn chặng chậm nhất và gây WARN spill.
4. Baseline/commit dùng so với Buổi 10? Code commit `eee6567` (tag `checkpoint-3`), input raw run `20261112T175026Z_822219b0`, SHA-256 `16910f90…4eea6b`. Baseline: run A 175,063 s, run B 171,039 s trên Windows 11, 8 CPU, 7,8 GiB RAM, Spark 3.5.9 `local[*]`, AQE bật, `shuffle.partitions = 200`, broadcast threshold 10 MiB, 8 input partitions. Buổi 10 so sánh trên cùng input/máy bằng `metrics.runtime_sec`, bảng chặng mục F và `compare_runs` để xác nhận kết quả không đổi.
