# Báo cáo Checkpoint 2 — Environment → Ingestion → Raw Storage

## Mẫu nộp Checkpoint 2

| Trường | Nội dung |
|---|---|
| Tên nhóm / thành viên | Nhóm 3 / TODO: danh sách thành viên và vai trò CP2 |
| Đề tài | Hệ thống phát hiện gian lận tài chính theo thời gian thực (Financial Fraud Detection on PaySim) |
| Git commit / tag | Tag **`checkpoint-2`** trên branch `Jake-branch`; code chạy evidence: commit `1aa5272` (`evidence/checkpoint2/20261112T174903Z/00_commit.txt`) |
| Environment | Python 3.12.10, Java 17.0.20.1 (Temurin), Spark 3.5.9 (PySpark 3.5.9), Mode `local[*]` — Windows 11, `HADOOP_HOME=D:\hadoop` |
| Data source | Kaggle `ealaxi/paysim1` (version 2), CC BY-SA 4.0; snapshot 493.534.783 bytes, 6.362.620 dòng, SHA-256 `16910f90…4eea6b` (`metadata/source.json`) |
| Ingestion command | `python src/ingest_paysim.py --source data/raw/paysim_data.csv --dataset paysim --raw-root data/raw` |
| Raw location | `data/raw/paysim/ingest_date=2026-11-12/run_20261112T175026Z_822219b0/paysim_data.csv` (ingest_date/run_id theo UTC, ngoài Git) |
| Manifest / log location | `metadata/manifest.json` (con trỏ) + manifest trong thư mục run; `logs/ingest_20261112T175026Z_822219b0.txt`; `metadata/ingest_history.jsonl` |
| Rerun behavior | Idempotent theo SHA-256: chạy 3 lần → 1 SUCCESS + 2 SKIPPED_DUPLICATE, 1 thư mục run, 0 byte chép thêm; `--force` để cố ý tạo run mới |
| Blocker còn lại | Snapshot 471 MB không nằm trong Git (máy khác phải tự đặt vào `data/raw/`); `reports/checkpoint3.md` còn nhắc run CP2 cũ đã xoá |
| Owner + deadline | TODO |

## 1. Thông tin chung

| Mục | Nội dung |
|---|---|
| Nhóm | Nhóm 3 |
| Thành viên | TODO: nhóm xác nhận danh sách thành viên và vai trò cho CP2 (README đang ghi 4 thành viên) |
| Đề tài | Hệ thống phát hiện gian lận tài chính theo thời gian thực (Financial Fraud Detection on PaySim) |
| Branch | `Jake-branch` |
| Commit code chạy evidence | `1aa5272` — feat(cp2): idempotent ingest, relative paths, run_id-matched logs (ghi trong `evidence/checkpoint2/20261112T174903Z/00_commit.txt`) |
| Commit nộp / tag | Commit "checkpoint-2: rerun env, ingestion, raw storage", tag **`checkpoint-2`** |
| Tag sao lưu kết quả cũ | `backup/before-cp2-rerun` (chỉ ở local, trỏ tới `d99b28f`, giữ log chạy cũ) |
| Evidence | [`evidence/checkpoint2/20261112T174903Z/`](../evidence/checkpoint2/20261112T174903Z/) |

## 2. Environment (ENV)

Nguồn: [`01_env_check.txt`](../evidence/checkpoint2/20261112T174903Z/01_env_check.txt), `logs/env_check.txt`.

| Thành phần | Version thực tế |
|---|---|
| OS | Windows 11 (AMD64) |
| Python | 3.12.10 (Microsoft Store) |
| Java | Eclipse Temurin OpenJDK 17.0.20.1, `JAVA_HOME` hợp lệ |
| PySpark / Spark | 3.5.9 / 3.5.9 — khớp `requirements.txt` (`pyspark==3.5.9`) |
| Spark master | `local[*]` (8 partition) |
| Hadoop | `HADOOP_HOME=D:\hadoop`, có `bin\winutils.exe` |
| Smoke test | DataFrame `range(1.000.000)` count + filter; Python worker `parallelize(range(1000),4).map(x*2).sum() = 999000` |
| Kết quả | **`ENV PASS`**, exit 0 |

## 3. Nguồn dữ liệu (SOURCE)

| Mục | Nội dung |
|---|---|
| Dataset | Synthetic Financial Datasets For Fraud Detection (PaySim) |
| Nguồn | Kaggle `ealaxi/paysim1` — https://www.kaggle.com/datasets/ealaxi/paysim1 (tác giả Edgar Lopez-Rojas) |
| License | **CC BY-SA 4.0** |
| File gốc | `PS_20174392719_1491204439457_log.csv`, lưu local là `data/raw/paysim_data.csv` |
| Snapshot | 493.534.783 bytes, 6.362.620 dòng, 11 cột, SHA-256 `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b` |
| Data card | `metadata/source.json` (chứa `snapshot_sha256` dùng cho quality gate) |
| Mẫu trong Git | `data/sample/paysim_sample_1000.csv` (header + 1.000 dòng, SHA-256 `762ef5d3…ad8d`) |

## 4. Ingestion (INGEST)

Lệnh:

```powershell
python src/ingest_paysim.py --source data/raw/paysim_data.csv --dataset paysim --raw-root data/raw
```

Luồng: quality gate trên nguồn (tồn tại, bytes > 0, header 11 cột, SHA-256 = `snapshot_sha256`) → kiểm tra trùng snapshot → copy binary sang `.part` → quality gate lần 2 trên bản sao → đổi tên → Spark đọc FAILFAST (count, parse toàn bộ partition, schema, 5 dòng mẫu) → manifest run → con trỏ `metadata/manifest.json` → 1 dòng `metadata/ingest_history.jsonl`.

Kết quả lần 1 ([`02_ingest_run1.txt`](../evidence/checkpoint2/20261112T174903Z/02_ingest_run1.txt)): **SUCCESS**, run `20261112T175026Z_822219b0`, 493.534.783 bytes, Spark `rows = parsed_rows = 6.362.620`, 8 partition, 11 cột, exit 0. Thời gian ~65 giây (copy + 2 lần hash ~3 giây, Spark ~60 giây).

## 5. Raw storage (RAW)

```text
data/raw/paysim/ingest_date=2026-11-12/run_20261112T175026Z_822219b0/
    paysim_data.csv                                  # byte-identical với nguồn
    manifest.json
    spark_read_20261112T175026Z_822219b0.json
```

- `ingest_date` và `run_id` theo **UTC**; timestamp trong từng dòng log theo giờ máy (+07).
- Raw zone ngoài Git (`.gitignore`); bằng chứng cây thư mục: [`08_tree_raw.txt`](../evidence/checkpoint2/20261112T174903Z/08_tree_raw.txt).

## 6. Manifest & log (TRACE)

| Artifact | Vị trí |
|---|---|
| Manifest run | `data/raw/paysim/ingest_date=2026-11-12/run_20261112T175026Z_822219b0/manifest.json` |
| Con trỏ run SUCCESS mới nhất | `metadata/manifest.json` (có `run_manifest`, `log_file`, `spark_report`) |
| Log run | `logs/ingest_20261112T175026Z_822219b0.txt` — tên khớp `run_id` trong manifest |
| Lịch sử | `metadata/ingest_history.jsonl` |
| Xác thực | `python src/verify_manifest.py --manifest metadata/manifest.json` → **PASS** (bytes, SHA-256, log tồn tại) — [`06_verify_manifest.txt`](../evidence/checkpoint2/20261112T174903Z/06_verify_manifest.txt) |
| Spark đọc lại | `python src/inspect_raw.py --manifest metadata/manifest.json` → **PASS**, 6.362.620 dòng — [`07_inspect_raw.txt`](../evidence/checkpoint2/20261112T174903Z/07_inspect_raw.txt) |

Mọi đường dẫn trong manifest / history / log là tương đối so với gốc repo.

## 7. Rerun behavior

Số liệu thật từ [`09_ingest_history.txt`](../evidence/checkpoint2/20261112T174903Z/09_ingest_history.txt):

| Run ID | Lệnh | Status | Destination | Exit |
|---|---|---|---|---|
| `20261112T175026Z_822219b0` | ingest lần 1 | SUCCESS | `run_20261112T175026Z_822219b0/paysim_data.csv` | 0 |
| `20261112T175141Z_f2d86f9d` | ingest lần 2 | SKIPPED_DUPLICATE (`duplicate_of` = `…822219b0`) | trỏ về run gốc | 0 |
| `20261112T175143Z_04cf1c55` | ingest lần 3 | SKIPPED_DUPLICATE (`duplicate_of` = `…822219b0`) | trỏ về run gốc | 0 |
| `20261112T175223Z_1852bbb6` | nguồn `khong_ton_tai.csv` | FAILED: `FileNotFoundError` | `null` (có `planned_destination`) | 1 |

Chạy cùng lệnh ingest 3 lần → **1 SUCCESS + 2 SKIPPED_DUPLICATE, 1 thư mục run, 0 byte chép thêm** (2 lần sau chỉ đọc nguồn để hash ~1 giây). Mỗi lần chạy vẫn có log riêng `logs/ingest_<run_id>.txt` và 1 dòng history. Muốn cố ý tạo run mới: `--force`.

## 8. Blocker & rủi ro

| # | Blocker / rủi ro | Trạng thái | Owner | Deadline |
|---|---|---|---|---|
| 1 | Kaggle cần đăng nhập/API token → không tự tải nguồn trong script; ingest từ snapshot local đã xác thực SHA-256 | Đã xử lý (quality gate theo `snapshot_sha256`) | TODO | TODO |
| 2 | Python bản Microsoft Store: Spark worker gọi `python3` có thể trỏ vào alias Store → `PYSPARK_PYTHON = sys.executable` trong `env_check.py` và `ingest_paysim.py` | Đã xử lý | TODO | TODO |
| 3 | Windows: tiến trình Java con của Spark giữ file khi redirect output bằng PowerShell → evidence được ghi qua pipe | Đã xử lý | TODO | TODO |
| 4 | `reports/checkpoint3.md` còn nhắc run CP2 cũ `run_20261023T180622Z_c45ac87c` (đã xoá khi chạy lại CP2) | Mở — cần nhóm quyết định cập nhật | TODO | TODO |
| 5 | Snapshot 471 MB không nằm trong Git → máy khác phải tự đặt file vào `data/raw/`; có `data/sample/` để chạy thử | Chấp nhận | TODO | TODO |

## 9. Exit ticket

1. **Một lệnh duy nhất để kiểm tra environment của nhóm là gì?**
   `python src/env_check.py` — kiểm tra OS, Java/`JAVA_HOME`, `HADOOP_HOME`/winutils, Python, PySpark (so với bản ghim trong `requirements.txt`) và chạy smoke test Spark (JVM + Python worker). Dòng cuối là `ENV PASS` (exit 0) hoặc `ENV FAIL: <lý do>` (exit 1). Bằng chứng: `01_env_check.txt`.
2. **Nhóm xác định version/snapshot dữ liệu bằng cách nào?**
   Nguồn Kaggle `ealaxi/paysim1` version 2, chốt bằng `snapshot_sha256` (`16910f90…4eea6b`) trong `metadata/source.json`. Ingest chạy quality gate và từ chối mọi file có SHA-256 khác; manifest của run lưu cả `sha256` thực tế lẫn `snapshot_sha256` kỳ vọng.
3. **Nếu chạy ingestion 3 lần, raw storage thay đổi thế nào?**
   Không đổi sau lần đầu: 1 SUCCESS + 2 SKIPPED_DUPLICATE, vẫn đúng 1 thư mục `run_20261112T175026Z_822219b0`, 0 byte chép thêm. Bằng chứng: `03_ingest_run2.txt`, `04_ingest_run3.txt`, `08_tree_raw.txt`, `09_ingest_history.txt`.
4. **Rủi ro lớn nhất trước Buổi 7 là gì?**
   Snapshot 471 MB không nằm trong Git, nên máy khác phải tự đặt file vào `data/raw/paysim_data.csv` trước khi chạy. Giảm thiểu: `data/sample/paysim_sample_1000.csv` để chạy thử đọc Spark, và quality gate kiểm SHA-256 để phát hiện ngay nếu đặt sai file. Owner: TODO. Deadline: TODO.

## 10. Checklist giảng viên tại bàn

| # | Mục kiểm tra | Kết quả | Bằng chứng |
|---|---|---|---|
| 1 | Repo có README + `src/` + raw/log/metadata | PASS | `README.md` mục 3; `src/env_check.py`, `src/ingest_paysim.py`, `src/verify_manifest.py`, `src/inspect_raw.py`; `logs/`, `metadata/`; raw ngoài Git theo quy ước README 3.4 |
| 2 | `env_check` PASS tại chỗ | PASS | `evidence/checkpoint2/20261112T174903Z/01_env_check.txt` (dòng cuối `ENV PASS`, exit 0); chạy lại tại bàn: `python src/env_check.py` |
| 3 | Source có provenance | PASS | `metadata/source.json` (Kaggle paysim1, tác giả, CC BY-SA 4.0, bytes, `snapshot_sha256`); `logs/data_source.json` (`matches_snapshot_sha256: true`) |
| 4 | Ingest chạy lại được + giải thích rerun | PASS | `02_ingest_run1.txt` (SUCCESS), `03_ingest_run2.txt` / `04_ingest_run3.txt` (SKIPPED_DUPLICATE), `09_ingest_history.txt`; chính sách ở mục 7 và README 3.5 |
| 5 | Raw tồn tại, không rỗng, có quy ước path | PASS | `08_tree_raw.txt`; `06_verify_manifest.txt` (493.534.783 bytes); quy ước `data/raw/<dataset>/ingest_date=YYYY-MM-DD/run_<run_id>/` |
| 6 | Manifest/log khớp raw | PASS | `metadata/manifest.json` (`run_id`, `destination`, `log_file` = `logs/ingest_20261112T175026Z_822219b0.txt`); `06_verify_manifest.txt` (bytes + SHA-256 khớp, log tồn tại) |
| 7 | Spark đọc được sample raw | PASS | `07_inspect_raw.txt` (count 6.362.620, schema 11 cột, 5 dòng mẫu); `spark_read_20261112T175026Z_822219b0.json` trong thư mục run |
| 8 | Mỗi thành viên trả lời được phần mình | Nhóm tự chuẩn bị | TODO: phân công câu hỏi theo thành viên |

## 11. Nội dung 6 slide

**Slide 1 — Nhóm 3 & đề tài**
- Phát hiện gian lận tài chính trên PaySim (6,36 triệu giao dịch mô phỏng).
- Checkpoint 2: Environment → Ingestion → Raw Storage. Tag `checkpoint-2`.
- Thành viên & vai trò: TODO.

**Slide 2 — Kiến trúc**
```text
Kaggle paysim1 (CC BY-SA 4.0) ──tải thủ công──▶ data/raw/paysim_data.csv (snapshot, SHA-256 chốt trong metadata/source.json)
        │ ingest_paysim.py: quality gate → dedup theo SHA-256 → copy .part → quality gate → publish
        ▼
data/raw/paysim/ingest_date=<UTC>/run_<run_id>/{paysim_data.csv, manifest.json, spark_read_<run_id>.json}
        │                         │
        ▼                         ▼
metadata/manifest.json (con trỏ)   logs/ingest_<run_id>.txt + metadata/ingest_history.jsonl
        │
        ▼
verify_manifest.py (bytes, SHA-256, log) · inspect_raw.py (Spark count/schema/sample)
```
- Trade-off: Local PySpark + file system thay vì HDFS/cloud: đủ cho snapshot 471 MB, dễ tái tạo; đổi lại chưa phân tán, sẽ scale khi cần.

**Slide 3 — Bằng chứng môi trường**
- Bảng version mục 2; ảnh chụp 3 dòng cuối `01_env_check.txt` (`sum = 999,000`, `KET QUA SMOKE TEST: PASS`, `ENV PASS`).
- `requirements.txt` ghim `pyspark==3.5.9`; env_check FAIL nếu version cài khác bản ghim.
- Setup command:
  ```powershell
  python -m venv .venv
  .venv\Scripts\activate
  pip install -r requirements.txt
  python src/env_check.py
  ```

**Slide 4 — Nguồn & ingestion**
- Kaggle paysim1, CC BY-SA 4.0, 493.534.783 bytes, 6.362.620 dòng, SHA-256 `16910f90…4eea6b`.
- Lệnh ingest (mục 4) và quality gate 4 điều kiện.
- Kết quả: SUCCESS, Spark đọc 6.362.620 dòng, ~65 giây.

**Slide 5 — Raw & manifest**
- Cây thư mục raw (mục 5), `ingest_date`/`run_id` theo UTC.
- Trích `metadata/manifest.json`: `run_id`, `bytes`, `sha256`, `destination`, `log_file`, `spark_rows`, `run_manifest` — đường dẫn tương đối.
- `verify_manifest.py` PASS, `inspect_raw.py` PASS.

**Slide 6 — Rerun & blocker**
- Bảng 4 dòng history (mục 7): 3 lần ingest → 1 SUCCESS + 2 SKIPPED_DUPLICATE, 1 thư mục run, 0 byte chép thêm; nguồn sai → FAILED exit 1.
- Blocker chính (mục 8) + owner/deadline: TODO.
