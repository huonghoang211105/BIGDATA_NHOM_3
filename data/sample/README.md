# data/sample — Mẫu PaySim 1.000 dòng

| Thuộc tính | Giá trị |
|---|---|
| File | `paysim_sample_1000.csv` |
| Cắt từ | `data/raw/paysim_data.csv`, snapshot Kaggle paysim1 (CC BY-SA 4.0), 493.534.783 bytes, SHA-256 `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b` |
| Cách cắt | Header + **1.000 dòng dữ liệu đầu tiên**, chép nguyên byte (không sửa, không đổi dòng kết thúc LF) |
| Số dòng | 1.001 dòng vật lý = 1 header + 1.000 giao dịch (step = 1) |
| Kích thước | 74.448 bytes |
| SHA-256 của file sample | `762ef5d31106b08ffc7203aabef0aefcc89646696b951da04a0f0b09eb35ad8d` |
| Giữ nguyên byte | `data/sample/.gitattributes` đặt `*.csv -text`, git không đổi LF↔CRLF |

## Mục đích

Để giảng viên / người review **chạy thử nhanh** mà không cần tải snapshot đầy đủ 471 MB từ Kaggle
(snapshot đầy đủ không đưa vào Git, `data/raw/*` bị `.gitignore`):

```powershell
# Đọc sample bằng Spark (11 cột, 1.000 dòng)
python src/inspect_raw.py --manifest data/sample/paysim_sample_1000.csv
```

Lưu ý: `src/ingest_paysim.py` có quality gate so SHA-256 với `snapshot_sha256` trong
`metadata/source.json`, nên **ingest file sample sẽ FAILED có chủ đích** (sample ≠ snapshot đầy đủ).
Sample chỉ dùng để thử đọc/schema, không phải nguồn của raw zone.

Kiểm tra lại hash:

```powershell
(Get-FileHash data/sample/paysim_sample_1000.csv -Algorithm SHA256).Hash.ToLower()
```
