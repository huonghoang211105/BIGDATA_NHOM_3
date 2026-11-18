# Smoke runs của bản tối ưu (KHÔNG tính vào benchmark)

| Run | Code | Chạy từ | Dimension SHA-256 | Mục đích |
|---|---|---|---|---|
| `20261117T160452Z_9f52428e` | working tree trước commit (nội dung = `d2b15a5`) | checkout chính | `e3061072…` (LF) | thử nhanh: checks PASS, `hash_candidates = 0` |
| `20261117T161313Z_eb7c725a` | commit `d2b15a5` | worktree `.bench/d2b15a5a8f73` | `9f570105…` (CRLF) | đúng điều kiện benchmark; `compare_vs_baseline.txt` = **RERUN PASS** so với baseline run `20261117T152622Z_864399ec` |

- **Vì sao 2 SHA của dim khác nhau:** repo bật `core.autocrlf=true`, nên `git worktree add` check-out
  `config/dim_transaction_type.csv` thành CRLF (211 B), còn checkout chính (và CP3) giữ LF (205 B). Mọi worktree
  benchmark (baseline lẫn optimized) đều nhận cùng các byte CRLF, nên input A/B giống hệt nhau.
- **CRLF không đổi kết quả:** `dim_crlf_data_check.txt` so dữ liệu output (schema + exceptAll 2 chiều) giữa CP3 run A
  (dim LF), baseline Phase 1 (dim CRLF) và smoke LF: DATA PASS, 152 dòng, 0 khác biệt.
