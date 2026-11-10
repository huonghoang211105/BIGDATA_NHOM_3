# Checkpoint 3 — Nhóm 3

## A. Nhóm và trạng thái

- Thành viên: Hoàng Thị Diệu Hương, Nguyễn Thị Hằng My, Nguyễn Hoàng Dương, Thân Văn Dũng.
- Repo/commit: [điền thật].
- Ngày demo, máy chạy: [điền].
- Run ID/path: [điền run SUCCESS].
- Trạng thái: [chưa chạy/PASS/FAILED + evidence].

## B. Input contract

- PaySim CSV, grain một giao dịch, không có transaction ID gốc.
- Input path/link truy cập được: [điền].
- Rows/files/bytes/SHA-256: [từ metrics].
- Schema evidence: [link schema_input.txt].
- Dimension type, một dòng/type; file/hash: [điền].

## C. Transformations

1. Normalize type và trim account IDs; lý do tránh sai join.
2. Validate/filter, giữ amount=0; invalid policy dừng publish và lưu rejected.
3. Derive ngày/giờ mô phỏng, log1p(amount).
4. Select schema rõ ràng.

Counts raw/normalized/valid/invalid/derived: [điền số đo thật].

## D. Join validation

- Left join trên type, fact many-to-one dimension.
- Left/right rows: [điền].
- Dimension duplicate keys/null: [điền].
- After rows: [điền].
- Matched/unmatched/rate: [điền].
- Policy: type thiếu → giữ evidence, dừng publish; review dimension rồi rerun.
- Evidence: [metrics + unmatched sample].

## E. Aggregation và output

- Câu hỏi: quy mô và tỷ lệ nhãn gian lận theo ngày mô phỏng/type.
- Grain: một dòng/(day_index,type) có dữ liệu.
- KPI: count, sum/avg amount, fraud count/amount/rate.
- Path/format/schema: [điền].
- Mode: snapshot/run riêng, errorifexists, latest_success chỉ trỏ run PASS.
- Rows/sample: [điền và link].
- Đối soát tổng giao dịch/gian lận/tiền, uniqueness group key: [điền].

## F. Quality, baseline và tái tạo

- Null/duplicate/invalid/amount zero/step range: [metrics].
- Runtime và phạm vi đo: [điền].
- Input bytes/hash/partitions: [điền].
- Python/Java/Spark/master/OS/CPU/RAM/ổ đĩa: [điền].
- Config AQE/shuffle partitions/auto broadcast: [điền giá trị thực tế].
- Explain saved: [link join/summary].
- Exchange/shuffle/join thấy ở node nào: [mô tả theo plan thật].
- Lệnh chạy: [điền].
- Rerun hai lần: [run IDs và kết quả compare_runs].
- Evidence location và quyền truy cập: [điền].

## G. Blocker và next action

| Việc | Owner | Deadline nhóm chốt | Điều kiện hoàn thành |
|---|---|---|---|
| [blocker hoặc next action thật] | [tên] | [thời điểm] | [evidence] |
| [next action] | [tên] | [thời điểm] | [evidence] |

## Exit ticket

1. End-to-end PASS bằng chứng nào? [trả lời].
2. Join grain/key và nguy cơ shuffle/data explosion? [trả lời].
3. Quality issue lớn nhất? [trả lời].
4. Baseline/commit dùng so với Buổi 10? [trả lời].
