# BIGDATA_NHOM_3
# Hệ thống phát hiện gian lận tài chính theo thời gian thực

Đồ án môn Dữ liệu lớn — Nhóm 3.

Dự án hướng đến việc tiếp nhận giao dịch, xử lý dữ liệu, dự đoán gian lận và hiển thị cảnh báo gần thời gian thực. Dữ liệu sử dụng là PaySim, một bộ dữ liệu giao dịch tài chính mô phỏng.

Trong **Checkpoint 2 — Buổi 6**, nhóm tập trung hoàn thành **Environment → Ingestion → Raw Storage**. Huấn luyện mô hình, Kafka và dashboard là các phần phát triển tiếp theo, không phải điều kiện hoàn thành checkpoint này.

## Thành viên

| Thành viên | Phụ trách trong checkpoint |
|---|---|
| Hoàng Thị Diệu Hương | Điều phối, repo, tài liệu và báo cáo |
| Nguyễn Hoàng Dương | Nguồn dữ liệu và ingestion |
| Nguyễn Thị Hằng My | Môi trường và kiểm tra dữ liệu bằng Spark |
| Thân Văn Dũng | Raw storage, manifest và kiểm tra tính toàn vẹn |

## Trạng thái hiện tại

Repo đã được tạo. Nhóm đang chuẩn bị notebook và bằng chứng thực thi trên Kaggle.

- [x] Tạo Git repository.
- [ ] Hoàn thiện cấu trúc repo và đưa notebook vào repo.
- [ ] Chạy Spark smoke test và lưu version/log.
- [ ] Xác nhận nguồn dữ liệu và snapshot.
- [ ] Ingest sample, sau đó ingest full dataset.
- [ ] Kiểm tra raw bằng Spark và đối chiếu manifest.
- [ ] Kiểm tra hành vi khi chạy ingestion lại 3 lần.
- [ ] Lưu Kaggle Notebook version, tải bằng chứng và ghi commit/tag checkpoint.

Chỉ đánh dấu hoàn thành khi đã có bằng chứng tương ứng. README này chưa xác nhận Spark hoặc ingestion đã chạy thành công.

## Mục tiêu Buổi 6

1. Môi trường Python, Java và PySpark chạy được, có thể tái tạo theo hướng dẫn.
2. Nguồn dữ liệu truy cập được và có thông tin nguồn gốc.
3. Ingestion script chạy thành công, báo lỗi rõ khi đầu vào không hợp lệ.
4. Raw được bảo toàn, lưu theo dataset, ngày ingest và run ID.
5. Manifest và log cho phép xác định nguồn, thời gian, dung lượng, hash và vị trí dữ liệu.
6. Spark đọc được raw và xuất số dòng, schema cùng sample.

## Nguồn dữ liệu

- **Tên:** Synthetic Financial Datasets For Fraud Detection — PaySim.
- **Kaggle dataset:** `ealaxi/paysim1`.
- **Nguồn:** https://www.kaggle.com/datasets/ealaxi/paysim1
- **Loại dữ liệu:** giao dịch tài chính mô phỏng; không phải giao dịch ngân hàng thực tế.
- **Giấy phép trên data card:** CC BY-SA 4.0; giữ thông tin nguồn và tuân thủ các điều kiện ghi công, chia sẻ tương tự khi chia sẻ lại dữ liệu thuộc phạm vi giấy phép.

Schema gồm 11 cột:

```text
step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig,
nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud
```

`isFraud` là nhãn phục vụ nghiên cứu mô hình. `isFlaggedFraud` là cờ luật có sẵn trong dataset, không đồng nhất với nhãn gian lận.

Số dòng, dung lượng và SHA-256 của snapshot sẽ được ghi từ lần chạy thực tế. Nếu xác định được dataset version trên Kaggle, lưu thêm version đó trong metadata.

## Môi trường thực thi

Nhóm sử dụng **Kaggle Notebook, Python và CPU** cho checkpoint. Spark chạy ở chế độ **`local[2]` trên một máy chủ Kaggle**.

Notebook checkpoint được chuẩn bị với **PySpark 4.1.1**, chọn **JDK 17 hoặc 21** và ghi nhận version Python thực tế của môi trường Kaggle. Không cần cài Spark hoặc Java trên laptop để thực hiện quy trình Kaggle này.

Version đã chạy phải lấy từ log và metadata sau smoke test; cấu hình dự kiến không thay thế bằng chứng thực thi.

## Cấu trúc repo

Cấu trúc mục tiêu:

```text
BIGDATA_NHOM_3/
├── README.md
├── .gitignore
├── src/                         # Script xuất từ notebook sau khi chạy
├── data/
│   ├── raw/                     # Giữ .gitkeep; CSV lớn không commit
│   └── sample/                  # Sample nhỏ, bổ sung khi cần
├── logs/                        # Log smoke test, ingestion, Spark read
├── metadata/                    # Nguồn, môi trường, manifest và rerun report
└── notebooks/
    └── 01_Checkpoint_Buoi6_Kaggle.ipynb
```

Các thư mục chưa có nội dung có thể dùng `.gitkeep` để Git lưu lại. Script, log, metadata và notebook được bổ sung trong quá trình thực hiện.

Raw full được lưu trong Kaggle Notebook Outputs và bản lưu dự phòng của nhóm. Repo lưu code, hướng dẫn, metadata, log nhỏ và đường dẫn truy cập dữ liệu; không commit CSV full, khóa truy cập hoặc mật khẩu.

## Chạy checkpoint trên Kaggle

### 1 Chuẩn bị notebook và dữ liệu

1. Tải file `01_Checkpoint_Buoi6_Kaggle.ipynb` từ thư mục `notebooks/` sau khi nhóm thêm file vào repo.
2. Đăng nhập Kaggle, tạo Notebook Python và import file trên bằng mục Import/Upload Notebook.
3. Chọn **Accelerator: None/CPU** và bật **Internet** cho bước cài dependency.
4. Trong Input, chọn **Add Input/Add Data**, tìm và gắn dataset **`ealaxi/paysim1`**.
5. Kiểm tra dataset vẫn được gắn sau khi import notebook. Không cần tải CSV về laptop rồi upload lại.

### 2 Chạy theo thứ tự

| Ô notebook | Nội dung |
|---|---|
| 01–03 | Tạo thư mục dự án, cài PySpark và cấu hình Java/Python |
| 04 | Tìm CSV PaySim, tính SHA-256 và ghi thông tin nguồn |
| 05 | Xuất các script dự án vào `src/` |
| 06 | Chạy Spark smoke test |
| 07 | Tạo sample có lineage, ingest và kiểm tra bằng Spark |
| 08 | Ingest full CSV, kiểm hash và đọc raw bằng Spark |
| Các ô còn lại | Kiểm rerun, trường hợp lỗi và đóng gói bằng chứng |

Nếu một ô báo lỗi, xử lý lỗi trước khi chuyển sang bước phụ thuộc. Nếu đã import một version PySpark khác, restart session và chạy từ đầu theo cấu hình notebook.

Smoke test đạt khi có Spark version, phép tính `spark.range(1_000_000).count()` trả về `1000000` và tiến trình kết thúc thành công.

### 3 Lưu kết quả

1. Khi notebook chạy hoàn chỉnh, chọn **Save Version → Save & Run All**.
2. Chờ version thực thi xong, mở đúng bản đã lưu và kiểm tra Outputs.
3. Tải ZIP hồ sơ nhẹ và notebook có output về máy.
4. Giữ raw trong saved Outputs và lưu bản dự phòng theo điều kiện của nhóm. ZIP hồ sơ nhẹ không chứa toàn bộ các CSV full.
5. Đưa script, log nhỏ và metadata cần nộp về repo.
6. Cập nhật README, ghi URL Kaggle Notebook version và commit hash hoặc tag `checkpoint-2` vào báo cáo.

Không coi draft hoặc notebook chưa có output là bằng chứng chạy thành công.

## Đường dẫn trên Kaggle

| Đường dẫn | Vai trò |
|---|---|
| `/kaggle/input/...` | Nguồn dataset chỉ đọc |
| `/kaggle/working/nhom3_buoi6/src/` | Mã nguồn được xuất từ notebook |
| `/kaggle/working/nhom3_buoi6/data/raw/` | Raw đã ingest |
| `/kaggle/working/nhom3_buoi6/logs/` | Log thực thi |
| `/kaggle/working/nhom3_buoi6/metadata/` | Thông tin nguồn, môi trường và kiểm tra |
| `/kaggle/working/Nhom3_Buoi6_ho_so_nhe.zip` | Hồ sơ nhẹ để tải về |

## Lệnh kiểm tra và ingestion

Notebook thực hiện các lệnh này bằng Python executable của môi trường hiện tại. Sau khi notebook đã tạo script và cấu hình môi trường, có thể chạy từ project root `/kaggle/working/nhom3_buoi6`:

```bash
python src/env_check.py
```

Ingest file PaySim:

```bash
python src/ingest_file.py --source "<duong_dan_CSV_nguon>" --dataset paysim --raw-root data/raw
```

Kiểm manifest và đọc raw bằng Spark:

```bash
python src/verify_manifest.py --manifest "<duong_dan_manifest.json>"
python src/inspect_raw.py --manifest "<duong_dan_manifest.json>"
```

Thay các giá trị trong `<...>` bằng đường dẫn thật do notebook xuất ra. Các lệnh này không phải output đã chạy; log thực tế phải được lưu sau khi thực thi.

## Quy tắc raw và manifest

Raw được tổ chức theo quy tắc:

```text
data/raw/paysim/
└── ingest_date=YYYY-MM-DD/
    └── run_<UTC_TIMESTAMP>_<id>/
        ├── <ten_file_nguon>.csv
        └── manifest.json
```

- Sao chép nguyên byte của CSV nguồn; không sửa tay, xóa dòng hay làm sạch trong raw.
- Kiểm nguồn tồn tại, không rỗng, đúng header và parse sample trước khi chấp nhận ingestion.
- Kiểm dung lượng và SHA-256 của bản sao khớp với nguồn.
- Chỉ công bố manifest thành công sau khi các bước kiểm tra và ghi truy vết cần thiết hoàn tất.
- Spark đọc đúng file theo manifest để kiểm count, schema và sample.

Manifest ghi tối thiểu: dataset, run ID, nguồn, thời gian ingest UTC, dung lượng byte, SHA-256, đường dẫn đích và trạng thái. Metadata nguồn bổ sung URL, dataset version nếu biết và thông tin giấy phép.

## Hành vi khi chạy lại

Policy hiện tại là **mỗi lần ingestion thành công tạo một run riêng**, không ghi đè raw của run trước.

Nếu chạy 3 lần với cùng một snapshot:

- Có 3 thư mục run và 3 manifest riêng.
- Các run có cùng SHA-256 nếu nội dung nguồn không thay đổi.
- Các bản sao được nhận diện là cùng snapshot bằng hash.
- Khi xử lý hoặc train, chọn một run cụ thể; không gộp cả ba bản sao làm dữ liệu bị nhân bản.

Đây là policy lưu snapshot theo run, chưa phải cơ chế bỏ qua bản sao tự động theo hash. Input lỗi phải có log lỗi rõ ràng, không được báo thành công giả.

## Bằng chứng cần nộp

- Version Python/Java/Spark và log smoke test PASS.
- Thông tin nguồn, snapshot hash và dataset version nếu xác định được.
- Ingestion script, lệnh thực thi và log.
- Raw không rỗng, có quy tắc thư mục và đường dẫn truy cập rõ ràng.
- Manifest khớp với raw theo dung lượng và SHA-256.
- Kết quả Spark đọc raw: số dòng, schema và sample.
- Kết quả chạy lại ingestion 3 lần và giải thích policy.
- README, Kaggle Notebook version, commit/tag và báo cáo checkpoint.
- Blocker còn lại, người phụ trách và thời hạn xử lý nếu có.

## Hướng phát triển sau checkpoint

- Huấn luyện baseline Logistic Regression và Random Forest trên Kaggle.
- Chia dữ liệu theo thời gian; chọn mô hình và ngưỡng bằng validation, đánh giá test sau khi khóa thiết kế.
- Giả lập luồng CSV qua Kafka và xử lý bằng Spark Structured Streaming.
- Lưu kết quả dự đoán, xây dựng dashboard và cảnh báo.
- Đo throughput, latency và hành vi khi khởi động lại pipeline.

Các mục này là kế hoạch phát triển. Không ghi thành kết quả đã đạt khi chưa có bằng chứng đo hoặc chạy tương ứng.
