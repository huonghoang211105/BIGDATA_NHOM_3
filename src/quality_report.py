"""Các hàm kiểm chất lượng dữ liệu chính xác cho pipeline (CP3, tối ưu kiểm trùng ở CP4).

Không dùng UDF, không cache, không tinh chỉnh config: mọi kết quả là đếm chính xác trên DataFrame.
Hàm chính:
    sha256()            – băm file input để khóa snapshot
    basic_profile()     – null từng cột, min/max, tổng tiền, số fraud, số dòng amount = 0 (một job)
    duplicate_profile() – đếm dòng trùng toàn bộ cột, chính xác, bằng hash-prefilter 2 bước (CP4)
    require(), save_json() – tiện ích kiểm điều kiện và ghi JSON (Decimal giữ dạng chuỗi chính xác)
"""
import hashlib
import json
from decimal import Decimal
from pathlib import Path
from pyspark.sql import functions as F


def sha256(path):
    """SHA-256 của một file, đọc theo khối 8 MiB để không nạp cả file vào RAM.

    Tham số: path – đường dẫn file. Trả về: chuỗi hex 64 ký tự.
    """
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def save_json(path, value):
    """Ghi value ra file JSON UTF-8 (indent 2). Decimal được ghi thành chuỗi để không mất chữ số."""
    def convert(item):
        if isinstance(item, Decimal):
            return str(item)
        raise TypeError(type(item).__name__)
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2,
                                     default=convert), encoding='utf-8')


def require(condition, message):
    """Cổng chất lượng: nếu condition sai thì raise RuntimeError(message) để pipeline dừng ở FAILED."""
    if not condition:
        raise RuntimeError(message)


def basic_profile(df):
    """Profile cơ bản trong MỘT lần aggregate trên input.

    Trả về dict: rows, null_<cột> cho từng cột, step_min/max, distinct_steps, amount_min/max,
    total_amount, fraud_rows, zero_amount_rows (số giao dịch amount = 0).
    """
    expressions = [F.count('*').alias('rows')]
    for name in df.columns:
        expressions.append(F.sum(F.when(F.col(name).isNull(), 1).otherwise(0))
                           .alias('null_' + name))
    expressions += [F.min('step').alias('step_min'),
                    F.max('step').alias('step_max'),
                    F.countDistinct('step').alias('distinct_steps'),
                    F.min('amount').alias('amount_min'),
                    F.max('amount').alias('amount_max'),
                    F.sum('amount').alias('total_amount'),
                    F.sum('isFraud').alias('fraud_rows'),
                    F.sum(F.when(F.col('amount') == 0, 1).otherwise(0))
                    .alias('zero_amount_rows')]
    return df.agg(*expressions).first().asDict()


def duplicate_candidates(df):
    """Bước 1 của kiểm trùng: tính xxhash64 của TOÀN BỘ dòng (mọi cột), giữ các hash xuất hiện > 1 lần.

    Vì sao nhanh hơn cách cũ (groupBy cả 11 cột):
      * Khóa groupBy chỉ còn MỘT cột bigint 8 byte thay cho 11 cột (6 cột decimal(20,2) + 2 chuỗi ID...).
        Mọi dòng PaySim đều khác nhau nên partial aggregate không gộp được gì: cả 6,36 triệu dòng vẫn
        phải qua Exchange. Số record shuffle KHÔNG đổi, nhưng mỗi record nhỏ hơn nhiều
        (CP4 đo được: 413,7 → 67,5 MiB shuffle write; spill giảm ~70 %; chặng này 38,4 → 17,4 s).
    Vì sao không bỏ sót: hai dòng giống hệt nhau (kể cả vị trí null) luôn cho cùng một hash, nên mọi
    nhóm trùng thật đều nằm trong tập hash có count > 1.

    Tham số: df – DataFrame input. Trả về: DataFrame (chưa thực thi) một cột '_row_hash' gồm các hash ứng viên.
    """
    return (df.groupBy(F.xxhash64(*df.columns).alias('_row_hash')).count()
            .filter(F.col('count') > 1).select('_row_hash'))


def duplicate_frame(df, candidates=None):
    """Đếm trùng CHÍNH XÁC trên toàn bộ cột; trả về DataFrame chưa thực thi (để explain được).

    Tham số:
      df         – DataFrame input.
      candidates – None: groupBy toàn bộ cột trên cả df (cách cũ, dùng làm chuẩn so sánh trong test).
                   DataFrame '_row_hash' (kết quả bước 1): bước 2 – broadcast tập hash ứng viên (nhỏ),
                   left_semi lọc df chỉ giữ dòng có hash ứng viên, rồi mới groupBy đủ mọi cột trên tập con.
    Vì sao vẫn chính xác: va chạm hash (hai dòng KHÁC nhau cùng hash) chỉ làm tập ứng viên lớn hơn;
    groupBy đủ cột ở bước 2 tách chúng ra thành các nhóm count = 1 và bị filter(count > 1) loại bỏ.
    Trả về: DataFrame một dòng: duplicate_groups (số nhóm trùng), duplicate_extra_rows (số dòng thừa).
    """
    rows = df
    if candidates is not None:
        rows = (df.withColumn('_row_hash', F.xxhash64(*df.columns))
                .join(F.broadcast(candidates), '_row_hash', 'left_semi').drop('_row_hash'))
    duplicates = rows.groupBy(*df.columns).count().filter(F.col('count') > 1)
    return duplicates.agg(F.count('*').alias('duplicate_groups'),
                          F.coalesce(F.sum(F.col('count') - 1), F.lit(0))
                          .alias('duplicate_extra_rows'))


def duplicate_profile(df, details=None):
    """Kiểm trùng toàn dòng 2 bước (hash-prefilter), kết quả chính xác như groupBy toàn bộ cột.

    Bước 1: duplicate_candidates() -> collect các hash ứng viên về driver.
      * Rỗng: chắc chắn không có dòng trùng (dòng trùng luôn cùng hash) -> trả 0/0, KHÔNG chạy bước 2.
        Đây là trường hợp của PaySim (hash_candidates = 0), nên chỉ tốn một job shuffle nhỏ.
      * Khác rỗng: bước 2 = duplicate_frame(df, candidates) kiểm chính xác trên tập con.
    Tham số: details – dict (tùy chọn) được điền method, hash_candidates, exact_step_ran để ghi evidence.
    Trả về: {'duplicate_groups': int, 'duplicate_extra_rows': int}.
    Lưu ý khi dữ liệu có RẤT nhiều dòng trùng thật: danh sách hash ứng viên được collect về driver.
    """
    hashes = [row['_row_hash'] for row in duplicate_candidates(df).collect()]
    if details is not None:
        details.update(method='xxhash64 prefilter + exact groupBy on candidates',
                       hash_candidates=len(hashes), exact_step_ran=bool(hashes))
    if not hashes:
        return {'duplicate_groups': 0, 'duplicate_extra_rows': 0}
    candidates = df.sparkSession.createDataFrame([(h,) for h in hashes], '_row_hash bigint')
    return duplicate_frame(df, candidates).first().asDict()


def profile(df):
    """Profile đầy đủ = basic_profile() + duplicate_profile() gộp vào một dict."""
    metrics = basic_profile(df)
    metrics.update(duplicate_profile(df))
    return metrics
