# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Đoàn Phương Linh |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602382 |
| GitHub | DoanPhuongLinh |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-05905 |

> Ghi chú về trip: MSSV có chữ `A` nên `int(MSSV) % 26` của GUIDE không chạy được; tôi dùng 5 số cuối `02382`: `2382 % 26 = 16` → phần tử thứ 16 của `scripts/lab.sh cli list-trips` là `trip-05905`.

## Kết quả cuối

- `dataset_version` của bạn: `00f83d03d17d`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 684 / 164 / 171 (tổng 1019 khung; 18 / 4 / 4 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).
- Đầu ra đầy đủ của từng bước và thí nghiệm: `report/run_logs/`.

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Khác biệt thấy trong `report/run_logs/step1_bronze.txt` (A 3021 dòng, B 3234 dòng):
1. **Tên cột**: A dùng `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer`; B dùng `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering`.
2. **Kiểu thời gian**: A là chuỗi ISO UTC (`2025-10-07T08:00:00.014Z`); B là epoch mili‑giây (`1759933801509`).
3. **Đơn vị tốc độ**: A là m/s (`1.213`), B là km/h (`55.11`).
4. **Bàn đạp**: A là tỷ lệ 0–1 (`gas 0.028`), B là phần trăm (`throttle_pct 2.0`).
5. **Đồng hồ**: B chạy trước đồng hồ video `telemetry_clock_offset_ms = 1500` (bảng `bronze_video_files`), A lệch 0.

Bronze giữ nguyên bản, mỗi nguồn một bảng, mọi cột là chữ, vì đây là bản ghi "sự thật gốc" chỉ thêm (append‑only): nếu ép kiểu/gộp ngay mà luật sai (ví dụ hiểu nhầm km/h là m/s, hay ô trống bị ép thành 0) thì mất thông tin gốc, không sửa lại được. Để là chữ thì không dòng nào bị rơi vì lỗi parse; việc chuẩn hoá dồn sang silver, nơi có thể chạy lại bất cứ lúc nào từ bronze. Bằng chứng nạp idempotent: lần nạp thứ hai báo `already present` (nhận diện bằng sha256 của file), và `cli history bronze.raw_telemetry_a` chỉ có **1 commit** (`WRITE`, 3021 dòng).

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

`stg_telemetry_b.sql` đổi tên cột về schema của A, chia `spd_kmh / 3.6` → m/s, chia bàn đạp `/100`, và đổi `timestamp_ms` thành timestamp UTC **sau khi trừ** `telemetry_clock_offset_ms` (1500 ms) lấy từ `bronze.video_files`. Hai nguồn được `union all by name` rồi phân loại từng dòng (`silver_telemetry_classified`).

Bằng chứng: B bắt đầu lúc `2025-10-08 14:30:00.009` — khớp giờ bắt đầu video `14:30:00` (nếu chưa trừ sẽ là `14:30:01.5`); tốc độ tối đa sau chuẩn hoá là 71 km/h (A) và 100 km/h (B), hợp lý; verify "drive B clock offset removed" PASS.

| drive | duplicate | speed_missing | speed_out_of_range | sạch | sạch + cách ly |
|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 2968 | 2968 + 53 = 3021 ✔ |
| drive_b | 20 | 30 | 3 | 3181 | 3181 + 53 = 3234 ✔ |

Dòng lỗi (106 dòng) vào `silver_telemetry_rejects` kèm `reject_reason` thay vì bị xoá vì: (1) có thể **đối soát** sạch + cách ly = nhận về, nên biết chắc không mất dòng nào im lặng; (2) nếu luật sai (ví dụ ngưỡng tốc độ quá chặt) có thể khôi phục/chạy lại; (3) số lượng theo lý do là tín hiệu giám sát nguồn dữ liệu (cảm biến hỏng, thiết bị gửi trùng).

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` (khoảng hở > 1000 ms giữa hai mẫu) là `warn` vì mất tín hiệu cảm biến là **khuyết tật đã biết và chấp nhận được**: dữ liệu vẫn đúng, chỉ thưa; khung hình nằm trong khoảng hở sẽ bị cờ `sync_lost` và loại riêng ở gold. Ở lần chạy của tôi test này báo `WARN 2` (2 khoảng hở) nhưng pipeline vẫn đi tiếp (`PASS=6 WARN=1 ERROR=0`). Còn `in_range` (0..70 m/s) là `error` vì tốc độ vô lý nghĩa là **dữ liệu sai**, đi tiếp sẽ làm hỏng nhãn huấn luyện — phải dừng.

Thí nghiệm `break-gate` nới luật làm sạch `max_speed_mps` 70 → 1000: các giá trị vô lý không bị cách ly nữa và lọt vào silver; `dbt test` cho `PASS=5 WARN=1 ERROR=1`, test `in_range_silver_telemetry_speed_mps__70__0` **bắt được 6 dòng** (đúng 3 + 3 dòng `speed_out_of_range` của hai drive). Khôi phục luật: `PASS=6 WARN=1 ERROR=0`. Điều này cho thấy cổng chất lượng là lớp phòng thủ **độc lập** với luật biến đổi: ai đó sửa nhầm luật làm sạch thì test vẫn chặn lại trước khi dữ liệu xấu tới gold (trong DAG, task sau `dbt_test_silver` sẽ không chạy).

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Chạy lại partition `trip-00003` hai lần: trước **1246 dòng**, dấu vân tay `6c7480dfde7e`; sau hai lần vẫn **1246 dòng**, vân tay `6c7480dfde7e` → không đổi (`unchanged: true`). `cli history silver.frames` cho thấy hai commit mới (version 26, 27), mỗi commit `WRITE 60 dòng` với predicate `trip_id = 'trip-00003'` — tức thay thế đúng partition chứ không thêm. Cách sai `append` hai lần cho cùng trip (60 khung) ra **120 dòng** → nhân đôi (2x).

Delta `overwrite` với predicate là **một giao dịch nguyên tử**: xoá dòng cũ của trip và thêm dòng mới nằm trong cùng một commit vào `_delta_log`. Nếu tiến trình chết giữa chừng thì commit không được ghi, người đọc vẫn thấy phiên bản cũ đầy đủ. Còn "xoá rồi ghi lại" là hai thao tác riêng: lỗi sau bước xoá để lại bảng **thiếu** trip đó (người đọc thấy dữ liệu mất), và lỗi giữa lúc ghi có thể để lại một nửa. Ngoài ra Delta giữ lịch sử nên có thể time travel về version trước.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung **đen** có độ sáng trung bình < 12 (`dark_luma`): camera hỏng/che ống kính, không có thông tin → **loại** (A 6, B 4 khung). Khung **tối** có độ sáng 12–40 (`dark_soft`): đêm, đường hầm, bóng râm — vẫn là cảnh thật → **gắn cờ `is_dark` nhưng giữ** (B có 61 khung, A 4). Tôi đồng ý: mô hình xe tự lái phải học được điều kiện thiếu sáng; bỏ khung tối sẽ làm dataset thiên lệch về ban ngày. Cờ vẫn hữu ích để cân bằng hoặc đánh giá riêng.

Tôi giữ ngưỡng **2**: ở 0 chỉ bắt khung giống hệt bit (A 61) — bỏ sót camera đứng hình có nhiễu nén; ở 6 thì A mất 363/602 khung (60%), B mất 244/644 — loại cả khung khác nhau thật khi xe chạy chậm. Ngưỡng phụ thuộc tốc độ: xe chậm (drive A, nhiều trip < 20 km/h) thì hai khung cách 0,5 s gần như giống nhau, nên cùng ngưỡng sẽ loại nhiều hơn hẳn (A 149 so với B 53 ở ngưỡng 2); có thể dùng ngưỡng thấp hơn khi tốc độ thấp hoặc lấy mẫu theo quãng đường thay vì theo thời gian.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Trên 1019 khung đáng dùng thuộc 26 trip: chia **theo khung hình** (ngẫu nhiên 70/15/15) làm **26/26 trip** nằm ở nhiều hơn một split, và **26 trip có khung ở cả train lẫn test**. Chia **theo trip** (gold của pipeline): **0 trip** bị chia cắt (test `no_straddle` PASS).

Các khung liền nhau của một trip cách nhau 0,5 s nên gần như giống hệt (cùng đường, cùng xe phía trước, cùng ánh sáng). Nếu khung t ở train và t+1 ở test, mô hình chỉ cần "nhớ" cảnh là đoán đúng — đó là đề thi đã lộ. Điểm test cao nhưng không phản ánh khả năng tổng quát hoá sang **cảnh mới**, là điều thật sự xảy ra khi xe chạy ngoài đường.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` = 12 ký tự đầu của sha256 trên danh sách `frame_id|split` đã sắp xếp. Vì là băm nội dung nên: cùng nội dung ⇒ cùng version ở mọi máy, mọi lúc (tái lập được); nội dung đổi dù một khung ⇒ version đổi (không thể có hai dataset khác nhau mang cùng tên). Ngày giờ hay số đếm thì đổi mỗi lần chạy dù dữ liệu y hệt, và không nói gì về nội dung.

Của tôi: `00f83d03d17d`. Chạy lại `cli export-dataset` → vẫn `00f83d03d17d`; `step all --reset` từ lake trống → vẫn `00f83d03d17d`. Thử đổi MSSV sang `2A202602383` → `ebcade044cbb` (train/val/test = 740/145/134), đặt lại MSSV thật → về `00f83d03d17d` (`report/run_logs/step4_mssv_change.txt`). Lý do: `gold_dataset_frames.sql` xếp trip bằng `md5(trip_id || LAB_STUDENT_ID)`, nên MSSV đổi thứ tự trip ⇒ đổi trip nào vào train/val/test ⇒ đổi cặp `frame_id|split` ⇒ đổi băm. Tổng số khung (1019) không đổi, chỉ cách chia đổi.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`lake/gold/datasets/00f83d03d17d/manifest.json` sau lần chạy cuối `step all --reset` (bản đầy đủ trong `report/run_logs/final_manifest.json`):

```json
"lineage": {
  "bronze.raw_telemetry_a": 0, "bronze.raw_telemetry_b": 0, "bronze.trips_raw": 0, "bronze.video_files": 1,
  "silver.frames": 25, "silver.telemetry": 0, "silver.frames_flagged": 0, "silver.frame_telemetry": 0,
  "input_video_sha256": {
    "drive_a": "636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa",
    "drive_b": "bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc" } }
```

Tức là: dataset được tạo từ hai video có đúng hai sha256 trên, telemetry bronze A/B ở version 0, bảng `silver.frames` ở **version 25** (26 commit 0..25, mỗi trip một commit overwrite theo partition), các bảng silver khác ở version 0. Đáng chú ý: ở bước 4 trước đó (sau thí nghiệm idempotent) lineage ghi `silver.frames: 27` (`report/run_logs/step4_manifest.json`) — khác version nhưng **cùng nội dung**, nên `dataset_version` (`00f83d03d17d`) và `manifest_sha256` (`de765ac5…`) giống hệt; theo luật `drop: is_black, is_blurry, is_near_dup, not sync_ok` và `split: per trip, stratified by drive, md5 order`; kết quả 1019 khung, `manifest_sha256 = de765ac5…`. Ba tháng sau, để tái tạo: dùng Delta time travel đọc đúng các version bảng trên, kiểm sha256 video gốc, dùng cùng code/luật (git commit, `dbt_project.yml` vars) và cùng `LAB_STUDENT_ID`; rồi so `dataset_version` và `manifest_sha256` để xác nhận trùng khớp.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Đặt `LAB_FAIL_ONCE=trip-05905`, trigger `day09_reset` (`reset_crash_1`, success) rồi `day09_video_pipeline` (`crash_run_1`). Kết quả (`report/run_logs/step5_crash_before_clear.txt`):
- **1 task đỏ**: `silver_frames` map index **16**, log: `RuntimeError: simulated crash while extracting trip-05905; run it again`;
- **25** task `silver_frames` của các trip khác vẫn **xanh** (mỗi trip một partition độc lập);
- **7 task `upstream_failed`**: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse` → dữ liệu dở dang không đi tới gold/warehouse.

Tôi chỉ clear task đỏ `silver_frames[16]` cùng các task phía sau đang `upstream_failed` (`--only-failed --downstream`), không chạy lại 25 trip đã xong. **8 task chạy lại** (1 + 7), run chuyển `success` với **37/37 task xanh** (`report/run_logs/step5_crash_after_clear.txt`).

Làm lại lần 2 (`crash_run_2`) vì log Airflow nằm trong container (không mount ra ngoài) nên đã mất khi container được tạo lại để gỡ `LAB_FAIL_ONCE` — một bài học vận hành: log của orchestrator cũng cần lưu bền. Lần 2 cho **đúng kết quả như lần 1** (1 đỏ ở map 16, 7 `upstream_failed`, 25 trip xanh); lần này clear bằng nút **Clear task** trong giao diện, hộp thoại chọn sẵn Downstream + Recursive và báo **Affected Tasks: 8**; run lại chuyển `success`, **37/37** (`report/run_logs/step5_crash_run2_ui_clear.txt`). Task map 16 có `attempt=1.log` (sập) và `attempt=2.log` (thành công).

Không nhân đôi (cả hai lần): `lake.silver_frames` có **60 dòng / 60 frame_id khác nhau** cho trip-05905 và tổng **1246 / 1246** — đúng như lần chạy sạch; `cli history silver.frames` cho thấy commit mới là `WRITE 60 dòng` với predicate `trip_id = 'trip-05905'` (overwrite theo partition). `scripts/lab.sh verify` vẫn **27/27**, và `dataset_version` vẫn **`00f83d03d17d`** — vì nội dung (tập `frame_id|split`) không đổi. Bằng chứng ảnh (`report/screenshots/`): `crash_red_log.png` (log lần thử 1, thấy `simulated crash while extracting trip-05905`), `crash_red_graph.png` (Graph lúc đỏ: `silver_frames` đỏ, 7 hộp cam `upstream_failed`), `crash_clear_dialog.png` (hộp thoại Clear, 8 task), `airflow_graph.png` (Graph xanh sau khi clear).

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Quy mô: 2 drive ≈ 10 phút video ra 1246 khung; 500 xe × vài giờ/ngày × 7 camera là hàng chục nghìn giờ video/ngày.
1. **Decode video bằng FFmpeg trong worker Airflow** (LocalExecutor, `PARALLELISM=2`, một container 2,5 GB): đây là nút cổ chai CPU. DAG sạch của tôi chạy 26 task `silver_frames` đã mất ~22 phút. Thay bằng cụm xử lý phân tán (Spark/Ray, hoặc KubernetesExecutor/Celery với autoscaling), decode bằng GPU (NVDEC), và đẩy việc lấy mẫu/khử trùng về gần nguồn (trên xe hoặc edge).
2. **DuckDB một file + dbt chạy trên một máy** (`/opt/work/lakehouse.duckdb`, một tiến trình ghi): không chia sẻ được, không song song được, bộ nhớ một node. Thay bằng engine lakehouse phân tán (Spark/Trino/Databricks SQL) đọc trực tiếp bảng Delta/Iceberg trên object storage; dbt vẫn giữ nhưng đổi adapter.
3. **Điều phối "một task cho mỗi trip" và lưu trữ một node MinIO/Postgres**: hàng trăm nghìn mapped task/ngày làm nghẽn scheduler và metadata DB của Airflow; một node MinIO không đủ băng thông/độ bền. Thay bằng ingest theo sự kiện/stream (Kafka, partition theo `vehicle_id/ngày`, mỗi task xử lý một lô), object storage có replication (S3/MinIO cluster), compaction định kỳ các file nhỏ của Delta, và warehouse phân tích chuyên dụng (BigQuery/ClickHouse) thay Postgres cho dashboard.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Câu 1 — **tỷ lệ khung bị loại theo từng trip** (card "[Câu hỏi riêng] Tỷ lệ khung bị loại theo từng trip"):

```sql
select drive_id, trip_id, avg_speed_kmh, frames, frames_kept, frames - frames_kept as dropped,
       round(100.0 * (frames - frames_kept) / frames, 1) as drop_pct, split
from marts.mart_trip_summary order by drop_pct desc
```

Kết quả: 6 trip bị loại nhiều nhất đều ở **drive_a** — `trip-04622` 56,7% (34/60), `trip-03904` 50,0%, `trip-02086` 46,7%, `trip-08888` 40,0% (xe 6,5 km/h), `trip-07774` 38,2%; trong khi 4 trip của drive_b không mất khung nào. Tra ngược ở silver (`silver.silver_frame_telemetry`): cả 5 trip đứng đầu bị loại **hoàn toàn do khung gần trùng** (trip-04622: 34 trùng, 0 đen, 0 mờ, 0 mất đồng bộ; trip-02086: 28; trip-08888: 24; trip-07774: 13; trip-03904: 11). Drive A chạy chậm trong phố và có đoạn camera đứng hình 6 giây, nên lấy mẫu 2 khung/giây sinh nhiều khung gần như giống nhau. Riêng trip-04622 có tốc độ trung bình 67 km/h nhưng vẫn mất 34/60 khung — tôi đoán trip này chứa đoạn dừng/đứng hình (tốc độ trung bình che mất đoạn đứng yên); đây là trip đáng xem lại ảnh trước khi tin vào ngưỡng dHash.

Câu 2 (điểm cộng, câu hỏi vận hành) — **tỷ lệ loại theo dải tốc độ**:

```sql
select case when avg_speed_kmh < 30 then '1: <30 km/h' when avg_speed_kmh < 60 then '2: 30-60 km/h' else '3: >=60 km/h' end as speed_band,
       count(*) as trips, sum(frames) as frames, round(100.0 * sum(frames - frames_kept) / sum(frames), 1) as drop_pct
from marts.mart_trip_summary group by 1 order by 1
```

| dải tốc độ | trip | khung | % bị loại |
|---|---|---|---|
| < 30 km/h | 6 | 256 | 29,3 |
| 30–60 km/h | 8 | 340 | 17,9 |
| ≥ 60 km/h | 12 | 650 | 14,0 |

Xe càng chậm càng mất nhiều khung (gấp đôi ở < 30 km/h so với ≥ 60 km/h) vì lấy mẫu 2 khung/giây theo thời gian làm khung liền nhau gần trùng. Hệ quả vận hành: dataset thiếu tình huống tốc độ thấp (nội đô, kẹt xe) — nên lấy mẫu theo quãng đường hoặc hạ ngưỡng dHash khi xe chậm.

## Nhìn lại

Điều tôi thấy bất ngờ nhất: chia ngẫu nhiên theo khung hình làm **cả 26/26 trip** rò rỉ sang test — không phải "một ít" như tôi nghĩ. Và `dataset_version` là băm nội dung nên chạy lại từ lake trống hay sau khi task sập vẫn ra đúng `00f83d03d17d`.
Phần vận hành khó hơn phần dữ liệu: ổ đĩa đầy làm Docker chuyển read‑only, containerd panic và đĩa ảo hỏng; Git trên Windows (`autocrlf=true`) đổi script sang CRLF làm container Airflow không khởi động được (`exec /airflow-entrypoint.sh: no such file or directory`) và làm sai sha256 của file được bảo vệ.
Chưa rõ: lineage ghi số version Delta (27 ở bước 4, 25 ở lần chạy cuối) nhưng hai lần đó có cùng `dataset_version` — vậy để tái tạo, số version chỉ có nghĩa trong đúng lake đó; sau một lần reset, "version 27" không còn tồn tại. Tôi chưa rõ cách làm lineage bền qua reset (có lẽ cần lưu thêm băm nội dung của từng bảng nguồn).
Lỗi dữ liệu pipeline **chưa** bắt: telemetry B lệch đồng hồ được trừ bằng hằng số khai báo trong `sources.json`; nếu độ lệch thực **trôi** theo thời gian (clock drift) hoặc khai báo sai, không test nào phát hiện — nên có test đối chiếu chuyển động trong video (optical flow) với tốc độ telemetry. Ngoài ra không có kiểm tra GPS nhảy vọt (vị trí bất khả thi giữa hai mẫu).
