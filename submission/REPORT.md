# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** `[Phùng Gia Bảo` / `2A202602386`

**Repo:** https://github.com/fubao1tuoi/K4-Track02-Day17-Data-Pipeline-Engineering

**Commit bài nộp:** `[ĐIỀN SHA COMMIT CUỐI SAU KHI COMMIT]`

**AI đã dùng và phạm vi hỗ trợ:** OpenAI Codex; hỗ trợ đọc đề/repo, chẩn đoán ba lỗi, đề xuất và thực hiện thay đổi trong `pipeline/`, giải thích lệnh PowerShell, chạy và đối chiếu các kiểm tra. Tôi đã review diff và chạy lại toàn bộ bằng chứng.
**Nguồn tham khảo khác:** README, rubric, checkpoints và mã nguồn/test đi kèm trong repo.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | Contract báo nhiều hàng cho cùng `ticket_id`; T-91 có cả trạng thái cũ và mới thay vì chỉ `high/closed/bug`. | Checksum `gold_feature_daily` lệch full recompute; event u05 xảy ra 12/08 nhưng đến 15/08 chưa được tính vào ngày 12/08. | T-97 vẫn còn dữ liệu cá nhân trong Silver, training snapshot mới nhất và RAG chunks sau ngày xoá. |
| **Nguyên nhân gốc** | `upsert_silver_tickets` chỉ dedup trong batch rồi `INSERT`, không upsert giữa các batch và không chặn replay cũ. | `LOOKBACK_DAYS=0` chỉ ghi lại partition ngày ingest, không bao phủ P99 lateness ba ngày theo event time. | Parser chỉ lấy `ticket_id` từ `after`; với `op='d'`, `after=null` nên delete bị lọc bỏ. |
| **Cách sửa** | `pipeline/silver.py`: `MERGE` theo `ticket_id`; chỉ update khi `s._lsn > t._lsn`. | `pipeline/config.py`: đặt `LOOKBACK_DAYS=3`; Gold overwrite các partition event-date trong cửa sổ. | `pipeline/staging.py`: lấy khoá bằng `coalesce(after.ticket_id, before.ticket_id, key.ticket_id)`; Silver giữ tombstone với PII null. |
| **Khái niệm trên slide** | Silver có khoá, keyed upsert, idempotency và CDC ordering/LSN. | Event time khác ingest time; đo P99 và recompute lookback partition. | Debezium delete khác Kafka tombstone; xoá phải lan xuống dữ liệu phục vụ. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`.
- `submission/checksums.txt`: **PASS** — Gold combined checksum: `39e115c510ecdf526800eac227158a4f`.
- `python -m scripts.parity`: **PARITY** cho `silver_tickets` và `gold_feature_daily`.

## 3. Lựa chọn công cụ / kỹ thuật

- Dùng MERGE theo khoá cho `silver_tickets` vì đây là bảng trạng thái một hàng/thực thể; dùng overwrite-partition cho `gold_feature_daily` vì aggregate theo ngày cần được tính lại trọn partition khi có late data.
- Giữ tombstone thay vì hard delete để bảo toàn LSN và dấu vết xoá, nhờ đó replay thay đổi cũ không làm ticket sống lại.
- Dựng snapshot training từ Bronze “as of” ngày đó và không sửa snapshot cũ để bảo đảm tái lập thí nghiệm, versioning và point-in-time correctness.
- Chọn DuckDB/dbt vì dữ liệu lab nhỏ, chạy local và cần SQL/contract/incremental rõ ràng; Spark sẽ tăng chi phí vận hành mà không đem lại lợi ích scale cần thiết.

## 4. Hai câu hỏi suy ngẫm

1. Trong production, quyền xoá PII phải ưu tiên hơn tính bất biến vật lý. Tôi giữ bất biến ở mức **logical/version metadata**, nhưng mã hoá dữ liệu nhạy cảm bằng khoá theo chủ thể để có thể crypto-shred, hoặc chạy quy trình purge có audit để viết lại/xoá snapshot và downstream index. Sau đó thu hồi artifact/model bị ảnh hưởng nếu cần, lưu tombstone không chứa PII, kiểm tra mọi bản sao/cache và ghi biên bản xoá. Như vậy vẫn tái lập được lineage/cấu hình, nhưng không giữ văn bản cá nhân trái yêu cầu.
2. Tôi đặt chốt PII ngay ranh giới Bronze→Silver trước khi dữ liệu được nhân sang training/RAG; Bronze raw phải mã hoá, giới hạn quyền và retention. Ngoài regex email/số điện thoại, dùng NER tiếng Việt hoặc DLP để phát hiện tên/địa chỉ/định danh, kết hợp allowlist và human review cho mẫu rủi ro cao. Đo bằng bộ test gán nhãn với precision/recall theo từng loại PII, false-negative rate là SLO chính, scan định kỳ mọi bảng/chunk và cảnh báo khi số dòng quarantine hoặc leakage tăng.

## 5. Output thực tế

```text
$ python -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ python -m pytest
..................................                                       [100%]
34 passed in 2.36s

$ python -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ python main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ dbt build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
Running with dbt=1.12.5
Registered adapter: duckdb=1.11.0
Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.57 seconds.
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ python -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
