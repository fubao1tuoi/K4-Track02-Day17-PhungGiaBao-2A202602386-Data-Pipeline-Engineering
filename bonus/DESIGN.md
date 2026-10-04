# Thiết kế pipeline dữ liệu cho trợ lý tra cứu pháp luật Việt Nam

## 1. Bài toán và ràng buộc thực tế

Tôi muốn xây một trợ lý giúp nhân viên pháp chế của doanh nghiệp Việt Nam tra cứu quy định và kiểm tra một chính sách nội bộ có còn phù hợp hay không. Người dùng không chỉ hỏi câu lookup như “Điều 12 nói gì?”, mà còn hỏi câu nhiều bước như “Quy định nào sửa đổi mức phạt này, có hiệu lực từ ngày nào và văn bản nào đang áp dụng cho doanh nghiệp tại Hà Nội?”. Nguồn dữ liệu gồm PDF từ cổng thông tin nhà nước, công báo, website bộ/ngành và tài liệu nội bộ. Chúng có PDF text, bản scan, bảng biểu, phụ lục, chữ ký, dấu, liên kết “sửa đổi/bãi bỏ/thay thế”, nhiều phiên bản và đôi khi cùng một văn bản được đăng lại với tên file khác nhau.

Ràng buộc quan trọng nhất là câu trả lời phải truy nguyên tới đúng điều, khoản, phiên bản và thời điểm hiệu lực. Độ trễ vài phút hay một ngày thường chấp nhận được, nhưng trả lời dựa trên văn bản hết hiệu lực thì không. Dữ liệu có tiếng Việt, lỗi OCR và thông tin nội bộ có thể chứa PII. Hệ thống không thay thế luật sư: nó phải trả nguồn, mức tin cậy và từ chối kết luận khi bằng chứng mâu thuẫn.

## 2. Kiến trúc đề xuất

```text
Cổng văn bản / Công báo / PDF nội bộ
                  |
                  v
        Ingest manifest + checksum
                  |
                  v
 Bronze bất biến: file gốc + metadata + thời điểm quan sát
                  |
          OCR / parse / chuẩn hoá
                  |
         +--------+---------+
         |                  |
         v                  v
 Silver document       Quarantine + cảnh báo
 điều/khoản/bảng        OCR/schema/PII lỗi
         |
         +------------------+
         |                  |
         v                  v
 Vector chunks       Knowledge graph
 + embeddings        sửa đổi/bãi bỏ/dẫn chiếu
         |                  |
         +--------+---------+
                  v
       Hybrid retrieval + reranker
                  |
                  v
 LLM trả lời có citation, thời điểm hiệu lực và confidence
                  |
                  v
 Trace + feedback -> eval set đã decontaminate
```

## 3. Năm quyết định then chốt

### 3.1. Batch hay streaming?

**Quyết định:** dùng batch tăng dần mỗi 4 giờ và một luồng ưu tiên theo webhook/RSS cho nguồn có thông báo văn bản mới. Tôi chọn batch-first thay vì streaming toàn bộ vì nguồn gốc chủ yếu là website/PDF, tần suất thay đổi thấp và SLA bốn giờ đủ cho người dùng pháp chế. Batch đơn giản hơn để replay, kiểm checksum và tái OCR. Luồng ưu tiên chỉ đưa URL vào cùng một code path ingest; nó không tạo pipeline thứ hai. Đánh đổi là văn bản khẩn cấp có thể chậm vài giờ, nhưng chi phí và độ phức tạp vận hành thấp hơn Kafka end-to-end. Với văn bản có thời điểm hiệu lực gấp, rule cảnh báo sẽ kích hoạt crawl ngay.

### 3.2. Dữ liệu bẩn và hợp đồng chất lượng được xử lý thế nào?

**Quyết định:** Bronze giữ nguyên binary, URL, SHA-256, `observed_at` và HTTP metadata; Silver chỉ nhận tài liệu vượt qua contract. Contract kiểm MIME thực, số trang, tỷ lệ ký tự OCR hợp lệ, ngôn ngữ, mã/số văn bản, ngày ban hành, cơ quan, cấu trúc điều/khoản và tính duy nhất của `(document_id, version)`. Bản lỗi đi vào quarantine chứ không làm dừng toàn batch. Tôi chọn quarantine thay vì “cố parse” vì một lỗi OCR im lặng có thể đổi số tiền hoặc điều luật, nguy hiểm hơn thiếu tạm một tài liệu. Đánh đổi là cần hàng đợi human review và SLA xử lý. Cảnh báo phát khi tỷ lệ quarantine theo nguồn vượt baseline hoặc trường bắt buộc bị thiếu đột biến.

### 3.3. RAG hay knowledge graph?

**Quyết định:** dùng hybrid. Vector retrieval phù hợp với câu hỏi diễn đạt tự nhiên và tìm đoạn tương tự; graph lưu quan hệ `AMENDS`, `REPEALS`, `CITES`, `EFFECTIVE_FROM`, `ISSUED_BY` để giải câu hỏi nhiều bước và lọc đúng phiên bản. Query planner chạy metadata/time filter trước, sau đó vector search, rồi graph expansion tối đa hai hop và rerank bằng bằng chứng. Tôi không chọn graph-only vì trích xuất quan hệ từ toàn bộ văn bản tốn công và graph kém với câu hỏi ngữ nghĩa mở. Tôi cũng không chọn vector-only vì hai chunk riêng biệt khó chứng minh chuỗi “A sửa B, B thay C”. Đánh đổi của hybrid là ingestion phức tạp hơn và cần kiểm consistency giữa chunk store với graph, nhưng phù hợp yêu cầu pháp lý cần citation và quan hệ phiên bản.

### 3.4. Làm sao bảo đảm point-in-time và chạy lại an toàn?

**Quyết định:** mọi entity có `valid_from`, `valid_to`, `observed_at`, `source_version` và content hash. Truy vấn nhận tham số “as of”; training/evaluation chỉ dùng tài liệu đã được quan sát trước thời điểm câu hỏi. Ingest là idempotent theo `(source_url, content_hash)`, Silver MERGE theo `document_id/version`, còn index Gold được xây theo generation rồi atomically đổi alias. Tôi chọn generation + alias thay vì cập nhật vector index tại chỗ vì backfill lỗi có thể rollback nhanh, người dùng không nhìn thấy index nửa cũ nửa mới. Đánh đổi là cần gấp đôi dung lượng trong lúc rebuild. Các side effect như gửi thông báo được ghi outbox với idempotency key, không gọi trực tiếp trong transform.

### 3.5. Chi phí lớn nhất và cách kiểm soát là gì?

**Quyết định:** OCR và LLM extraction chỉ chạy khi content hash hoặc extractor version thay đổi; embedding cache có khóa `hash(chunk)+model_version`; tài liệu text-native không qua OCR. Trước mỗi backfill, pipeline ước lượng số trang, token và chi phí rồi yêu cầu phê duyệt nếu vượt ngân sách. Tôi chọn model nhỏ có schema validation cho extraction ban đầu, chỉ chuyển trang khó sang model lớn. Đánh đổi là routing làm pipeline phức tạp và có thể bỏ sót một số trang khó, nên cần lấy mẫu audit ngẫu nhiên. Metric vận hành gồm chi phí trên tài liệu hợp lệ, cache-hit rate, OCR word error rate và số token trên câu trả lời.

## 4. PII, flywheel và cách đo chất lượng

Tài liệu nội bộ được phân loại trước indexing. Email, số điện thoại, căn cước và tên cá nhân được phát hiện bằng regex kết hợp NER tiếng Việt; bản raw được mã hoá và giới hạn quyền, còn index chỉ nhận nội dung đã mask hoặc tài liệu được phép. Chốt PII nằm ở Bronze→Silver để downstream không nhân bản dữ liệu nhạy cảm. Tôi đo precision/recall trên tập gán nhãn theo từng loại PII, ưu tiên false-negative rate, đồng thời scan định kỳ vector chunks và graph properties.

Trace truy vấn, citation được mở và feedback của chuyên viên tạo flywheel. Tuy nhiên, tôi không đưa mọi feedback thẳng vào training. Dữ liệu phải loại PII, dedup theo semantic hash, tách người dùng/tổ chức giữa train và eval, và decontaminate prompt gần giống eval. Tập đánh giá cố định đo citation precision, recall của văn bản đang hiệu lực, tỷ lệ trả lời đúng thời điểm, groundedness và tỷ lệ từ chối hợp lý. Feedback chỉ là tín hiệu chọn mẫu; nhãn cuối cho câu hỏi rủi ro cao cần chuyên viên pháp chế duyệt.

## 5. Phương án bị loại

Tôi loại phương án “đưa tất cả PDF vào một vector database rồi để LLM tự tổng hợp” dù đây là cách ra prototype nhanh nhất. Nó không biểu diễn rõ quan hệ sửa đổi/bãi bỏ, khó lọc point-in-time, dễ trộn văn bản hết hiệu lực và không tạo được bằng chứng nhiều bước đáng tin cậy. Tôi cũng không chọn Spark ở giai đoạn đầu: dung lượng dự kiến vài chục nghìn văn bản phù hợp object storage + DuckDB/dbt và worker OCR song song. Chỉ khi số trang hoặc SLA tăng khoảng 100 lần, profiling chứng minh parse/OCR hoặc shuffle metadata là bottleneck, tôi mới chuyển riêng stage cần scale sang hệ phân tán thay vì thay toàn bộ kiến trúc.

## 6. Tiêu chí thành công và lộ trình

MVP thành công khi 100% câu trả lời có citation tới đúng điều/khoản, không sử dụng văn bản hết hiệu lực trong bộ test point-in-time, citation precision đạt ít nhất 95%, recall tài liệu liên quan đạt ít nhất 90%, và không phát hiện PII mức nghiêm trọng trong Gold. Giai đoạn một ingest 1.000 văn bản và xây eval 200 câu đã được chuyên viên duyệt. Giai đoạn hai bổ sung graph cho quan hệ sửa đổi/bãi bỏ. Giai đoạn ba mới mở flywheel có kiểm soát và tối ưu chi phí. Cách đi này ưu tiên độ đúng, khả năng audit và quyền riêng tư trước tốc độ mở rộng tính năng.
