# Kafka Ecommerce Pipeline

Pipeline xử lý dữ liệu sự kiện ecommerce theo thời gian thực: sinh dữ liệu giả lập → đẩy qua Kafka → lọc dữ liệu (Silver) → lưu vào Postgres → tạo view phân tích (Gold) → trực quan hóa bằng Metabase.

## Kiến trúc
- **producer.py**: sinh sự kiện ecommerce giả lập (PAGE_VIEW, ADD_TO_CART, PURCHASE) với ~25% dữ liệu lỗi có chủ đích, gửi vào topic `raw_events`.
- **clean_stream.py**: đọc từ `raw_events`, lọc bỏ sự kiện không hợp lệ (thiếu customer_id, sai event_type, amount âm, thiếu currency), forward dữ liệu sạch sang topic `clean_events`.
- **postgres_consumer.py**: đọc từ `clean_events`, insert theo batch (10 record/lần) vào bảng `kafka_events_silver` trong Postgres.
- **init-db/create_table_and_view.sql**: tự động chạy khi Postgres khởi tạo lần đầu, tạo sẵn bảng silver và 2 view gold.
- **Metabase**: kết nối tới Postgres để dựng dashboard trên các view gold.

## Cài đặt

**1. Clone repo và cài dependency:**

```bash
git clone https://github.com/nampham171105-cyber/kafka-ecommerce-pipeline.git
cd kafka-ecommerce-pipeline
pip install -r requirements.txt
```

**2. Khởi động hạ tầng (Kafka, Postgres, Metabase):**

```bash
docker compose up -d
```

Lần khởi tạo đầu tiên, Postgres sẽ tự động chạy `init-db/create_table_and_view.sql` để tạo bảng `kafka_events_silver` và 2 view `daily_customer_revenue`, `event_funnel`. Không cần chạy SQL thủ công.

> Nếu muốn Postgres init lại từ đầu (ví dụ sau khi sửa file SQL), phải xóa volume cũ trước: `docker compose down -v`.

**3. Chạy 3 script theo thứ tự, mỗi script một terminal riêng:**

```bash
python producer.py
```
```bash
python clean_stream.py
```
```bash
python postgres_consumer.py
```

**4. Xem kết quả trên Metabase:**

Mở `http://localhost:3000`, chọn **PostgreSQL** với thông tin:

| Field | Value |
|---|---|
| Host | `postgres` |
| Port | `5432` |
| Database name | `kafka_db` |
| Username | `postgres` |
| Password | `postgres` |

Sau khi kết nối, có thể dựng dashboard trực tiếp trên 2 view:
- `daily_customer_revenue`: doanh thu và số lượt mua theo từng khách hàng, theo ngày.
- `event_funnel`: phễu chuyển đổi page_view → add_to_cart → purchase theo ngày.

## Cấu trúc thư mục

```
.
├── docker-compose.yml
├── producer.py              # sinh dữ liệu raw, gửi vào topic raw_events
├── clean_stream.py          # lọc dữ liệu hợp lệ, đẩy vào topic clean_events
├── postgres_consumer.py     # ghi dữ liệu sạch vào Postgres
├── requirements.txt
└── init-db/
    └── create_table_and_view.sql   # tự chạy khi Postgres khởi tạo lần đầu
```

## Ghi chú

- Dữ liệu Kafka lưu ở `/tmp/kraft-combined-logs` trong container nên sẽ mất khi `docker compose down`.
- Dữ liệu Postgres được lưu bền qua volume `pgdata`, chỉ mất khi chạy `docker compose down -v`.

