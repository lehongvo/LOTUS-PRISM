# Phase 3: Data Processing Implementation Details

## Overview
Phase 3 triển khai các pipeline xử lý dữ liệu cho hệ thống LOTUS-PRISM, bao gồm cả xử lý batch và xử lý real-time.

## 1. Batch Processing Pipeline

### Components:
- **ETL Jobs (PySpark)**: Các job chạy định kỳ để xử lý dữ liệu từ Bronze đến Silver và Gold layer
- **Data Cleaning & Transformation**: Loại bỏ dữ liệu không hợp lệ, chuyển đổi định dạng, chuẩn hóa dữ liệu
- **Product Categorization**: Phân loại sản phẩm theo danh mục, thương hiệu, đặc tính
- **Price Normalization**: Chuẩn hóa giá theo đơn vị, khối lượng, kích thước để so sánh chính xác
- **RFM Analysis**: Phân tích Recency, Frequency, Monetary để phân loại khách hàng
- **Delta Lake**: Sử dụng Delta Lake tables cho từng layer (Bronze, Silver, Gold)
- **Data Validation**: Kiểm tra chất lượng dữ liệu, phát hiện anomalies, xác định tính đầy đủ

### Workflow:
1. **Bronze Layer**: Dữ liệu thô từ các nguồn (web scraping, internal APIs, market trends)
2. **Silver Layer**: Dữ liệu đã được làm sạch, chuẩn hóa và chuyển đổi
3. **Gold Layer**: Dữ liệu cho phân tích và tiêu thụ bởi các ứng dụng

### Thực hiện:
- Các batch job sẽ được triển khai bằng PySpark trên Databricks
- Schedule chạy batch job qua Databricks Jobs hoặc Azure Data Factory
- Monitoring và logging sử dụng Databricks monitoring tools

## 2. Real-time Processing Pipeline

### Components:
- **Kafka/Event Hubs**: Hệ thống message queue cho dữ liệu real-time
- **Producer Simulators**: Mô phỏng các sự kiện thay đổi giá real-time
- **Spark Streaming Jobs**: Xử lý dữ liệu streaming từ Kafka/Event Hubs
- **Real-time Aggregation**: Tính toán và cập nhật các metrics theo thời gian thực
- **Notification System**: Gửi thông báo khi phát hiện thay đổi giá đáng kể

### Data Flow:
1. Producer gửi dữ liệu thay đổi giá vào Kafka/Event Hubs
2. Spark Streaming job tiêu thụ và xử lý dữ liệu từ Kafka/Event Hubs
3. Dữ liệu được phân tích để phát hiện biến động giá đáng kể
4. Thông báo được gửi nếu phát hiện thay đổi quan trọng
5. Dữ liệu được lưu vào Delta Lake cho phân tích sau này

### Thực hiện:
- Sử dụng Spark Structured Streaming trên Databricks
- Kết nối với Azure Event Hubs hoặc Kafka cluster
- Checkpointing để đảm bảo exactly-once processing
- Windowed operations cho các calculations theo thời gian

## 3. Công nghệ sử dụng

- **PySpark**: Framework xử lý dữ liệu phân tán
- **Delta Lake**: Storage layer cho data lake với ACID transactions
- **Azure Databricks**: Nền tảng cho phân tích dữ liệu và ML
- **Kafka/Azure Event Hubs**: Message queuing cho real-time data
- **Azure Data Lake Storage Gen2**: Lưu trữ dữ liệu
- **Azure Synapse Analytics**: Data warehouse cho phân tích

## 4. Monitoring và Validation

- **Data Quality Checks**: Kiểm tra tính đầy đủ, tính nhất quán, và khoảng giá trị hợp lệ
- **Processing Metrics**: Theo dõi thời gian xử lý, throughput, latency
- **Error Handling**: Xử lý lỗi và retry mechanisms
- **Alerting**: Cảnh báo khi phát hiện vấn đề về chất lượng dữ liệu hoặc hiệu suất

## 5. Kết quả đầu ra

- **Bronze-Silver-Gold Data**: Dữ liệu được lưu trong Delta Lake tables theo các layer
- **Real-time Metrics**: Các chỉ số theo thời gian thực về giá cả và so sánh với đối thủ
- **Notification Events**: Thông báo về những thay đổi giá quan trọng
- **Analytical Datasets**: Dữ liệu được chuẩn bị cho phân tích và visualizations
