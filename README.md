# OCM Workers - Real-time Data Sync Service

Workers service độc lập để đồng bộ dữ liệu realtime từ RabbitMQ vào ClickHouse Data Warehouse.

## 🏗️ Kiến trúc

```
RabbitMQ Queues                Workers                  ClickHouse
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

dwh.orders.v2      ──────►   OrderWorker      ──────►   Fact_Order_Delta

dwh.line_item      ──────►   LineItemWorker   ──────►   Fact_Line_Item_Delta
```

## ✨ Tính năng

- ✅ 2 workers độc lập xử lý order events và line item events
- ✅ Hoàn toàn tách biệt khỏi ocm_be (không import từ ocm_be)
- ✅ Kết nối đến Postgres để query dữ liệu
- ✅ Kết nối đến ClickHouse để insert delta
- ✅ Tự động retry khi query hoặc insert thất bại
- ✅ Prefetch control để tối ưu performance
- ✅ Graceful shutdown
- ✅ Health check
- ✅ Docker support

## 📁 Cấu trúc thư mục

```
workers/
├── cmd/
│   └── main.go                 # Entry point
├── config/
│   └── config.go               # Configuration management
├── models/
│   └── event.go                # Data models
├── internal/
│   ├── clickhouse/
│   │   └── client.go          # ClickHouse client
│   ├── postgres/
│   │   └── client.go          # Postgres client
│   ├── rabbitmq/
│   │   └── consumer.go        # RabbitMQ consumer
│   └── workers/
│       ├── order_worker.go    # Order worker logic
│       └── line_item_worker.go # Line item worker logic
├── pkg/
│   └── logger/
│       └── logger.go          # Logger utilities
├── .env.example               # Environment variables template
├── docker-compose.yml         # Docker Compose config
├── Dockerfile                 # Docker build config
├── go.mod                     # Go module dependencies
└── README.md                  # This file
```

## 🚀 Yêu cầu

- Go 1.23+
- RabbitMQ
- ClickHouse
- Postgres
- Docker & Docker Compose (optional)

## ⚙️ Cấu hình

### 1. Tạo file .env

```bash
cp .env.example .env
```

### 2. Chỉnh sửa các thông số kết nối

```env
# RabbitMQ
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
RABBITMQ_ORDER_QUEUE=dwh.orders.v2
RABBITMQ_LINE_ITEM_QUEUE=dwh.line_item

# ClickHouse
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_DATABASE=ocm_dev

# Postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=ocm_dev
POSTGRES_USERNAME=postgres
POSTGRES_PASSWORD=postgres
```

## 🏃 Chạy service

### Cách 1: Chạy trực tiếp với Go

```bash
# Install dependencies
go mod download

# Run
go run cmd/main.go
```

### Cách 2: Build và chạy binary

```bash
# Build
go build -o workers cmd/main.go

# Run
./workers
```

### Cách 3: Chạy với Docker Compose (Khuyến nghị)

```bash
# Build và chạy tất cả services
docker-compose up -d

# Chỉ chạy workers (nếu RabbitMQ, ClickHouse, Postgres đã có)
docker-compose up -d workers

# Xem logs
docker-compose logs -f workers

# Stop
docker-compose down
```

## 🔍 Kiểm tra hoạt động

### Kiểm tra workers đang chạy

```bash
docker-compose ps
```

### Kiểm tra logs

```bash
# Realtime logs
docker-compose logs -f workers

# Last 100 lines
docker-compose logs --tail=100 workers
```

### Test gửi message

```bash
# Vào RabbitMQ Management UI
open http://localhost:15672
# Login: guest/guest

# Hoặc dùng CLI để publish test message
docker-compose exec rabbitmq rabbitmqadmin publish \
  routing_key=dwh.orders.v2 \
  payload='{"event":"created","order_id":123,"store_id":1}'
```

### Kiểm tra dữ liệu trong ClickHouse

```bash
docker-compose exec clickhouse clickhouse-client

# Check data
SELECT * FROM ocm_dev.Fact_Order_Delta ORDER BY _updated_at DESC LIMIT 10;
SELECT * FROM ocm_dev.Fact_Line_Item_Delta ORDER BY _updated_at DESC LIMIT 10;
```

### Kiểm tra kết nối Postgres

```bash
docker-compose exec postgres psql -U postgres -d ocm_dev

# Check tables
\dt
SELECT count(*) FROM orders;
```

## 🐛 Troubleshooting

### Workers không kết nối được RabbitMQ

```bash
# Kiểm tra RabbitMQ đang chạy
docker-compose ps rabbitmq

# Kiểm tra network
docker-compose exec workers ping rabbitmq

# Check logs
docker-compose logs rabbitmq
```

### Workers không query được từ Postgres

```bash
# Kiểm tra Postgres
docker-compose exec postgres psql -U postgres -d ocm_dev -c "SELECT 1"

# Check connection từ workers
docker-compose exec workers sh
# Inside container:
env | grep POSTGRES
```

### Workers không ghi được vào ClickHouse

```bash
# Kiểm tra ClickHouse
docker-compose exec clickhouse clickhouse-client --query "SELECT 1"

# Kiểm tra database exists
docker-compose exec clickhouse clickhouse-client --query "SHOW DATABASES"

# Check tables
docker-compose exec clickhouse clickhouse-client --query "SHOW TABLES FROM ocm_dev"
```

## 📊 Monitoring

### Metrics cần theo dõi

- Message processing rate (messages/second)
- Failed messages count
- Queue length (RabbitMQ)
- ClickHouse insert latency
- Postgres query latency
- Connection status
- Worker uptime

## 🔧 Development

### Thêm worker mới

1. Tạo model mới trong `models/event.go`
2. Tạo worker mới trong `internal/workers/`
3. Thêm consumer trong `cmd/main.go`
4. Update `docker-compose.yml` nếu cần

### Testing

```bash
# Run tests
go test ./...

# With coverage
go test -cover ./...
```

## 📝 License

MIT
