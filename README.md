# Device WebSocket Service

Go микросервис для управления IoT устройствами через WebSocket + MQTT. Интегрируется с gRPC AccessService для проверки прав доступа.

## 🏗️ Архитектура

### Компоненты
- **WebSocket Server** - единый endpoint `/ws` для команд и подписок
- **MQTT Client** - связь с устройствами через mosquitto
- **Redis** - кэширование запросов и подписок
- **gRPC Client** - проверка прав доступа через AccessService

### Потоки данных

#### Write Flow
```
User -> /ws command -> AccessService.Check(WRITE) -> Redis store -> MQTT publish
                                                                   ↓
                                              Device response -> Redis match -> User
```

#### Read Flow  
```
User -> /ws subscribe -> AccessService.Check(READ) -> Redis subscribe
                                                              ↓
                                     MQTT event -> Check subscriptions -> broadcast to users
                                                              ↓
                                                    Redis Stream (analytics -> ClickHouse)
```

## 🚀 Быстрый старт

### Требования
- Go 1.25+
- Docker & Docker Compose
- Protocol Buffers compiler

### Локальная разработка

1. **Установка зависимостей**
```bash
go mod download
```

2. **Генерация proto кода**
```bash
protoc --go_out=. --go-grpc_out=. api/proto/access.proto
```

3. **Запуск с Docker Compose**
```bash
# Поднимает Redis + Mosquitto + Device WebSocket Service
docker-compose up -d

# Просмотр логов
docker-compose logs -f device-ws

# Остановка
docker-compose down
```

4. **Запуск локально** (нужны Redis и Mosquitto)
```bash
# Запустить Redis
redis-server

# Запустить Mosquitto
mosquitto -c mosquitto/config/mosquitto.conf

# Запустить сервис (нужен AccessService на :9090)
go run ./cmd/device-ws
```

## 📋 API

### /ws - Отправка команды

**Client Message**
```json
{
  "type": "command",
  "request_id": "uuid",
  "consumer_id": "device-1",
  "contract_name": "relay.set",
  "payload": {
    "state": true
  }
}
```

**Headers**
```
X-Client-ID: user-uuid (set by Gateway)
```

**Success Response**
```json
{
  "type": "success",
  "request_id": "uuid",
  "consumer_id": "device-1",
  "contract_name": "relay.set",
  "status": "ok",
  "payload": {
    "state": true
  },
  "ts": "2026-05-24T12:00:01Z"
}
```

**Error Response**
```json
{
  "type": "error",
  "request_id": "uuid",
  "code": "ACCESS_DENIED",
  "message": "Access denied"
}
```

### /ws - Подписка на события

**Client Message**
```json
{
  "type": "subscribe",
  "consumer_id": "device-1",
  "contracts": [
    "temperature",
    "humidity",
    "battery.level"
  ]
}
```

**Subscription Response**
```json
{
  "type": "subscription",
  "consumer_id": "device-1",
  "status": "subscribed",
  "accepted": ["temperature", "humidity"],
  "denied": ["battery.level"]
}
```

**Unsubscribe Message**
```json
{
  "type": "unsubscribe",
  "consumer_id": "device-1",
  "contracts": ["humidity"]
}
```

**Event Response** (streamed)
```json
{
  "type": "event",
  "consumer_id": "device-1",
  "contract_name": "temperature",
  "payload": {
    "value": 23.5
  },
  "ts": "2026-05-24T12:00:00Z"
}
```

**Error Response**
```json
{
  "type": "error",
  "code": "ACCESS_DENIED",
  "message": "Access denied to requested contracts"
}
```

## 🔑 Error Codes

| Code | Описание |
|------|----------|
| `ACCESS_DENIED` | Клиент не имеет прав на операцию |
| `INVALID_MESSAGE` | Некорректный формат сообщения |
| `INVALID_CONTRACT` | Неизвестный контракт |
| `MQTT_PUBLISH_FAILED` | Ошибка публикации в MQTT |
| `TIMEOUT` | Таймаут ответа от устройства (30s) |
| `INTERNAL_ERROR` | Внутренняя ошибка сервера |

## 🔗 MQTT Topics

### Commands (QoS 1)
```
devices/{consumer_id}/commands
```

Payload:
```json
{
  "request_id": "uuid",
  "consumer_id": "device-1",
  "contract_name": "relay.set",
  "payload": {...},
  "ts": "2026-05-24T12:00:00Z"
}
```

### Responses (QoS 1)
```
devices/{consumer_id}/responses
```

Payload:
```json
{
  "request_id": "uuid",
  "consumer_id": "device-1",
  "contract_name": "relay.set",
  "status": "ok",
  "payload": {...},
  "ts": "2026-05-24T12:00:01Z"
}
```

### Events (QoS 1)
```
devices/{consumer_id}/events
```

Payload:
```json
{
  "consumer_id": "device-1",
  "contract_name": "temperature",
  "payload": {"value": 23.5},
  "ts": "2026-05-24T12:00:00Z"
}
```

### Redis Analytics Stream
```
analytics.events
```
(durable stream для analytics service -> ClickHouse)

## 💾 Redis Keys

```
ws:req:{request_id}              # Request metadata (TTL 30s)
ws:subs:{consumer_id}:{contract} # Set of connected users
ws:conn:{connection_id}          # Connection metadata (TTL 1h)
```

## ⚙️ Configuration

Через переменные окружения:

```bash
HTTP_ADDR=:8080                           # HTTP listen address
REDIS_ADDR=localhost:6379                # Redis connection
ANALYTICS_STREAM=analytics.events        # Redis Stream for analytics
MQTT_BROKER=tcp://localhost:1883         # MQTT broker
MQTT_USERNAME=contacts-service           # service MQTT username for broker ACL
MQTT_PASSWORD=                           # optional
ACCESS_SERVICE_ADDR=localhost:9090        # gRPC AccessService
```

## 📊 Mocking

Для тестирования без реальных устройств:

1. **Mock MQTT Device** - pub на `devices/{id}/responses` с `request_id` из `devices/{id}/commands`
2. **Mock Event Producer** - pub на `devices/{id}/events` с тестовыми данными
3. **Mock AccessService** - используйте existing domain service на localhost:9090

## 🔧 Troubleshooting

### Redis connection refused
```bash
docker-compose logs redis
```

### MQTT connection issues
```bash
docker-compose logs mosquitto
# Проверить конфиг: mosquitto/config/mosquitto.conf
```

### gRPC AccessService unreachable
```bash
# Убедиться что domain service запущен на :50051
# Если на macOS в Docker, используйте host.docker.internal вместо localhost
```

### Таймауты ответов
- Увеличить TTL в RequestManager (текущий: 30s)
- Проверить что устройства публикуют на правильные топики
- Проверить MQTT QoS сетинги

## 🧪 Testing

```bash
# Unit tests
go test ./...

# Build Docker image
docker build -t device-ws:latest .

# Run tests in Docker
docker run --rm device-ws:latest go test ./...
```

## 📝 Future Improvements

- [ ] Metrics (Prometheus)
- [ ] Tracing (Jaeger)
- [ ] Circuit breaker for AccessService
- [ ] Connection pooling optimization
- [ ] Persistent request queue (for production)
- [ ] Load testing
- [ ] Multi-instance coordination with Redis
