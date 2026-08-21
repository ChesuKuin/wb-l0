# WB L0 — сервис заказов (Go + Kafka + PostgreSQL)

Микросервис получает заказы из Kafka, сохраняет в PostgreSQL, кэширует
в памяти и отдаёт данные по HTTP + через простую веб-страницу поиска
по `order_uid`.

## Стек

- Go (net/http, encoding/json, sync)
- PostgreSQL (jackc/pgx)
- Kafka (segmentio/kafka-go)
- Docker Compose (поднимает Postgres и Kafka)

## Структура проекта

```
cmd/service/    — главный сервис (HTTP-сервер + Kafka consumer)
cmd/producer/   — эмулятор отправки заказов в Kafka (для тестирования)
internal/models — структуры заказа (соответствуют model.json из задания)
internal/db     — работа с PostgreSQL: транзакции, upsert, чтение
internal/cache  — потокобезопасный кэш в памяти (map + sync.RWMutex)
internal/kafka  — consumer: чтение сообщений, валидация, сохранение, коммит offset
migrations/     — SQL-схема БД (применяется автоматически при первом старте Postgres)
web/            — HTML-страница для поиска заказа по ID
data/model.json — пример заказа из задания, используется producer'ом по умолчанию
```

## Запуск

### 1. Поднять инфраструктуру (PostgreSQL + Kafka)

```bash
docker compose up -d
```

Подождать 15–20 секунд, пока оба контейнера полностью стартуют.
Таблицы (`orders`, `deliveries`, `payments`, `items`) создаются автоматически
из `migrations/init.sql` при первом запуске Postgres.

Проверить статус:
```bash
docker compose ps
```

### 2. Установить зависимости Go

```bash
go mod tidy
```

### 3. Запустить сервис

```bash
go run ./cmd/service
```

Ожидаемый вывод:
```
[service] подключение к Postgres установлено
[service] кэш восстановлен из БД, заказов: 0
[kafka] consumer запущен, топик=orders, брокеры=[localhost:9092]
[service] HTTP сервер слушает на :8081
```

Топик `orders` в Kafka создаётся автоматически при первом сообщении
(включено `KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true` в docker-compose.yml).

### 4. Отправить тестовый заказ (в отдельном терминале)

```bash
go run ./cmd/producer -file ./data/model.json
```

Выведет сгенерированный `order_uid`. В логах сервиса появится:
```
[kafka] заказ <uid> сохранён и закэширован
```

Можно запускать повторно — каждый раз генерируется новый `order_uid`,
так что дубли не создаются.

Проверка обработки невалидных сообщений (сервис не должен падать):
```bash
go run ./cmd/producer -bad
```

### 5. Проверить HTTP API

```bash
curl http://localhost:8081/order/<order_uid>
```
Возвращает заказ в формате JSON. Заголовок `X-Cache: HIT` означает, что
данные отданы из кэша, `X-Cache: MISS` — что подняты из БД (и после этого
попали в кэш).

### 6. Веб-интерфейс

Открыть в браузере:
```
http://localhost:8081/
```
Ввести `order_uid`, нажать «Найти».

### 7. Проверка восстановления кэша при рестарте

1. Остановить сервис (Ctrl+C)
2. Запустить снова: `go run ./cmd/service`
3. В логах: `кэш восстановлен из БД, заказов: N` (N > 0 — заказы,
   отправленные ранее, найдутся сразу, без повторной отправки в Kafka)

## Обработка ошибок и надёжность

- Невалидный JSON из Kafka — логируется и пропускается, сервис не падает
  (`internal/kafka/consumer.go`)
- Запись в БД происходит в транзакции (`internal/db/db.go`, `SaveOrder`) —
  либо все 4 таблицы (orders/deliveries/payments/items) записываются
  успешно, либо ни одна (rollback)
- Offset в Kafka коммитится только **после** успешной записи в БД —
  при сбое БД сообщение не считается обработанным и будет прочитано повторно
- Upsert (`ON CONFLICT DO UPDATE`) на уровне БД защищает от дублей при
  повторной обработке одного и того же order_uid
- Кэш защищён `sync.RWMutex` от гонок при параллельных запросах

## Переменные окружения (опционально, есть дефолты)

| Переменная      | По умолчанию                                                        |
|-----------------|----------------------------------------------------------------------|
| DATABASE_DSN    | postgres://wbuser:wbpass@localhost:5432/wb_orders?sslmode=disable    |
| KAFKA_BROKERS   | localhost:9092                                                       |
| KAFKA_TOPIC     | orders                                                               |
| KAFKA_GROUP     | orders-service                                                       |
| HTTP_ADDR       | :8081                                                                |

## Альтернативный запуск через VS Code (без набора команд)

В `.vscode/launch.json` есть три готовые конфигурации запуска
(вкладка "Run and Debug" → выбрать из списка → F5):
- **Запустить сервис**
- **Отправить тестовый заказ (producer)**
- **Отправить битое сообщение (producer -bad)**

Требуется расширение **Go** для VS Code. Для Docker Compose можно также
использовать расширение **Docker** (правый клик на `docker-compose.yml` →
"Compose Up") вместо команды `docker compose up -d`.
