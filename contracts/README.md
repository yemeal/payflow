# Контракты сообщений OrderFlow / PayFlow

Этот каталог играет роль ОТДЕЛЬНОГО репозитория контрактов (ADR-007 в docs/saga-design.md).
Правила игры "это не монорепозиторий":

- сервисы НЕ импортируют этот каталог в рантайме;
- у каждого сервиса СВОИ Pydantic-модели сообщений (осознанная дупликация - цена автономии команд);
- каждый сервис обязан иметь contract-тест: его фикстуры сообщений валидируются
  против JSON Schema отсюда;
- изменение схемы - через изменение здесь + запись в CHANGELOG.md (semver на схему).

## Топики

| Топик | Назначение | Пишут | Читают |
|---|---|---|---|
| orders.events | шина саги заказа: order.created, inventory.*, saga.* | order-service, inventory-service, orchestrator | orchestrator, order-service |
| inventory.commands | команды складу | orchestrator | inventory-service |
| payments.commands | команды оплате (контракт payment_service) | orchestrator | payment-service |
| payments.events | исходы оплат (контракт payment_service) | payment-service | orchestrator, analytics |
| notifications.commands | команды уведомлений (зарезервировано) | orchestrator | notification-service |
| `<топик>.dlq` | парный DLQ каждого топика | консюмер топика | dlq-watcher |

Ключ партиционирования: business_key саги (для заказа - order_id). Все сообщения
одной саги идут с одним ключом - Kafka сохраняет их порядок внутри партиции.

## Конверты

- **Команда**: `{"metadata": <command-metadata>, "data": {...}}`. Метаданные в camelCase
  (исторически: выравнивание по фактическому контракту payment_service; его схема
  принимает оба регистра, но канон - camelCase).
- **Событие**: `{"metadata": <event-metadata>, "data": {...}}`. Метаданные в snake_case
  (фактический формат событий payment_service). data - всегда camelCase.
- Схемы конвертов: envelope/*.schema.json.

## Правила

1. **Echo (корреляция)**: участник, обрабатывая команду, обязан вернуть в ответном
   событии блок `metadata.correlation = {sagaId, businessKey, commandId}` как
   непрозрачные значения - не интерпретируя их. Участник с отложенными исходами
   (payment_service: pending -> completed/failed) персистит correlation рядом со
   своим агрегатом и проставляет его во ВСЕ события по этому агрегату.
   События без correlation (агрегат создан не командой саги, например платёж
   через HTTP API) к сагам не относятся и оркестратором игнорируются.
2. **Идемпотентность участника**: журнал command_id -> результат. Дубликат команды
   не выполняется повторно - переиздаётся сохранённый ответ.
3. **failure-блок**: каждое `*.failed` событие обязано нести
   `data.failure = {code, message, retriable}`. retriable=false - бизнес-отказ
   (компенсация немедленно), retriable=true - технический сбой (оркестратор ретраит шаг).
4. **Идемпотентность консюмера**: дедупликация по event_id (события) / commandId
   (команды) в одной транзакции с бизнес-эффектом.
5. **Эволюция**: аддитивные изменения - minor (консюмеры - tolerant reader,
   неизвестные поля игнорируются); breaking - major + новый файл `*.v2.schema.json`.
6. **DLQ**: не смогший обработать сообщение консюмер публикует в `<топик>.dlq`
   конверт envelope/dlq-envelope.v1.schema.json (оригинал + метаданные сбоя).
   Переигровка (re-drive): republish оригинала в sourceTopic с тем же message_id,
   redriveCount+1, лимит 2.

## Команды и события саги заказа (order-fulfillment)

| Сообщение | Топик | Схема |
|---|---|---|
| order.created | orders.events | orders/order-created.v1.schema.json |
| inventory.reserve | inventory.commands | inventory/reserve.v1.schema.json |
| inventory.reserved / inventory.reserve-failed | orders.events | inventory/reserve-result.v1.schema.json |
| payment.process | payments.commands | payments/process.v1.schema.json |
| payment.completed / payment.failed | payments.events | payments/payment-result.v1.schema.json |
| inventory.commit_reservation | inventory.commands | inventory/commit-reservation.v1.schema.json |
| inventory.reservation-committed / inventory.commit-failed | orders.events | inventory/commit-result.v1.schema.json |
| inventory.cancel_reservation | inventory.commands | inventory/cancel-reservation.v1.schema.json |
| inventory.reservation-cancelled | orders.events | inventory/reservation-cancelled.v1.schema.json |
| saga.completed / saga.cancelled / saga.failed | orders.events | orders/saga-finished.v1.schema.json |

Инвариант конфигурации: TTL резерва (inventory) >= дедлайн шага оплаты (orchestrator)
+ буфер (>= 5 минут). Нарушение - гонка "оплата успела, резерв истёк": сага уходит
в FAILED на ручной разбор (docs/saga-design.md, 12/итерация 3, п.1).
