# dlq_watcher

Единая точка наблюдения за мёртвыми сообщениями платформы (docs/saga-design.md, 9.10).

Сервис читает **все** DLQ-топики разом и на каждое сообщение делает ровно три вещи:

1. инкрементирует метрику `dlq_messages_total{topic}`;
2. пишет структурный ERROR-лог `dlq_message_received`;
3. дёргает алерт-канал (`AlertSinkProtocol`).

И больше ничего. Watcher **не обрабатывает, не чинит и не переигрывает** сообщения:
его задача - видимость. Re-drive в MVP остаётся ручной операцией (см. ниже).

## Как работает

Kafka-only сервис, БД нет. Точка входа: `src/app/entrypoints/messaging/watcher.py`.

```text
<любой топик>.dlq --regex--> KafkaBroker(pattern) --> DlqService
                                                        |-- metrics: dlq_messages_total{topic}
                                                        |-- log:     ERROR dlq_message_received
                                                        '-- alert:   LoggingAlertSink ("ALERT ...")
```

Разбирается конверт `contracts/envelope/dlq-envelope.v1.schema.json`:
`{"original": {...}, "dlqMeta": {sourceTopic, partition, offset, consumerGroup,
errorClass, errorMessage, retryCount, redriveCount, failedAt}}`.

В лог и алерт попадают `sourceTopic`, `errorClass`, `errorMessage`, `retryCount`,
`redriveCount`, координаты (`partition`/`offset`) и корреляция саги (`sagaId`,
`businessKey`), если исходное сообщение её несло. Корреляцию читаем толерантно:
у команды она лежит прямо в `original.metadata`, у события - в
`original.metadata.correlation` (см. contracts/README).

## Зачем regex-подписка

DLQ-топиков столько же, сколько рабочих (`orders.events.dlq`, `inventory.commands.dlq`,
`payments.commands.dlq`, ...), и список растёт вместе с платформой. Перечислять их
статически - значит гарантированно однажды забыть про новый топик и **ослепнуть ровно
там, где сломалось**. Поэтому подписка идёт по паттерну `.*\.dlq$` (`KAFKA_DLQ_PATTERN`):
новый DLQ-топик подхватывается сам, без правки конфига и редеплоя watcher'а.

Реализовано штатными средствами FastStream: `broker.subscriber(pattern=...)`. Проверено
по faststream 0.7.2 (`kafka/subscriber/usecase.py`): regex доезжает до
`aiokafka.consumer.subscribe(pattern=...)` без искажений - `compile_path()` трогает
строку только при наличии плейсхолдеров `{param}`, которых в нашем паттерне нет.
Поэтому собственный asyncio-цикл на aiokafka не понадобился: он потребовал бы
переписать коммиты, ребаланс и graceful shutdown ради того же самого результата.

Новый топик замечается не мгновенно: aiokafka обновляет метаданные раз в
`metadata_max_age_ms` (по умолчанию 5 минут). Для наблюдателя это приемлемо.

## Устойчивость

Watcher обязан пережить **любое** содержимое DLQ - там по определению лежит то, что
уже сломало другой сервис.

- Подписчик берёт сырые байты (`msg.body`) и не просит FastStream декодировать тело:
  декодер ленивый, поэтому не-JSON мусор не может уронить фреймворк до нашего кода.
- Битый конверт (не JSON, нет `dlqMeta`, нет `sourceTopic`) не приводит к падению:
  пишется ERROR-лог `dlq_envelope_invalid` + отдельный алерт, обработка продолжается.
- Исключения в хендлере гасятся. При `ack_policy=NACK_ON_ERROR` (инвариант консюмеров
  репозитория) всплывшее исключение дало бы NACK на **уже мёртвом** сообщении, то есть
  вечный цикл на одном offset - и watcher перестал бы видеть всё остальное.
- `group_id=dlq-watcher` + `auto_offset_reset=earliest`: offset'ы коммитятся, а watcher,
  поднятый после аварии, видит сообщения, умершие в его отсутствие.

## Метрики

Мини-экспортер `prometheus_client` на `METRICS_PORT` (по умолчанию 9100), так как
своего HTTP-сервера у Kafka-only сервиса нет (saga-design, 9.9).

- `dlq_messages_total{topic}` - counter, инкремент на каждое прочитанное сообщение,
  **включая** сообщения с битым конвертом (иначе самые сломанные были бы невидимы).

```bash
curl http://localhost:9100/metrics | grep dlq_messages_total
```

## Конфигурация

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | - | брокеры Kafka |
| `KAFKA_DLQ_PATTERN` | `.*\.dlq$` | regex подписки на DLQ-топики |
| `KAFKA_CONSUMER_GROUP` | `dlq-watcher` | consumer group |
| `METRICS_PORT` | `9100` | порт мини-экспортера Prometheus |
| `DEV_LOGS` | `true` | `true` - консоль, `false` - JSON |
| `LOG_LEVEL` | `INFO` | уровень логирования |

## Запуск

```bash
poetry install
poetry run python -m app.entrypoints.messaging.watcher   # PYTHONPATH=src
poetry run pytest
```

## Re-drive (ручная операция)

Автоматики в MVP нет и это осознанно: переигрывать можно только **после устранения
причины** (деплой фикса), иначе сообщение снова умрёт и вернётся в DLQ.

Порядок действий:

1. Найти сообщение в DLQ и взять из него `original` и `dlqMeta.sourceTopic`:

   ```bash
   kafka-console-consumer --bootstrap-server kafka:29092 \
     --topic inventory.commands.dlq --from-beginning --max-messages 1
   ```

2. Убедиться, что причина устранена (`errorClass` / `errorMessage` из `dlqMeta`).
3. Проверить лимит: если `dlqMeta.redriveCount` уже **2** - переигрывать НЕЛЬЗЯ,
   только ручной разбор. Иначе получаем бесконечную карусель poison-сообщения.
4. Опубликовать `original` **как есть** обратно в `sourceTopic`, сохранив ключ
   (`businessKey`) и тот же `message_id` / `commandId` / `event_id`:

   ```bash
   kafka-console-producer --bootstrap-server kafka:29092 \
     --topic inventory.commands \
     --property "parse.key=true" --property "key.separator=|"
   # order-42|{"metadata": {...тот же commandId...}, "data": {...}}
   ```

5. Инкрементировать `redriveCount` в DLQ-конверте при следующем попадании в DLQ.

Почему это безопасно: обработка транзакционна (дедуп-запись + бизнес-эффект + outbox
в одной транзакции). Раз сообщение попало в DLQ - его эффект **не закоммичен**, значит
повторная обработка чиста. Если эффект всё же прошёл, дедуп по `message_id` отбросит
дубль. Тот же `message_id` при re-drive - обязателен: именно он делает переигровку
идемпотентной.

Инструмент (CLI или admin API) для re-drive - в бэклоге.
