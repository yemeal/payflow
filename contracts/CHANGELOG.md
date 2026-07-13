# Changelog контрактов

Формат: `<schema>: <semver> - <дата> - <что изменилось>`

## 2026-07-14

- envelope/command-metadata: 1.0.0 - первая фиксация (camelCase, sagaId/businessKey).
- envelope/event-metadata: 1.0.0 - первая фиксация (snake_case, correlation-блок echo).
- envelope/dlq-envelope: 1.0.0 - первая фиксация.
- orders/order-created: 1.0.0 - первая фиксация.
- orders/saga-finished: 1.0.0 - saga.completed / saga.cancelled / saga.failed.
- inventory/reserve, commit-reservation, cancel-reservation: 1.0.0 - команды склада (+ ttlSeconds у reserve).
- inventory/reserve-result, commit-result, reservation-cancelled: 1.0.0 - события склада.
- payments/process: 1.0.0 - зеркало фактического контракта payment_service (payments.commands).
- payments/payment-result: 1.0.0 - зеркало payments.events + ТРЕБОВАНИЕ failure-блока
  в payment.failed (правка payment_service, saga-design 12/итерация 3 п.3).
