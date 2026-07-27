# Decisiones de Diseño

## Broker: Kafka

Kafka particionado por `numericOrderId`. Todos los ER de una misma orden van a la misma partición,
garantizando orden de procesamiento sin locks distribuidos. Un consumer group con 2 instancias
reparte particiones automáticamente.

**Alternativa descartada:** RabbitMQ con consistent hash exchange — pierde log reproducible y
complica DLQ/replay.

## Persistencia: PostgreSQL + JPA

Motor relacional por la naturaleza transaccional del problema: leer estado actual, validar, escribir
ledger y actualizar orden como operación atómica.

Se usa JPA para simplificar el CRUD. **Si los tiempos de respuesta no son satisfactorios, se migrará
a JDBC directo** que garantiza mayor velocidad al eliminar el overhead del ORM.

## Locking: SELECT FOR UPDATE (pessimistic)

Dentro de una partición Kafka el procesamiento ya es secuencial, así que el lock casi nunca contende.
Se utiliza exclusivamente `SELECT FOR UPDATE` (`@Lock(LockModeType.PESSIMISTIC_WRITE)`). Se prescinde de `@Version` (optimistic locking) para evitar duplicación de mecanismos de sincronización.

## Idempotencia: fix_id UNIQUE en order_ledger

`existsByFixId` actúa como un short-circuit de performance para el caso no concurrente; la garantía real de exactamente-una-vez la proporciona el constraint `UNIQUE` en `order_ledger.fix_id` junto con la captura de la violación en la inserción previa en `order_ledger`, retornando `ApplyResult.AlreadyProcessed` de forma transparente.

## Estado de la orden: calculado, no sobreescrito

`OrderStateMachine.apply(currentState, incomingER)` — función pura. El nuevo estado se computa
desde el estado ya persistido + el ER entrante.

## executions_applied_count

Cuenta **todo ER aplicado**, incluyendo NEW. Es "cantidad de ERs efectivamente aplicados", no solo
fills.

## Settlement: transactional outbox + relay SKIP LOCKED

Insert en `settlement_outbox` en la misma transacción que el update a FILLED. Relay con
`SELECT ... FOR UPDATE SKIP LOCKED` para que ambas instancias puedan correr el poller sin duplicar
publicaciones.

## Errores

- **Transitorios** (DB timeout, conexión): retry con backoff exponencial, N intentos.
- **Permanentes** (JSON inválido, campos faltantes): DLQ topic + ack del original.
- **ER sobre orden terminal**: anomalía logueada + commit normal. NO va a DLQ.

## Trade-offs dejados afuera

> TODO: se completará en Fase 9
