# Decisiones de Diseño y Arquitectura — ExecutionReports Service

Este documento condensa las decisiones arquitectónicas del proyecto y responde punto por punto a los requerimientos y garantías evaluadas.

---

## 1. Elección del Broker de Mensajería: Apache Kafka

### ¿Por qué Kafka y no RabbitMQ / SQS / colas tradicionales?
1. **Particionado por Clave y Orden Garantizado**: Kafka garantiza el orden estricto de los mensajes dentro de una misma partición. Al particionar por `key = numericOrderId`, todos los `ExecutionReport` de una misma orden caen en la misma partición y se consumen en el orden exacto de publicación.
2. **Paralelismo sin Locks Distribuidos**: Un *consumer group* reparte las particiones entre las $N$ instancias (2 en este despliegue). Cada partición es leída por exactamente un hilo/instancia a la vez, logrando *secuencial por orden, paralelo entre órdenes* de forma nativa.
3. **Log Persistente y Reproducible**: Permite reprocesamiento, inspección de offsets y derivación limpia hacia colas de errores (*Dead-Letter Queue*).

### Compromiso Aceptado (*Trade-off*)
Kafka garantiza la entrega a nivel de transporte en modo *at-least-once*. La garantía de *exactly-once* a nivel de negocio no depende exclusivamente de Kafka, sino de la combinación de **idempotencia en base de datos (`order_ledger.fix_id UNIQUE`)** y **commit manual de offset posterior al commit de la transacción de BD**.

---

## 2. Estrategia de Secuencia con 2 Consumidores

- **Particionamiento**: El topic `execution-reports` posee 6 particiones (superior al número de instancias = 2).
- **Asignación por Consumer Group**: Kafka asigna 3 particiones a `service-a` y 3 a `service-b`.
- **Garantía de Secuencia**: Ninguna orden es consumida simultáneamente por 2 instancias porque su partición pertenece a un solo consumidor.
- **Failover / Caída de Instancia**: Si `service-a` cae, Kafka dispara un *rebalance* y asigna sus particiones a `service-b`. Como el offset solo se commitea tras persistir en PostgreSQL, `service-b` retoma la lectura desde el último offset confirmado sin perder ni duplicar eventos.

---

## 3. Idempotencia y Clave de Deduplicación

- **Clave Elegida: `fix_id`**: Es el identificador único presente en cada `ExecutionReport`.
- **Mecanismo de Guardián Único (*Ledger First*)**:
  1. Al procesar un ER, se ejecuta primero el `INSERT` en `order_ledger`.
  2. La columna `order_ledger.fix_id` posee una restricción de unicidad (`UNIQUE`).
  3. Si el `fix_id` ya existe en la base de datos, el `INSERT` arroja una excepción de violación de constraint único, la transacción realiza `ROLLBACK` y se retorna un resultado idempotente *no-op* sin alterar la tabla `orders`.
  4. Si el `INSERT` es exitoso, se evalúa la transición de estado y se actualiza `orders` en la misma transacción.

---

## 4. Motor de Persistencia y Modelo de Datos: PostgreSQL

Se eligió un motor relacional ACID por ser una operación intrínsecamente transaccional (lectura del estado actual, inserción en ledger y actualización de la orden en un bloque atómico `ALL-OR-NOTHING`).

### Tablas Principales:
1. **`orders`**: Estado mutable calculado de la orden.
   - `numeric_order_id` (PK)
   - `status` (`NEW`, `PARTIALLY_FILLED`, `FILLED`, `CANCELLED`)
   - `executions_applied_count` (contador incremental de ejecuciones aplicadas)
   - `leaves_nominal_amount`, `accumulative_nominal_amount`, `avg_price`
2. **`order_ledger`**: Histórico inmutable de ejecuciones aplicadas.
   - `id BIGSERIAL` (PK, autoincremental que refleja el orden real de inserción)
   - `numeric_order_id` (FK)
   - `fix_id` (**UNIQUE**, gate de idempotencia)
   - `payload` (`JSONB` con el reporte original completo)
3. **`settlement_outbox`**: Patrón *Transactional Outbox*.
   - `id BIGSERIAL` (PK)
   - `numeric_order_id`, `payload`, `status` (`PENDING`, `SENT`), `created_at`, `sent_at`

---

## 5. Garantía de Cálculo de Estado (Máquina de Estados Pura)

El estado de la orden **nunca se sobreescribe a ciegas** desde el reporte recibido. Se evalúa mediante la máquina de estados funcional de dominio puro `OrderStateMachine` (Java 21 `sealed interface ApplyResult`):

- Si la orden está en estado terminal (`FILLED` o `CANCELLED`), reportes posteriores son rechazados (`TERMINAL_STATE`).
- Si llega un reporte para una orden inexistente diferente de `NEW`, es rechazado (`ORPHAN_REPORT`).
- Transiciones válidas recalculan importes y marcan la bandera `settlementRequired = true` únicamente al alcanzar `FILLED`.

---

## 6. Política de Manejo de Errores y Resiliencia

- **Ack Mode Manual (`MANUAL_IMMEDIATE`)**: El offset de Kafka solo se commitea tras confirmar el commit de la transacción en PostgreSQL.
- **Manejo de Fallas Transitorias**: Reintentos automáticos configurados en Spring Kafka `DefaultErrorHandler` con backoff.
- **Errores Permanentes / DLQ**: Mensajes no procesables son enviados al topic `execution-reports.dlq` con metadata de error y el offset original es commiteado para no bloquear la partición.

---

## 7. Garantía de Settlement Exactly-Once

- **Patrón Transactional Outbox**: Cuando la orden pasa a `FILLED`, la fila en `settlement_outbox` se inserta en la **misma transacción de BD**.
- **Relay Desacoplado**: El caso de uso `RelaySettlementUseCase` lee lotes `PENDING` utilizando `SELECT ... FOR UPDATE SKIP LOCKED` (evitando contención entre instancias), transmite a Kafka topic `settlement` con clave `numericOrderId` y actualiza la entidad inmutable a `SENT` mediante `.toBuilder()`.

---

## 8. Resumen de Requerimientos y Cumplimiento

| Requerimiento del Challenge | Mecanismo de Solución |
|---|---|
| Ingesta asíncrona de alto rendimiento | Kafka + Spring `@KafkaListener` con Ack manual |
| Dos instancias procesando en paralelo | Kafka Consumer Group `execution-reports-group` con 6 particiones |
| Orden garantizado por orden | Key de Kafka = `numericOrderId` (misma partición = mismo orden) |
| Idempotencia ante duplicados | `order_ledger.fix_id UNIQUE` (ledger first) |
| Estado calculado y ledger con ID autoincremental | PostgreSQL + `OrderStateMachine` + `order_ledger.id BIGSERIAL` |
| Settlement exactamente una vez | Transactional Outbox + `SKIP LOCKED` relay a topic `settlement` |
| Endpoint HTTP de consulta | `GET /orders/{numericOrderId}` con `ledger ORDER BY id ASC` |
| Tolerancia a fallas y caída de nodo | Offsets commiteados post-persistencia + failover automático |
