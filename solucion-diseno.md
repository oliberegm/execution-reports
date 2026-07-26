# Diseño de solución — Servicio de procesamiento de ExecutionReports

Este documento explica **la solución propuesta**, sin código: qué se construye, por qué, y cómo cada
decisión resuelve una de las garantías pedidas por el challenge. Sirve como base para después
traducirlo a `DECISIONS.md` + implementación.

---

## 1. Objetivo del sistema en una frase

Consumir un stream multiplexado de ExecutionReports (ER), aplicar cada uno al estado de su orden
**en la secuencia correcta**, **sin duplicarlo ni perderlo**, con **dos instancias corriendo en
paralelo**, y emitir un evento de *settlement* **exactamente una vez** cuando una orden queda `FILLED`.

Todo lo demás (UI, auth, features extra) queda deliberadamente afuera.

---

## 2. Arquitectura general

```
                 ┌──────────────┐
  productor de   │   Kafka      │   topic: execution-reports
  ER de prueba ─▶│  (particiones│   key = numericOrderId
                 │  por orderId)│
                 └──────┬───────┘
                         │  (2 particiones o más asignadas
                         │   entre las 2 instancias)
           ┌─────────────┴─────────────┐
           ▼                           ▼
   ┌───────────────┐          ┌───────────────┐
   │ Instancia A    │          │ Instancia B    │
   │ (Spring Boot)  │          │ (Spring Boot)  │
   └───────┬────────┘          └───────┬────────┘
           │                            │
           ▼                            ▼
   ┌───────────────────────────────────────────┐
   │            PostgreSQL                      │
   │  orders (estado)  ledger (histórico)        │
   │  outbox (settlement pendiente)  dlq (opc.)  │
   └───────────────────────┬────────────────────┘
                            │ relay (poller/CDC)
                            ▼
                 ┌─────────────────────┐
                 │  Kafka: settlement   │
                 └─────────────────────┘
```

Dos instancias del mismo servicio, mismo *consumer group*, consumiendo el mismo topic. Una única
base de datos relacional compartida (no hay estado en memoria que sobreviva un restart).

---

## 3. Elección del broker: Kafka

**Por qué Kafka y no otra cosa (RabbitMQ, SQS, etc.):**

- Kafka **particiona por clave** y garantiza orden **dentro de una partición**. Si particionamos por
  `numericOrderId`, todos los ER de una misma orden caen siempre en la misma partición y se leen en
  el orden en que el broker los recibió — que es justo la garantía que pide el challenge.
- Un *consumer group* con 2 instancias hace que **cada partición sea consumida por una sola
  instancia a la vez**. Esto da "secuencial por orden, paralelo entre órdenes" gratis, sin tener que
  inventar un mecanismo de locks distribuidos: el particionamiento *es* el mecanismo.
- Es un log persistente y reproducible: si hay que reprocesar (debug, replay, DLQ), los mensajes
  siguen ahí. Con una cola tradicional (RabbitMQ/SQS) esto se pierde una vez consumido.
- Commits de offset manuales dan control fino sobre cuándo se considera "procesado" un mensaje —
  clave para la recuperación ante fallas (sección 7).

**Qué se resigna:** Kafka no da *exactly-once* de fábrica a nivel de aplicación externa (solo dentro
de su propio log con transacciones Kafka-a-Kafka). Por eso el *exactly-once* real de este sistema no
depende de Kafka sino de la combinación **idempotencia en la base + commit manual de offset después
de persistir** (ver secciones 5 y 7). Kafka es el vehículo de entrega "al menos una vez"; la
correctitud final la garantiza la base de datos.

**Alternativa descartada:** RabbitMQ con *consistent hash exchange* podría dar un particionamiento
similar, pero pierde el log reproducible y hace más artesanal el manejo de DLQ y replay. Para este
problema, donde la garantía central es de secuencia por clave, Kafka es la herramienta que modela el
problema más directamente.

---

## 4. Particionado y secuencia con 2 instancias

- El topic `execution-reports` se crea con varias particiones (por ejemplo 6), **más que el número
  de instancias**, para que la unidad de paralelismo real sea la orden, no la instancia.
- El productor (el generador de stream de prueba) publica cada ER con `key = numericOrderId`. Kafka
  garantiza que todos los mensajes con la misma key van a la misma partición, en el orden de
  publicación.
- El *consumer group* de las 2 instancias reparte las particiones entre sí (por ejemplo, instancia A
  toma 3 particiones, B toma 3). Dentro de cada partición, el consumo es estrictamente secuencial —
  eso es lo que impide que un `FILLED` se aplique antes que su `NEW`.
- Entre órdenes de particiones distintas no hay ninguna coordinación: se procesan en paralelo, tanto
  dentro de una instancia (si usa varios threads, uno por partición) como entre instancias.
- **Qué se resigna:** dos órdenes que caigan en la misma partición no se procesan en paralelo entre
  sí, aunque no tengan relación. Es un trade-off aceptado: más particiones reduce la probabilidad de
  colisión, pero nunca la elimina del todo (limitación inherente al particionamiento por hash).
- Si una instancia muere, Kafka reasigna sus particiones a la otra (*rebalance*). Como el offset solo
  se commitea después de persistir en la base (sección 7), no hay ventana en la que un ER se
  considere aplicado sin estarlo.

---

## 5. Modelo de persistencia

Motor: **PostgreSQL**. Se eligió un motor relacional porque la garantía central ("el nuevo estado se
calcula a partir del estado persistido más el ER entrante, todo o nada") es intrínsecamente
transaccional: necesito leer el estado actual, validarlo, escribir el ledger y actualizar la orden
como una sola operación atómica. Un store de documentos sin transacciones multi-fila obligaría a
reconstruir a mano justo lo que un RDBMS da de fábrica.

Tablas:

**`orders`** (una fila por `numericOrderId`, estado mutable)
- `numeric_order_id` (PK)
- `status` (`NEW | PARTIALLY_FILLED | FILLED | CANCELLED`)
- `leaves_nominal_amount`, `accumulative_nominal_amount`, `avg_price` (últimos valores del ER
  aplicado — son snapshots, así que se sobreescriben, no se suman)
- `executions_applied_count` (se incrementa en +1 respecto al valor ya guardado, nunca se
  recalcula desde cero)
- `last_applied_fix_id` (auditoría: qué ER dejó la orden en este estado)
- `version` (para *optimistic locking*, ver más abajo)

**`order_ledger`** (una fila por ER efectivamente aplicado — el histórico inmutable)
- `id BIGSERIAL` (PK, autoincremental — refleja el orden de inserción real, que es lo que pide el
  challenge para poder mostrar el detalle "en orden de inserción")
- `numeric_order_id` (FK a `orders`)
- `fix_id` (**UNIQUE** — es la clave de dedup, ver sección 6)
- payload completo del ER (columna `jsonb`, para no perder ningún campo aunque no se use en el
  modelo de dominio)
- `applied_at`

**`settlement_outbox`** (patrón *transactional outbox*, ver sección 8)
- `id`, `numeric_order_id`, `payload`, `status (PENDING|SENT)`, `created_at`

**`processing_dlq`** (opcional si no se delega 100% en un DLQ topic de Kafka)
- guarda el ER crudo + motivo de error, para inspección manual.

---

## 6. Idempotencia: clave de dedup

El challenge aclara algo importante: *"muchos ER por orden es lo normal, no un duplicado — la
identidad de un duplicado es el ER individual, no la orden."* Es decir, el dedup no puede ser "ya
tengo esta orden", tiene que ser "ya tengo **este mensaje exacto**".

**Clave elegida: `fixId`.** Es el único campo presente en *todos* los ER (a diferencia de
`secondaryTradeId`/`operationNumber`, que aplican a ejecuciones/fills y no necesariamente a un `NEW`
o un `CANCELLED`), y por enunciado identifica la foto puntual del ER. Se lo modela como constraint
`UNIQUE` en `order_ledger.fix_id`.

Mecanismo: al procesar un ER, se intenta `INSERT` en `order_ledger` primero. Si la base devuelve
violación de constraint único (`fix_id` ya existe), se trata como **no-op idempotente**: no se toca
`orders`, y el mensaje se considera procesado (se commitea el offset igual). Si el insert tiene
éxito, recién ahí se aplica el efecto sobre `orders`, en la misma transacción.

Esto es más robusto que "verificar antes de insertar" (que tiene una *race condition* si dos
instancias procesaran el mismo mensaje — no debería pasar por el particionamiento, pero además cubre
reprocesos por *rebalance* o reinicio): la base es la única fuente de verdad sobre qué ya se aplicó,
gracias al constraint único, no un chequeo optimista en memoria.

---

## 7. Flujo de procesamiento de un ER (paso a paso)

1. El consumidor de Kafka recibe el ER de la partición que le toca (offset **no** commiteado
   todavía).
2. Se abre una transacción de base de datos.
3. `INSERT` en `order_ledger` con `fix_id`. Si falla por duplicado → se hace `ROLLBACK`/no-op,
   `COMMIT` vacío, se pasa al paso 7 directamente (se commitea offset, no hay nada más que hacer).
4. Si el insert fue exitoso: `SELECT ... FOR UPDATE` sobre la fila de `orders` (o alternativamente
   optimistic locking con `version`) para evitar carreras si en algún escenario excepcional dos
   procesos tocaran la misma orden a la vez (por ejemplo, durante un *rebalance* mal timeado).
5. Se valida la transición de estado **contra lo que ya está persistido**, no contra lo que trae el
   ER a ciegas: si `orders.status` ya es `FILLED` o `CANCELLED` (terminal), el ER no se aplica como
   avance normal — se trata como anomalía (ver sección 8: no se descarta en silencio, se loguea/
   deriva a revisión, porque llegar un ER después de terminal es indicio de un problema de origen,
   no algo a ignorar).
6. Si la transición es válida: se actualiza `orders` (status, leaves, accumulative, avg_price,
   `executions_applied_count = executions_applied_count + 1`, `last_applied_fix_id`). Si el nuevo
   status es `FILLED`, se inserta también una fila en `settlement_outbox` **en la misma transacción**
   (ver sección 8).
7. `COMMIT` de la transacción de base de datos.
8. **Solo después del commit exitoso**, se commitea el offset de Kafka para ese mensaje.

El orden "primero base, después offset" es la pieza clave de la recuperación ante fallas: si el
proceso muere entre el paso 7 y el 8, al reiniciar Kafka vuelve a entregar el mismo mensaje (porque
el offset no se movió), pero el paso 3 lo detecta como duplicado por `fix_id` y no lo vuelve a
aplicar. Nunca se pierde (porque no se avanza el offset sin persistir) ni se aplica dos veces (porque
el ledger es único por `fix_id`).

---

## 8. Settlement exactamente una vez

Cuando una orden llega a `FILLED`, hay que publicar un mensaje downstream **una sola vez por orden**,
incluso si:
- el ER que la dejó en `FILLED` se reentrega,
- las dos instancias "ven" el completado en algún escenario límite,
- el publish a Kafka falla a mitad de camino.

**Patrón: transactional outbox + relay idempotente.**

- El insert en `settlement_outbox` ocurre en la **misma transacción de base de datos** que el update
  de `orders` (paso 6 de la sección 7). Esto garantiza que "orden quedó FILLED" y "hay un settlement
  pendiente de publicar" son atómicos entre sí: no puede pasar una cosa sin la otra.
- Como el estado FILLED solo se alcanza una vez (una vez terminal, ER posteriores no vuelven a
  aplicar — sección 7, paso 5), solo se inserta **una fila** de outbox por orden. No depende de si el
  ER que la completó se reentrega: la reentrega es detectada como duplicado por `fix_id` *antes* de
  llegar a esta lógica.
- Un proceso separado (*relay*: un poller programado, o alternativamente Debezium leyendo el WAL de
  Postgres) lee filas `PENDING` de `settlement_outbox` y las publica al topic `settlement`, usando
  `numericOrderId` como **key** del mensaje y como **id idempotente del producer** de Kafka. Recién
  después de la confirmación de publicación (`ack`), marca la fila como `SENT`.
- Si el relay muere entre publicar y marcar `SENT`, al reiniciar reintenta publicar la misma fila. El
  downstream puede terminar viendo el mensaje más de una vez a nivel de transporte (*at-least-once*),
  pero como la clave es `numericOrderId` (fijo, uno por orden), el consumidor downstream puede
  deduplicar trivialmente por key — que es exactamente lo que el challenge permite ("aun cuando... vía
  idempotencia/outbox/dedup" — no exige exactly-once de transporte, exige que sea *deduplicable* de
  forma directa).
- `CANCELLED` no dispara ninguna fila en `settlement_outbox` — se filtra explícitamente en el paso 6.

---

## 9. Manejo de errores de procesamiento

Se distinguen dos clases:

**Errores transitorios** (ej: la base no responde, timeout de conexión, deadlock): no se commitea el
offset, se reintenta el mismo mensaje con backoff acotado (por ejemplo, reintentos in-process con
backoff exponencial hasta N intentos). Como el procesamiento es idempotente (sección 6), reintentar
no tiene efectos secundarios.

**Errores permanentes** (mensaje malformado, o un ER que no puede aplicarse porque su orden ya está
en estado terminal, o falta un campo obligatorio): agotados los reintentos razonables, el mensaje se
deriva a un **topic de dead-letter** (`execution-reports.dlq`) con metadata del error, y se
**commitea el offset del mensaje original** para no bloquear el resto del flujo de esa partición. La
clave del DLQ es no descartar en silencio: queda persistido, visible, y asociado a su
`numericOrderId` para que alguien lo revise.

**Por qué esto importa especialmente en este diseño:** como el orden se procesa secuencialmente
dentro de una partición, un mensaje que se queda trabado bloquearía *todas* las órdenes de esa
partición, no solo la suya — es el costo de haber elegido partición como mecanismo de orden (sección
4). Por eso los reintentos son acotados y con un techo claro: preferimos aislar el problema a una
orden (vía DLQ) antes que frenar la partición entera indefinidamente.

**Órdenes incompletas:** si un ER se pierde/rechaza (ej: llega un `PARTIALLY_FILLED` sin que exista
el `NEW` previo, indicando que ese `NEW` fue a la DLQ), la orden queda visiblemente en un estado que
no avanza — no hay reconciliación automática en este alcance, pero **queda detectable**: el ledger de
esa orden tiene un hueco visible, y el DLQ tiene el motivo. Se documenta como algo a resolver con
reconciliación manual o un job de detección de huecos en una versión más completa, y se anota como
supuesto en `DECISIONS.md`.

---

## 10. Endpoint HTTP de consulta

`GET /orders/{numericOrderId}` devuelve:
- estado actual (`status`, `executionsAppliedCount`, cantidades vigentes),
- el detalle completo del ledger de esa orden, ordenado por `id` ascendente (que es justamente la
  columna autoincremental que refleja el orden real de inserción, no un `ORDER BY transactionTime`
  que sería, por enunciado, un dato no confiable).

Es una simple lectura de las dos tablas — no hay lógica de negocio adicional acá, toda la
complejidad ya se resolvió en el momento de la ingesta.

---

## 11. Recuperación ante fallas — resumen

| Escenario | Qué garantiza el diseño |
|---|---|
| Una instancia muere a mitad de un ER | Offset no commiteado → Kafka reentrega tras rebalance → dedup por `fix_id` evita doble aplicación |
| Las dos instancias procesan (en algún escenario anómalo) el mismo ER | El `UNIQUE` de `fix_id` en la base es la fuente de verdad, no importa cuántas instancias lo intenten |
| Reinicio de ambas instancias | Los offsets commiteados están en Kafka, no en memoria; se retoma exactamente donde quedó cada partición |
| Reentrega intencional de un ER ya aplicado (duplicado del challenge) | Insert en `order_ledger` falla por constraint único → no-op, sin tocar `orders` |
| Falla del relay de settlement a mitad de publicar | Reintenta la misma fila `PENDING`; dedup downstream por `numericOrderId` como key |

---

## 12. Testing — qué priorizar

El enunciado pide foco, no cobertura amplia. Los puntos frágiles reales de este diseño son:

1. **Aplicación fuera de orden dentro de una misma orden** (simular que llega `FILLED` antes que
   `NEW` de la misma orden) → debe rechazarse/marcarse como anomalía, no aplicarse.
2. **Dedup por `fix_id`**: aplicar el mismo ER dos veces (misma clave) → el ledger no debe crecer, el
   contador de ejecuciones no debe incrementarse dos veces.
3. **Transición sobre estado terminal**: un ER que llega después de `FILLED`/`CANCELLED` → no debe
   mutar la orden.
4. **Concurrencia real con 2 instancias**: test de integración con docker-compose, publicando ER
   intercalados de varias órdenes, verificando que el estado final de cada orden es exactamente el
   que resulta de aplicar sus ER en orden, sin importar el entrelazado entre órdenes.
5. **Settlement exactamente una vez**: forzar que una orden llegue a `FILLED` más de una vez a nivel
   de intento (reentrega del ER final) y verificar que solo hay una fila en `settlement_outbox` /solo
   un mensaje publicado.
6. **Caída de instancia a mitad de un lote**: matar el proceso entre el commit de base y el commit de
   offset (se puede simular con un delay/breakpoint controlado) y verificar que al reiniciar no
   duplica ni pierde.

Todo lo demás (validación de campos triviales, serialización JSON, etc.) no aporta valor de test acá.

---

## 13. Trade-offs dejados afuera a propósito

- **Reconciliación automática de órdenes incompletas** (huecos en la secuencia por ER perdidos/DLQ):
  se detecta pero no se repara sola. En una versión completa: job periódico que cruza `orders` contra
  gaps esperables, o un mecanismo de *sequence number* explícito si el broker de mercado lo expusiera.
- **Exactly-once de transporte end-to-end con el downstream de settlement**: se resuelve con
  dedup por key, no con transacciones distribuidas — más simple y suficiente para el alcance pedido.
- **Rebalance óptimo / afinidad de partición histórica**: no se implementa *sticky assignment*
  avanzado; se usa el comportamiento estándar del consumer group de Kafka.
- **Hardening de producción** (retries con jitter configurable, métricas, alerting, seguridad): fuera
  de alcance por restricción explícita del challenge.
- **Escalado más allá de 2 instancias**: el diseño escala naturalmente agregando particiones e
  instancias, pero no se prueba más allá del par pedido.

---

## 14. Componentes del `docker-compose`

- `zookeeper` + `kafka` (o Kafka en modo KRaft sin zookeeper) — broker y topics (`execution-reports`,
  `execution-reports.dlq`, `settlement`).
- `postgres` — persistencia.
- `service-a`, `service-b` — dos instancias de la misma imagen Spring Boot, mismo `group.id`.
- un **generador de stream de prueba**: el más simple es un pequeño *runner* (puede ser un endpoint
  `POST /test/seed` en el propio servicio, o un script aparte) que publica un set de ER con varias
  órdenes intercaladas a propósito y al menos un `fix_id` repetido, para poder reproducir los
  escenarios pedidos (intercalado, duplicado, caída de instancia) simplemente documentando en el
  README los pasos: sembrar → matar `service-a` a mitad de carga → levantarlo de nuevo → consultar
  `/orders/{id}` y verificar consistencia.

---

Con esto cubierto, el `DECISIONS.md` final del repo sería básicamente este documento resumido en
formato de respuestas puntuales a cada pregunta que pide el enunciado, y el código sería la
implementación literal de las secciones 5 a 9.
