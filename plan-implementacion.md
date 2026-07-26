# Plan de implementación — Servicio de ExecutionReports

Plan de ejecución fase por fase para construir la solución descripta en `solucion-diseno.md`.
Pensado para ir de cero a entregable completo, en orden de dependencias reales (no por prolijidad).
Cada fase termina en un estado funcional y verificable, no en código a medio hacer.

---

## Fase 0 — Setup del repo y esqueleto

**Objetivo:** tener `docker compose up` levantando algo, aunque no haga nada útil todavía.

1. Crear repo Git, primer commit vacío con estructura de carpetas:
   ```
   /service            (proyecto Spring Boot)
   /docker-compose.yml
   /README.md
   /DECISIONS.md
   /scripts o /seed     (generador de stream de prueba)
   ```
2. Proyecto Spring Boot (Java 21, Gradle o Maven — decidir uno y no volver atrás) con dependencias
   base: `spring-boot-starter-web`, `spring-kafka`, `spring-boot-starter-data-jpa` (o jdbc si se
   prefiere más control), driver de Postgres, `spring-boot-starter-test`, `testcontainers`.
3. `docker-compose.yml` mínimo: `postgres`, `kafka` (+ zookeeper o KRaft), y **una sola** instancia
   del servicio (todavía sin lógica) que levante y responda `GET /health`.
4. Verificar que `docker compose up` deja todo arriba y el healthcheck responde. Commit.

**Criterio de salida:** stack completo levanta, servicio responde, sin lógica de negocio aún.

---

## Fase 1 — Modelo de datos y capa de persistencia

**Objetivo:** las tablas existen y son accesibles, sin consumidor de Kafka todavía.

1. Migraciones (Flyway o Liquibase — elegir uno; Flyway es más simple para este alcance) con:
   - `orders` (numeric_order_id PK, status, leaves_nominal_amount, accumulative_nominal_amount,
     avg_price, executions_applied_count, last_applied_fix_id, version, updated_at)
   - `order_ledger` (id BIGSERIAL PK, numeric_order_id FK, fix_id UNIQUE NOT NULL, payload JSONB,
     applied_at)
   - `settlement_outbox` (id, numeric_order_id, payload, status, created_at, sent_at)
   - índice explícito sobre `order_ledger(numeric_order_id, id)` para el endpoint de consulta
     ordenado.
2. Entidades JPA (o DTOs + JDBC directo si se prefiere evitar el overhead de JPA para las
   transacciones críticas — **decisión a tomar y anotar en DECISIONS.md**, ya que en la Fase 3 esto
   importa para controlar exactamente qué pasa dentro de la transacción).
3. Repositorios: `OrderRepository`, `OrderLedgerRepository`, `SettlementOutboxRepository`.
4. Tests unitarios de repositorio contra Postgres real vía Testcontainers (no H2 — el comportamiento
   de constraint único y locking tiene que ser el real).

**Criterio de salida:** se puede insertar/leer manualmente (vía test) una orden y su ledger,
incluyendo el rechazo por `fix_id` duplicado.

---

## Fase 2 — Modelo de dominio: aplicar un ER sobre una orden

**Objetivo:** la lógica de negocio pura, testeada, **sin Kafka todavía** (se invoca desde un test o
un endpoint temporal).

1. Clase de dominio `OrderStateMachine` (o similar) con una función central:
   `apply(currentOrderState (nullable), incomingER) -> Result<NewOrderState | Rejection>`.
2. Reglas a codificar explícitamente:
   - si `currentOrderState == null` y el ER no es `NEW` → rechazo (falta el `NEW` — orden huérfana).
   - si `currentOrderState == null` y el ER es `NEW` → se crea la orden.
   - si `currentOrderState.status` ya es `FILLED` o `CANCELLED` → rechazo (terminal).
   - transición válida → se calcula el nuevo estado: `status`, `leaves`, `accumulative`, `avgPrice`
     tomados tal cual del ER (son snapshots), `executionsAppliedCount = actual + 1`.
   - si el nuevo `status == FILLED` → se marca que corresponde emitir settlement.
3. Tests unitarios exhaustivos de esta función (es el corazón del sistema, mayor densidad de tests
   acá que en cualquier otra parte):
   - `NEW` sobre orden inexistente → crea.
   - `PARTIALLY_FILLED` sobre orden inexistente (sin `NEW` previo) → rechazo.
   - secuencia `NEW → PARTIALLY_FILLED → FILLED` → estado final correcto, contador en 3.
   - ER sobre orden ya `FILLED` → rechazo, no cambia estado.
   - ER sobre orden ya `CANCELLED` → rechazo.
   - `CANCELLED` no genera marca de settlement; `FILLED` sí.

**Criterio de salida:** la lógica de transición está 100% cubierta por tests unitarios y no depende
de Spring, Kafka ni la base — es una función pura testeable en aislamiento.

---

## Fase 3 — Persistencia transaccional del ER (sin Kafka aún)

**Objetivo:** conectar el dominio de la Fase 2 con la base de la Fase 1, con la transacción completa
descripta en la sección 7 del documento de diseño, invocada desde un test de integración (todavía sin
Kafka de por medio).

1. `ExecutionReportProcessor.process(er)`:
   - abre transacción,
   - intenta insertar en `order_ledger` (fix_id) → si falla por unique constraint, corta acá
     (no-op, commit vacío),
   - si insertó: lock de la fila de `orders` (`SELECT FOR UPDATE` o manejo de `OptimisticLockException`
     con `version` — elegir uno de los dos mecanismos, no mezclar ambos sin necesidad),
   - invoca `OrderStateMachine.apply(...)`,
   - si es rechazo por estado terminal/huérfano → loguear como anomalía (tabla o log estructurado;
     no lanzar excepción que reintente infinito) y hacer commit igual (el ER "se procesó": se
     registró la anomalía, no se aplicó al estado),
   - si es válido → persistir el nuevo estado de `orders`,
   - si el nuevo estado es `FILLED` → insertar fila en `settlement_outbox` en la misma transacción,
   - commit.
2. Tests de integración (Testcontainers) que reproducen exactamente los casos de la Fase 2 pero ahora
   pasando por la base real: duplicado por `fix_id`, secuencia completa, ER sobre terminal, y además
   **concurrencia**: dos threads procesando el mismo ER en paralelo (simulando el escenario límite de
   dos instancias) → verificar que el ledger tiene una sola fila y `orders` se actualizó una sola vez.

**Criterio de salida:** se puede llamar a `process(er)` repetidamente, con duplicados, fuera de orden,
y concurrentemente, y el estado en base siempre es el correcto. Esta es la pieza más importante del
challenge — no avanzar a Kafka hasta que esto esté sólido.

---

## Fase 4 — Integración con Kafka: consumidor con orden por partición

**Objetivo:** conectar el `ExecutionReportProcessor` al stream real, con el flujo de commit manual de
offset descripto en el diseño.

1. Crear topics: `execution-reports` (N particiones, ej. 6), `execution-reports.dlq`, `settlement`.
2. Configurar el `KafkaListener` (o `KafkaConsumer` manual si se necesita más control fino sobre el
   commit) con:
   - `enable.auto.commit=false`,
   - `ackMode = MANUAL` (Spring Kafka) — el ack ocurre **después** de que `process(er)` retorna
     exitosamente,
   - deserializador tolerante a error (para no tirar abajo el consumidor con un mensaje corrupto —
     eso ya es un error "permanente", va directo a manejo de errores, no a un crash del listener).
3. Distinguir en el manejo de excepciones de `process(er)`:
   - excepción transitoria (`DataAccessException` de conexión, timeout) → no ack, dejar que
     Spring Kafka reintente (con `DefaultErrorHandler` + backoff acotado, ej. 5 intentos con backoff
     exponencial).
   - agotados los reintentos, o excepción marcada explícitamente como permanente (ej. JSON
     inválido) → publicar en `execution-reports.dlq` con el motivo, y **sí** hacer ack del original
     (para no bloquear la partición).
4. Test de integración con Testcontainers de Kafka: publicar ER intercalados de 3-4 órdenes distintas
   con claves distintas, y un duplicado exacto (mismo `fix_id`) en el medio del stream, y verificar
   que al final el estado de cada orden en base es el correcto y el ledger no tiene el duplicado.

**Criterio de salida:** un productor externo (aunque sea un test) publica ER y el estado en base
converge correctamente, con orden por partición garantizado y duplicados absorbidos.

---

## Fase 5 — Relay de settlement (outbox → Kafka)

**Objetivo:** las filas `PENDING` de `settlement_outbox` se publican al topic `settlement` de forma
confiable.

1. Componente `OutboxRelay`: un `@Scheduled` con intervalo corto (ej. cada 500ms–1s) que:
   - lee un lote de filas `PENDING` (con `LIMIT` y orden por `id`),
   - publica cada una al topic `settlement` con `key = numericOrderId`,
   - al recibir el `ack` del producer, marca la fila como `SENT`.
2. Configurar el producer de Kafka como idempotente (`enable.idempotence=true`) — protege contra
   duplicados a nivel de reintento de red del propio producer, complementario (no sustituto) del
   dedup por key que hace el consumidor downstream.
3. Test: forzar que una orden llegue a `FILLED` dos veces a nivel de intento (reentrega del ER que la
   completa) y verificar que solo existe una fila en `settlement_outbox` y que el topic `settlement`
   recibe un solo mensaje para esa orden.
4. (Opcional, si da el tiempo) Reemplazar el poller por Debezium/CDC sobre la tabla outbox — mencionar
   en DECISIONS.md como alternativa considerada aunque no se implemente, si se decide no hacerlo por
   tiempo.

**Criterio de salida:** toda orden que llega a `FILLED` termina, de forma confiable y sin duplicar,
con un mensaje en el topic `settlement`.

---

## Fase 6 — Endpoint HTTP de consulta

**Objetivo:** exponer el estado.

1. `GET /orders/{numericOrderId}`:
   - 404 si no existe,
   - 200 con `status`, `executionsAppliedCount`, cantidades vigentes, y el detalle del ledger
     (`ORDER BY id ASC`).
2. Test de integración simple: sembrar una secuencia de ER vía Kafka, esperar a que se procesen
   (polling con timeout corto en el test), consultar el endpoint y verificar el JSON.

**Criterio de salida:** se puede levantar todo el stack, publicar ER, y consultar el resultado por
HTTP.

---

## Fase 7 — Dos instancias reales + prueba de caída

**Objetivo:** verificar en `docker-compose` real (no en Testcontainers) el escenario central que
evalúan: 2 instancias + reentrega + caída.

1. Completar el `docker-compose.yml` con `service-a` y `service-b`, mismo `group.id` de Kafka,
   mismas variables de conexión a Postgres.
2. Generador de stream de prueba (ver Fase 8) que publique un volumen razonable de ER intercalados
   entre varias órdenes.
3. Prueba manual documentada (y automatizada si el tiempo alcanza, con un script que orqueste
   `docker compose kill service-a` a mitad de carga):
   - levantar todo,
   - sembrar el stream,
   - matar `service-a` mientras procesa,
   - verificar que `service-b` (o `service-a` al reiniciar) retoma sin duplicar ni perder,
   - consultar `/orders/{id}` de varias órdenes y verificar consistencia contra lo esperado.
4. Ajustar `docker-compose.yml` con healthchecks y `depends_on` con condición, para que `docker
   compose up` sea confiablemente reproducible de punto a punto.

**Criterio de salida:** el escenario completo del challenge (2 instancias, intercalado, duplicados,
caída) se puede reproducir con los pasos que van a documentarse en el README.

---

## Fase 8 — Generador de stream de prueba

**Objetivo:** una forma simple y reproducible de sembrar escenarios.

Opción recomendada (más simple de operar y de documentar): un endpoint interno
`POST /test/seed` en el propio servicio (guardado detrás de un profile `test`/`local`, nunca en
`prod` real — aunque acá no hay prod real, se anota igual como supuesto) que:
- genera N órdenes con M ER cada una (secuencias válidas `NEW → ... → FILLED/CANCELLED`),
- intercala la publicación entre órdenes (no publica todos los ER de una orden seguidos),
- inyecta a propósito 1-2 `fix_id` repetidos,
- publica todo directo al topic `execution-reports` con las keys correctas.

Alternativa: script standalone (Python o un `curl`+`kafka-console-producer` con JSON pregenerado) si
se prefiere no tener código de test mezclado en el servicio productivo — **a definir y anotar el
motivo en DECISIONS.md**.

**Criterio de salida:** un solo comando reproduce el escenario de prueba completo.

---

## Fase 9 — Documentación final

**Objetivo:** los tres entregables de texto.

1. `README.md`: cómo levantar (`docker compose up`), cómo sembrar datos, cómo reproducir cada
   escenario clave (intercalado, duplicado, caída de instancia) paso a paso con comandos concretos.
2. `DECISIONS.md`: condensar `solucion-diseno.md` en las respuestas puntuales que pide el enunciado
   (broker, secuencia con 2 consumidores, idempotencia, persistencia, consistencia del estado,
   errores, settlement exactamente-una-vez, trade-offs).
3. Revisión final: releer el enunciado punto por punto contra lo entregado, marcando explícitamente
   en `DECISIONS.md` cualquier punto que se haya resuelto distinto a lo ideal por tiempo, en vez de
   dejarlo sin mencionar.

**Criterio de salida:** alguien que no vio el proyecto puede clonar, levantar, reproducir los
escenarios y entender el porqué de cada decisión leyendo `DECISIONS.md`.

---

## Orden de prioridad si el tiempo aprieta

Si hay que recortar, este es el orden de qué sacrificar (de lo menos grave a lo más grave respecto de
lo que se evalúa):

1. Automatizar el test de caída de instancia (queda como procedimiento manual documentado).
2. Reemplazar el poller de outbox por CDC (el poller simple alcanza y está permitido).
3. Tabla `processing_dlq` propia (usar directamente el topic DLQ de Kafka y loguear).
4. Cobertura de tests sobre el endpoint HTTP (es la parte menos riesgosa del sistema).

**Nunca recortar:** Fase 2 (máquina de estados) y Fase 3 (transacción + idempotencia) — son
literalmente lo que el challenge evalúa como "correctitud de las garantías".

---

## Checklist de verificación contra el enunciado (para revisar al final)

- [ ] Ingesta asíncrona vía broker — Fase 4
- [ ] Dos instancias en docker-compose, ambas consumiendo — Fase 7
- [ ] Secuencia garantizada por orden, paralelismo entre órdenes — Fase 4 (particionado)
- [ ] Idempotencia ante ER duplicado/reentregado — Fase 3
- [ ] Entidad orden + ledger con key autoincremental — Fase 1
- [ ] Estado calculado desde el persistido, no sobreescritura ciega — Fase 2 y 3
- [ ] Endpoint `GET /orders/{id}` con estado + ledger ordenado — Fase 6
- [ ] Recuperación ante caída de instancia — Fase 4 (commit de offset) + Fase 7 (prueba real)
- [ ] Errores transitorios vs permanentes, sin descarte silencioso — Fase 4
- [ ] Settlement exactamente una vez, no en CANCELLED — Fase 5
- [ ] Script/endpoint de seed con intercalado y duplicados — Fase 8
- [ ] README con pasos de reproducción — Fase 9
- [ ] DECISIONS.md completo — Fase 9
- [ ] Tests focalizados en lo crítico, no cobertura amplia — Fases 2, 3, 4, 5
