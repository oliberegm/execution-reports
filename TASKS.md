# TASKS.md

Checklist accionable para el agente. Ejecutar **en orden**: cada fase asume que la anterior está
verificada (tests corriendo en verde) antes de empezar. Marcar cada casillero al cerrar la tarea, no
en bloque al final. Ante cualquier ambigüedad, ver `AGENT.md` §6.

Convención: cada tarea indica su **criterio de aceptación** — no se marca como hecha si no lo cumple.

---

## Fase 0 — Setup del repo

- [ ] Crear estructura de carpetas: `/service`, `/docker-compose.yml`, `/README.md`,
      `/DECISIONS.md`, `/scripts` (o donde viva el generador de seed).
      **Acepta:** estructura commiteada, README con un placeholder.
- [ ] Inicializar proyecto Spring Boot (Java 21, Gradle) con dependencias base: `web`, `spring-kafka`,
      `data-jpa` (o jdbc), driver Postgres, `spring-boot-starter-test`, `testcontainers`.
      **Acepta:** `./gradlew build` corre sin errores.
- [ ] Endpoint `GET /health` trivial.
      **Acepta:** responde 200 corriendo local (`./gradlew bootRun`).
- [ ] `docker-compose.yml` mínimo: `postgres`, `kafka` (KRaft, sin zookeeper si se puede simplificar
      así), una sola instancia del servicio.
      **Acepta:** `docker compose up` deja todo arriba, `curl localhost:PORT/health` responde 200.
- [ ] Commit inicial.

---

## Fase 1 — Modelo de datos y persistencia

- [ ] Migración Flyway: tabla `orders` (numeric_order_id PK, status, leaves_nominal_amount,
      accumulative_nominal_amount, avg_price, executions_applied_count, last_applied_fix_id, version,
      updated_at).
- [ ] Migración Flyway: tabla `order_ledger` (id BIGSERIAL PK, numeric_order_id FK, fix_id UNIQUE NOT
      NULL, payload JSONB, applied_at) + índice sobre `(numeric_order_id, id)`.
- [ ] Migración Flyway: tabla `settlement_outbox` (id, numeric_order_id, payload, status, created_at,
      sent_at).
      **Acepta:** `docker compose up` corre las migraciones sin error; se puede verificar el esquema
      con `\d orders` etc. desde `psql`.
- [ ] Entidades/DTOs de persistencia (JPA o JDBC directo — decisión anotada en `DECISIONS.md`).
- [ ] Repositorios: `OrderRepository`, `OrderLedgerRepository`, `SettlementOutboxRepository`, con
      los métodos mínimos necesarios (buscar por id, insertar, actualizar, listar ledger ordenado).
- [ ] Test de integración (Testcontainers, Postgres real): insertar una orden, insertar dos filas de
      ledger con distinto `fix_id` → ok; insertar dos filas con el **mismo** `fix_id` → la segunda
      falla por constraint único.
      **Acepta:** test verde, corre contra Postgres real vía Testcontainers, no contra H2.

---

## Fase 2 — Dominio: máquina de estados de la orden

- [ ] Modelar `ExecutionReport` y `Order` como tipos de dominio puros (records donde aplique), sin
      dependencias de Spring/JPA.
- [ ] Implementar `OrderStateMachine.apply(currentState (nullable), incomingER)` con las reglas de
      `AGENT.md` §3 punto 3 y 4:
      - orden inexistente + ER no-`NEW` → rechazo (huérfano).
      - orden inexistente + ER `NEW` → crea.
      - orden en estado terminal (`FILLED`/`CANCELLED`) + cualquier ER → rechazo.
      - transición válida → nuevo estado con `executionsAppliedCount + 1`, campos snapshot
        actualizados desde el ER.
      - nuevo estado `FILLED` → resultado indica explícitamente "corresponde settlement".
      - nuevo estado `CANCELLED` → resultado indica explícitamente "no corresponde settlement".
- [ ] Tests unitarios (sin Spring context) — mínimo, uno por caso:
      - `newOrder_fromNewER_createsOrder`
      - `partiallyFilledWithoutPriorNew_isRejected`
      - `fullSequence_newPartialFilled_resultsInCorrectFinalState`
      - `erOnAlreadyFilledOrder_isRejectedAndStateUnchanged`
      - `erOnAlreadyCancelledOrder_isRejectedAndStateUnchanged`
      - `transitionToFilled_marksSettlementRequired`
      - `transitionToCancelled_doesNotMarkSettlement`
      **Acepta:** todos los tests verdes, cero dependencia de Spring/DB/Kafka en esta capa.

---

## Fase 3 — Procesamiento transaccional (sin Kafka todavía)

- [x] Implementar `ExecutionReportProcessor.process(er)`:
      1. intenta insertar en `order_ledger` por `fix_id` → si falla por duplicado, no-op y retorna.
      2. si insertó: obtiene el estado actual de `orders` con locking (elegir `SELECT FOR UPDATE` u
         optimistic locking con `version` — anotar la elección en `DECISIONS.md`).
      3. invoca `OrderStateMachine.apply(...)`.
      4. si es rechazo → loguea la anomalía (con `numericOrderId` y `fixId`) y hace commit igual
         (no relanza excepción que gatille reintentos infinitos).
      5. si es válido → persiste el nuevo estado de `orders`.
      6. si corresponde settlement → inserta fila en `settlement_outbox`, misma transacción.
      Todo el método corre dentro de una única transacción (`@Transactional` o manejo explícito).
- [x] Test de integración: aplicar secuencia completa `NEW → PARTIALLY_FILLED → FILLED` → verificar
      estado final en `orders` y 3 filas en `order_ledger`.
- [x] Test de integración: aplicar el mismo ER (mismo `fix_id`) dos veces → `order_ledger` tiene una
      sola fila, `executions_applied_count` no se duplicó.
- [x] Test de integración: aplicar un ER sobre una orden ya `FILLED` → `orders` no cambia, pero el ER
      queda registrado como anomalía (según el mecanismo elegido: log estructurado o tabla).
- [x] Test de integración: `FILLED` inserta fila en `settlement_outbox`; `CANCELLED` no inserta nada.
- [x] Test de concurrencia: dos threads invocando `process(er)` con el **mismo** `fix_id` al mismo
      tiempo → el ledger termina con una sola fila (no dos por *race condition*).
      **Acepta:** todos los tests verdes contra Postgres real. Esta fase es la más crítica del
      proyecto — no avanzar a Fase 4 sin cobertura sólida acá.

---

## Fase 4 — Integración con Kafka

- [x] Crear topics (vía script de init o config del broker): `execution-reports` (≥6 particiones),
      `execution-reports.dlq`, `settlement`.
- [x] Configurar productor de prueba/consumidor con `key = numericOrderId` para
      `execution-reports`, garantizando el particionado correcto.
- [x] Listener de Kafka con `enable.auto.commit=false` y ack manual **después** de que
      `ExecutionReportProcessor.process(er)` retorna con éxito.
- [x] Manejo de errores diferenciado:
      - transitorio (ej. `DataAccessException` de conexión) → no ack, reintento con backoff acotado
        (`DefaultErrorHandler` de Spring Kafka, N intentos configurables).
      - permanente (deserialización inválida, o excepción de dominio marcada como no-recuperable) →
        tras agotar reintentos razonables, publicar en `execution-reports.dlq` con el motivo del
        error, y hacer ack del mensaje original.
- [x] Test de integración con Testcontainers Kafka: publicar ER intercalados de 3-4 órdenes distintas
      con un `fix_id` duplicado en el medio del stream → verificar estado final correcto de cada
      orden y ausencia del duplicado en el ledger.
- [x] Test de integración: publicar un mensaje deliberadamente inválido (JSON corrupto) → aparece en
      `execution-reports.dlq`, el consumidor sigue procesando los mensajes siguientes de esa misma
      partición sin bloquearse.
      **Acepta:** todos los tests verdes; verificar manualmente con `kafka-console-consumer` que los
      mensajes de una misma orden llegan siempre a la misma partición.

---

## Fase 5 — Relay de settlement

- [x] Implementar `OutboxRelay`: job programado (`@Scheduled`, intervalo corto) que lee filas
      `PENDING` de `settlement_outbox`, publica al topic `settlement` con `key = numericOrderId`, y
      marca `SENT` tras el ack del producer.
- [x] Configurar producer de Kafka con `enable.idempotence=true`.
- [x] Test de integración: forzar que una orden llegue a `FILLED` a través de una reentrega del ER
      final → verificar una sola fila en `settlement_outbox` y un solo mensaje en el topic
      `settlement` para esa orden.
- [x] Test de integración: simular fallo del relay entre publicar y marcar `SENT` (mockeando el
      repositorio o cortando la conexión a mitad) → al reintentar, no se pierde la fila ni queda
      publicada dos veces de forma no deduplicable (la key sigue siendo la misma orden).
      **Acepta:** tests verdes; verificar manualmente con un consumidor de `settlement` que no hay
      duplicados por `numericOrderId` tras una reentrega intencional.

---

## Fase 6 — Endpoint HTTP

- [x] `GET /orders/{numericOrderId}`: 404 si no existe; 200 con `status`,
      `executionsAppliedCount`, cantidades vigentes, y el detalle del ledger ordenado por `id ASC`.
- [x] Test de integración: sembrar una secuencia de ER vía Kafka (extremo a extremo), esperar
      procesamiento (con timeout/polling corto en el test), consultar el endpoint, verificar el JSON
      contra lo esperado.
      **Acepta:** test verde, JSON de respuesta incluye ledger en el orden correcto de inserción.

---

## Fase 7 — Dos instancias + prueba de caída

- [x] Completar `docker-compose.yml`: `service-a` y `service-b` (misma imagen, mismo `group.id` de
      Kafka), healthchecks, `depends_on` con condición para arranque confiable.
- [x] Verificar reparto de particiones: con `docker compose up`, confirmar (log o herramienta de
      Kafka) que las particiones del topic se dividen entre `service-a` y `service-b`.
- [x] Prueba documentada (manual, automatizada si el tiempo alcanza): sembrar carga → matar
      `service-a` a mitad de procesamiento (`docker compose kill service-a`) → verificar vía
      `/orders/{id}` en `service-b` que el estado sigue advancing correctamente → levantar
      `service-a` de nuevo → verificar que no hay duplicados ni pérdidas en ninguna orden afectada.
      **Acepta:** el procedimiento se puede reproducir siguiendo únicamente los pasos que van a
      quedar en el README (Fase 9), sin conocimiento previo del código.

---

## Fase 8 — Generador de stream de prueba

- [x] Implementar `POST /test/seed` (o script standalone — decisión anotada en `DECISIONS.md`) que:
      genera N órdenes con secuencias válidas de ER, intercala la publicación entre órdenes (no
      publica todos los ER de una orden seguidos), e inyecta 1-2 `fix_id` repetidos a propósito.
      **Acepta:** un solo comando (`curl` al endpoint o `./scripts/seed.sh`) deja el sistema con
      varias órdenes en distintos estados, verificable vía el endpoint de consulta.

---

## Fase 9 — Documentación final

- [ ] `README.md`: instrucciones de `docker compose up`, cómo sembrar datos, y pasos concretos para
      reproducir cada escenario clave (intercalado, duplicado, caída de instancia).
- [ ] `DECISIONS.md`: condensar `solucion-diseno.md` respondiendo punto por punto lo que pide el
      enunciado original (broker y por qué, estrategia de secuencia con 2 consumidores, idempotencia
      y clave de dedup, motor de persistencia y por qué, cómo se garantiza que el estado refleje la
      secuencia exacta, política de errores, garantía de settlement exactamente una vez, trade-offs
      dejados afuera).
- [ ] Revisión final: releer el enunciado original punto por punto contra el checklist de
      `plan-implementacion.md` §"Checklist de verificación" y marcar explícitamente en
      `DECISIONS.md` cualquier desvío consciente.
      **Acepta:** un tercero puede clonar el repo, seguir el README sin ayuda adicional, y reproducir
      los tres escenarios clave.

---

## Recordatorio de prioridad (ver AGENT.md §6.5)

Nunca recortar Fase 2 y Fase 3. Si el tiempo aprieta, recortar en este orden: automatización de la
prueba de caída (Fase 7) → CDC en vez de poller (Fase 5, ya se usa poller por defecto) → tabla propia
de DLQ (Fase 4, ya se usa topic Kafka por defecto) → cobertura de tests del endpoint HTTP (Fase 6).
