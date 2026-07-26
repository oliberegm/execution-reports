# AGENT.md

Este documento es el contexto persistente para el agente de IA que va a implementar el proyecto.
Léelo completo antes de tocar código. Cuando haya conflicto entre lo que parece "más prolijo" y lo
que dice este documento, **gana este documento**. Cuando haya ambigüedad no cubierta acá, tomá la
decisión más simple que cumpla la garantía pedida, and documentala en `DECISIONS.md` — no hace falta
preguntar para cada detalle menor.

---

## 1. Qué es este proyecto

Un servicio backend que consume, de forma asíncrona y desde dos instancias en paralelo, un stream de
`ExecutionReport` (ER) — snapshots del estado de órdenes de mercado — y mantiene el estado de cada
orden **siempre correcto**, incluso ante reentregas, mensajes intercalados de distintas órdenes, y
caída de instancias. Cuando una orden llega a `FILLED`, se publica un evento de *settlement* exactamente
una vez.

Es un challenge técnico de arquitectura. **El objetivo no es un sistema grande**: es un servicio chico
con las garantías difíciles (secuencia, idempotencia, recuperación de fallas, settlement exactamente
una vez) resueltas de forma explícita y verificable. Preferí siempre menos código bien razonado por
sobre más código con features no pedidas.

El diseño completo de la solución ya está decidido y documentado en `solucion-diseno.md` y el plan de
trabajo fase por fase en `plan-implementacion.md` (equivalente a `TASKS.md`). Este archivo no repite
esas decisiones en detalle: las referencia y fija cómo trabajar sobre ellas.

---

## 2. Stack fijo (no renegociable sin razón fuerte y documentada)

- Java 21, Spring Boot.
- Kafka como broker (particionado por `numericOrderId`).
- PostgreSQL como persistencia.
- Testcontainers para tests de integración (Postgres y Kafka reales, no mocks/H2).
- Docker Compose para orquestar todo (2 instancias del servicio + Kafka + Postgres).
- Build tool: Gradle (si no hay una razón para usar Maven, usar Gradle — más simple para
  multi-módulo si hiciera falta).
- Migraciones de esquema: Flyway.

No agregar librerías nuevas para resolver algo que Spring/Kafka/Postgres ya resuelven de forma
directa. Si aparece la tentación de sumar una dependencia, primero preguntarse si el problema se
resuelve con SQL simple o con la API estándar de Kafka.

---

## 3. Reglas de dominio que el código tiene que reflejar sin excepción

Estas son las garantías centrales del challenge. Cualquier implementación que las viole está mal,
sin importar qué tan "limpia" se vea el código:

1. **Secuencia por orden, paralelismo entre órdenes**: se logra particionando el topic Kafka por
   `numericOrderId`. Nunca se debe introducir un mecanismo adicional de locking distribuido para
   esto — el particionado ya lo resuelve. Ver `solucion-diseno.md` §4.
2. **Idempotencia**: la clave de dedup es `fixId`, único por ER, con constraint `UNIQUE` en
   `order_ledger.fix_id`. Un ER duplicado nunca debe mutar `orders`. Ver §6.
3. **El estado de una orden se calcula desde el estado ya persistido + el ER entrante**, nunca por
   sobreescritura directa del último ER. La función que hace esto (`OrderStateMachine` o equivalente)
   tiene que ser una función pura, testeable sin Spring ni base de datos.
4. **Transiciones sobre estado terminal** (`FILLED`/`CANCELLED`) se rechazan explícitamente, nunca se
   aplican silenciosamente ni se ignoran sin dejar rastro.
5. **Orden "primero base, después offset"**: el offset de Kafka se commitea únicamente después de que
   la transacción de base de datos correspondiente al ER cerró con éxito. Esto es lo que garantiza
   "no se pierde ni se aplica dos veces" ante una caída de instancia.
6. **Settlement exactamente una vez**: vía outbox transaccional (insert en `settlement_outbox` en la
   misma transacción que el update de `orders` a `FILLED`) + relay separado + dedup downstream por
   `numericOrderId` como key. `CANCELLED` nunca genera settlement.
7. **Errores permanentes van a DLQ, nunca se descartan en silencio.** Errores transitorios se
   reintentan con backoff acotado, sin bloquear indefinidamente la partición.

Si en algún punto de la implementación una de estas reglas obliga a un código más feo o más largo que
la alternativa "limpia" — gana la regla. La correctitud de estas garantías es lo que se evalúa.

---

## 4. Estándares de código (clean code / buenas prácticas)

### Principios generales
- **Funciones pequeñas, con un solo motivo para cambiar.** Si una función mezcla "decidir" con
  "persistir" con "publicar a Kafka", separala. El caso paradigmático es el flujo de aplicar un ER:
  la decisión de transición (dominio puro) debe estar separada de la persistencia (repositorio) y de
  la infraestructura de mensajería (listener de Kafka).
- **Nombres que dicen qué hacen, no cómo lo hacen.** `applyExecutionReport(...)`, no
  `processMsg(...)`. Evitar abreviaturas salvo las del dominio ya establecidas en el enunciado
  (`ER`, `fixId`, `numericOrderId`).
- **Sin lógica de negocio en los controllers ni en los listeners de Kafka.** Estos son adaptadores:
  deserializan, delegan al dominio/servicio de aplicación, manejan la respuesta/ack. La lógica vive en
  una capa de dominio sin dependencias de framework.
- **Inmutabilidad donde no hay razón para lo contrario.** DTOs y objetos de dominio como `record` de
  Java siempre que sea posible. Las entidades JPA mutables se limitan a lo estrictamente necesario
  para el ORM.
- **Excepciones con intención**: distinguir explícitamente (con tipos de excepción propios, no
  `RuntimeException` genérica) entre error transitorio y error permanente. El manejo de Kafka decide
  qué hacer según el tipo, no según el mensaje de la excepción.
- **Nada de comentarios que expliquen "qué" hace una línea obvia.** Comentarios solo para "por qué"
  cuando la razón no es evidente por el código (por ejemplo, por qué se elige `SELECT FOR UPDATE` en
  vez de optimistic locking en un punto puntual).
- **No premature optimization ni over-engineering**: no armar interfaces/abstracciones "por si
  después hay otro broker" o "por si después hay otra base". Este es un servicio chico y acotado;
  las abstracciones se justifican solo si hay más de una implementación real dentro del alcance del
  challenge.

### Estructura de paquetes sugerida
```
com.maxcapital.executionreports
├── domain/              # OrderStateMachine, Order, ExecutionReport (modelo), reglas puras. Sin Spring.
├── application/         # ExecutionReportProcessor: orquesta dominio + persistencia + outbox, transaccional.
├── infrastructure/
│   ├── kafka/           # listener, DLQ publisher, configuración de consumer/producer
│   ├── persistence/     # entidades JPA/JDBC, repositorios, migraciones Flyway
│   └── outbox/          # OutboxRelay
├── api/                 # controller HTTP (GET /orders/{id})
└── testsupport/         # generador de seed / endpoint de test
```
La capa `domain` no debe importar nada de Spring, Kafka ni JPA. Es la parte que más se testea y la
que menos debería cambiar si mañana cambia el broker o el motor de base.

### Testing
- Tests unitarios de `domain` sin Spring context (rápidos, sin `@SpringBootTest`).
- Tests de integración con Testcontainers para todo lo que toca Postgres o Kafka real.
- No testear getters/setters, mappers triviales, ni configuración de Spring. Ver `plan-implementacion.md`
  §12 (equivalente) para la lista priorizada de qué testear.
- Cada test tiene que poder explicar, por su nombre, qué garantía está protegiendo (ej.
  `duplicateFixId_doesNotIncrementExecutionCount`, no `test3`).

### Logging
- Log estructurado (no solo `System.out`/strings sueltas) en los puntos de decisión importantes:
  ER rechazado por estado terminal, ER enviado a DLQ, settlement publicado. Incluir siempre
  `numericOrderId` y `fixId` en el contexto del log para poder trazar un caso puntual.
- No loguear el payload completo del ER en nivel INFO por cada mensaje (ruido); sí en DEBUG.

### Commits
- Commits chicos y descriptivos, uno por unidad de trabajo coherente (no "wip", no un commit gigante
  al final). El historial de commits es parte de lo que se evalúa según el enunciado original — tiene
  que mostrar el proceso de construcción, no un squash final.

---

## 5. Qué NO construir (restricción explícita del challenge)

- Sin autenticación/autorización.
- Sin UI.
- Sin features extra no pedidas (no hay pedido de cancelar órdenes, no hay pedido de listar todas las
  órdenes, no hay pedido de métricas/observabilidad avanzada).
- Sin hardening de producción (rate limiting, seguridad de red, TLS entre servicios, etc.).
- Si en algún momento la implementación de algo pedido se vuelve compleja al punto de no ser "core",
  preferí resolver una versión simple y anotar el trade-off en `DECISIONS.md`, en vez de construir la
  versión completa.

---

## 6. Cómo trabajar: proceso

1. Trabajar **siguiendo el orden de fases de `plan-implementacion.md` / `TASKS.md`**, sin saltear a
   Kafka antes de que la Fase 2 y 3 (dominio + transacción) estén sólidas y testeadas. Ese orden no es
   arbitrario: cada fase depende de que la anterior esté verificada.
2. Al terminar cada fase, correr toda la suite de tests antes de avanzar a la siguiente.
3. Si una decisión de diseño no está cubierta por `solucion-diseno.md`, tomar la opción más simple que
   cumpla la garantía correspondiente, e inmediatamente anotarla en `DECISIONS.md` con una frase de
   justificación. No dejar decisiones tácitas sin registrar.
4. Actualizar `TASKS.md` marcando cada tarea como completada a medida que se cierra (no al final en
   bloque), para que el estado del checklist siempre refleje la realidad del repo.
5. Si al implementar aparece que una garantía del §3 de este documento no se puede cumplir con el
   approach elegido, **parar y replantear el approach de esa fase** — no avanzar con una garantía
   rota documentándola como "trade-off", porque esas garantías son justamente lo que se evalúa, a
   diferencia de las features accesorias que sí se pueden recortar.

---

## 7. Definition of Done (aplica a cada tarea de TASKS.md)

Una tarea se considera terminada solo si:
- El código compila y los tests correspondientes pasan (unitarios y, si aplica, de integración con
  Testcontainers).
- No introduce warnings nuevos del compilador ni del linter configurado.
- Está commiteada con un mensaje que describe la unidad de trabajo.
- Si la tarea toca una de las garantías del §3, hay al menos un test que la verifica explícitamente
  por nombre.
- Si la tarea implicó una decisión no trivial, quedó anotada en `DECISIONS.md`.
