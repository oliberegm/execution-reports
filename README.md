# Servicio de Procesamiento de ExecutionReports

Solución completa para el consumo, procesamiento idempotente y orquestación de **ExecutionReports (ER)** en un entorno de alta disponibilidad multinodo.

---

## 🚀 Guía de Inicio Rápido

### Requisitos Previos
- **Docker** & **Docker Compose**
- **Java 21** y **Gradle** (para ejecutar la suite de pruebas localmente)

---

## 🛠️ 1. Levantar el Entorno Multinodo

Ejecutar en la raíz del proyecto:

```bash
# Compilar el JAR y levantar la infraestructura (PostgreSQL, Kafka, service-a, service-b)
./gradlew bootJar
docker compose up --build -d
```

Para verificar que todos los servicios estén saludables (`healthy`):

```bash
docker compose ps
```

Deberás ver los contenedores `postgres`, `kafka`, `kafka-init`, `service-a` (puerto 8080) y `service-b` (puerto 8081) en estado `healthy` / `running`.

---

## 🌾 2. Sembrar Stream de Prueba

Disparar la siembra de eventos mediante el script ejecutable:

```bash
./scripts/seed.sh
```

El script genera 5 órdenes intercaladas con ejecuciones `NEW`, `PARTIALLY_FILLED`, `FILLED` y `CANCELLED`, inyectando intencionalmente duplicados de `fix_id` para validar la regla de idempotencia.

---

## 🔍 3. Consultar Estado e Histórico de Órdenes

Puedes consultar la información consolidada de cualquier orden enviando una petición HTTP `GET` a cualquiera de las dos instancias (`8080` o `8081`):

```bash
# Consultar orden por numericOrderId en service-a (puerto 8080)
curl -s http://localhost:8080/orders/90001 | jq .

# O en service-b (puerto 8081)
curl -s http://localhost:8081/orders/90002 | jq .
```

### Ejemplo de Respuesta JSON:
```json
{
  "numericOrderId": 90001,
  "marketOrderId": "MKT-90001",
  "ticker": "GGAL",
  "side": "BUY",
  "securityType": "COMMON_STOCK",
  "status": "FILLED",
  "orderPrice": 11.00,
  "nominalAmounts": 100.00,
  "leavesNominalAmount": 0.00,
  "accumulativeNominalAmount": 100.00,
  "avgPrice": 11.00,
  "executionsAppliedCount": 2,
  "lastAppliedFixId": 9000102,
  "createdAt": "2026-07-27T16:00:00",
  "updatedAt": "2026-07-27T16:00:01",
  "ledger": [
    {
      "id": 1,
      "numericOrderId": 90001,
      "fixId": 9000101,
      "status": "NEW",
      "payload": { ... },
      "appliedAt": "2026-07-27T16:00:00"
    },
    {
      "id": 2,
      "numericOrderId": 90001,
      "fixId": 9000102,
      "status": "FILLED",
      "payload": { ... },
      "appliedAt": "2026-07-27T16:00:01"
    }
  ]
}
```

---

## 🧪 4. Reproducción de los Escenarios Clave

### Escenario A: Procesamiento Intercalado y Reparto de Particiones
- **Objetivo**: Confirmar que Kafka balancea las 6 particiones del topic `execution-reports` entre `service-a` y `service-b`.
- **Pasos**:
  1. Ejecutar `./scripts/seed.sh`
  2. Revisar los logs de ambas instancias:
     ```bash
     docker compose logs service-a | grep "ExecutionReportProcessor"
     docker compose logs service-b | grep "ExecutionReportProcessor"
     ```
  3. Verás que algunas órdenes fueron procesadas por `service-a` y otras por `service-b`, manteniendo la secuencia dentro de cada orden.

### Escenario B: Idempotencia ante Duplicados
- **Objetivo**: Confirmar que reentregar un `ExecutionReport` con un `fix_id` idéntico no muta la orden ni duplica entradas en el ledger.
- **Pasos**:
  1. Ejecutar `./scripts/seed.sh` (inyecta duplicados de `fix_id` automáticamente).
  2. Consultar la orden sembrada (ej. `90001`):
     ```bash
     curl -s http://localhost:8080/orders/90001 | jq .
     ```
  3. Verificar que `executionsAppliedCount` refleja exactamente el número de ejecuciones únicas recibidas y la longitud del `ledger` coincide sin duplicados.

### Escenario C: Prueba de Caída y Recuperación (*Failover*)
- **Objetivo**: Garantizar cero pérdidas y cero duplicación de eventos si una de las dos instancias cae abruptamente a mitad de procesamiento.
- **Pasos**:
  1. Disparar una siembra de datos:
     ```bash
     ./scripts/seed.sh 20
     ```
  2. Matar inmediatamente la instancia `service-a`:
     ```bash
     docker compose kill service-a
     ```
  3. Consultar las órdenes en la instancia superviviente `service-b` (puerto 8081):
     ```bash
     curl -s http://localhost:8081/orders/90001 | jq .status
     ```
  4. Reiniciar `service-a`:
     ```bash
     docker compose start service-a
     ```
  5. Verificar que ambas instancias responden correctamente y las órdenes alcanzaron su estado final consistente sin pérdidas ni duplicados.

---

## 🧪 5. Ejecución de Pruebas Automatizadas

Para ejecutar la suite completa de pruebas unitarias y de integración de Testcontainers (PostgreSQL 16 + EmbeddedKafka):

```bash
cd service
./gradlew test --info
```

---

## 📄 Documentación Adicional
- **[DECISIONS.md](file:///Users/olibergarcia/proyectos/olibersystem/project-ia/execution-reports/DECISIONS.md)**: Justificación detallada de cada decisión arquitectónica (Kafka, PostgreSQL, Transactional Outbox, Idempotencia y Resiliencia).
- **[TASKS.md](file:///Users/olibergarcia/proyectos/olibersystem/project-ia/execution-reports/TASKS.md)**: Plan de tareas completado fase por fase.
