# Execution Reports Service

Servicio de procesamiento de ExecutionReports para Max Capital.

## Requisitos

- Java 21
- Docker & Docker Compose
- Gradle

## Levantar el stack

```bash
# Compilar el servicio
cd service && ./gradlew clean build -x test && cd ..

# Levantar todo (Postgres + Kafka + Service)
docker compose up --build -d

# Verificar health
curl http://localhost:8080/health
```

## Ejecutar tests

```bash
cd service
./gradlew test
```

## Escenarios de prueba

> TODO: se completará en Fase 8-9
