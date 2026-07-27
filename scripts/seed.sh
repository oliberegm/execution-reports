#!/usr/bin/env bash
set -e

HOST="${1:-http://localhost:8080}"
ORDERS_COUNT="${2:-5}"

echo "============================================================"
echo "  Siembra de Stream de Prueba (Execution Reports)"
echo "  Target Host: $HOST"
echo "  Ordenes: $ORDERS_COUNT"
echo "============================================================"

RESPONSE=$(curl -s -X POST "$HOST/test/seed?ordersCount=$ORDERS_COUNT&injectDuplicates=true")

echo "Respuesta del Servidor:"
echo "$RESPONSE"
echo ""
echo "Siembra completada exitosamente."
