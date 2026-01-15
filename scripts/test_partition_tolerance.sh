#!/usr/bin/env bash
set -euo pipefail

echo "=============================================="
echo " Test de Tolerancia a Particiones"
echo "=============================================="

COORD=${COORD_URL:-http://localhost:8700}

check_leaders() {
  echo ""
  echo "Estado de lideres:"
  curl -s "$COORD/leaders" | python3 -m json.tool 2>/dev/null || curl -s "$COORD/leaders"
}

check_circuit_breaker() {
  echo ""
  echo "Estado del Circuit Breaker:"
  curl -s "$COORD/circuit-breaker/status" | python3 -m json.tool 2>/dev/null || curl -s "$COORD/circuit-breaker/status"
}

check_node_state() {
  local port=$1
  echo ""
  echo "Estado nodo puerto $port:"
  curl -s "http://localhost:$port/raft/state" | python3 -m json.tool 2>/dev/null || echo "Nodo no disponible"
}

test_failover() {
  local node=$1
  echo ""
  echo "=== TEST: Tumbar $node y verificar failover ==="
  
  echo "1. Estado antes:"
  check_leaders
  
  echo ""
  echo "2. Tumbando $node..."
  docker stop "$node"
  
  echo ""
  echo "3. Esperando 5s para eleccion..."
  sleep 5
  
  echo ""
  echo "4. Estado despues:"
  check_leaders
  
  echo ""
  echo "5. Levantando $node de nuevo..."
  docker start "$node"
  
  echo ""
  echo "6. Esperando 3s para sincronizacion..."
  sleep 3
  
  echo ""
  echo "7. Estado final:"
  check_leaders
}

echo "Verificando conectividad con coordinador..."
if ! curl -s "$COORD/health" > /dev/null; then
  echo "ERROR: No se puede conectar al coordinador en $COORD"
  echo "Asegurate de ejecutar primero: bash scripts/start_single_pc.sh"
  exit 1
fi

echo "Coordinador OK!"

case "${1:-menu}" in
  leaders)
    check_leaders
    ;;
  circuit)
    check_circuit_breaker
    ;;
  node)
    check_node_state "${2:-8801}"
    ;;
  failover)
    test_failover "${2:-raft_events_am_1}"
    ;;
  full)
    echo "=== TEST COMPLETO ==="
    check_leaders
    check_circuit_breaker
    echo ""
    echo "=== Probando failover de raft_events_am_1 ==="
    test_failover raft_events_am_1
    echo ""
    echo "=== Probando failover de raft_users_1 ==="
    test_failover raft_users_1
    echo ""
    echo "TEST COMPLETO FINALIZADO"
    ;;
  *)
    echo ""
    echo "Uso:"
    echo "  bash scripts/test_partition_tolerance.sh leaders   - Ver lideres"
    echo "  bash scripts/test_partition_tolerance.sh circuit   - Ver circuit breaker"
    echo "  bash scripts/test_partition_tolerance.sh node 8801 - Ver estado de nodo"
    echo "  bash scripts/test_partition_tolerance.sh failover raft_events_am_1 - Test failover"
    echo "  bash scripts/test_partition_tolerance.sh full      - Test completo"
    ;;
esac
