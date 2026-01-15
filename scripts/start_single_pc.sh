#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# start_single_pc.sh - Levanta TODO el cluster en una sola PC para testing
# ==============================================================================
# Uso: bash scripts/start_single_pc.sh
# Para tumbar: bash scripts/stop_single_pc.sh
# ==============================================================================

NETWORK=${NETWORK:-agenda_net}
SELF_IP=$(hostname -I | awk '{print $1}')

echo "=============================================="
echo " AGENDA DISTRIBUIDA - Single PC Test Mode"
echo "=============================================="
echo "IP: $SELF_IP"
echo ""

docker rm -f coordinator_1 coordinator_2 coordinator_3 frontend \
  raft_events_am_1 raft_events_am_2 raft_events_am_3 \
  raft_events_nz_1 raft_events_nz_2 raft_events_nz_3 \
  raft_groups_1 raft_groups_2 raft_groups_3 \
  raft_users_1 raft_users_2 raft_users_3 2>/dev/null || true

if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
  docker network create "$NETWORK"
  echo "Red $NETWORK creada"
fi

run_raft_node() {
  local name=$1 port=$2 shard=$3 peers=$4
  docker run -d --name "$name" --hostname "$name" --network "$NETWORK" -p "${port}:${port}" \
    -e PYTHONPATH="/app:/app/backend" \
    -e SHARD_NAME="$shard" \
    -e NODE_ID="$name" \
    -e NODE_URL="http://${name}:${port}" \
    -e PORT="$port" \
    -e PEERS="$peers" \
    -e COORD_URL="http://coordinator:8700" \
    agenda_backend uvicorn distributed.nodes.raft_node:app --host 0.0.0.0 --port "$port"
}

echo "Lanzando nodos EVENTOS_A_M..."
run_raft_node raft_events_am_1 8801 EVENTOS_A_M "http://raft_events_am_2:8802,http://raft_events_am_3:8803"
run_raft_node raft_events_am_2 8802 EVENTOS_A_M "http://raft_events_am_1:8801,http://raft_events_am_3:8803"
run_raft_node raft_events_am_3 8803 EVENTOS_A_M "http://raft_events_am_1:8801,http://raft_events_am_2:8802"

echo "Lanzando nodos EVENTOS_N_Z..."
run_raft_node raft_events_nz_1 8804 EVENTOS_N_Z "http://raft_events_nz_2:8805,http://raft_events_nz_3:8806"
run_raft_node raft_events_nz_2 8805 EVENTOS_N_Z "http://raft_events_nz_1:8804,http://raft_events_nz_3:8806"
run_raft_node raft_events_nz_3 8806 EVENTOS_N_Z "http://raft_events_nz_1:8804,http://raft_events_nz_2:8805"

echo "Lanzando nodos GRUPOS..."
run_raft_node raft_groups_1 8807 GRUPOS "http://raft_groups_2:8808,http://raft_groups_3:8809"
run_raft_node raft_groups_2 8808 GRUPOS "http://raft_groups_1:8807,http://raft_groups_3:8809"
run_raft_node raft_groups_3 8809 GRUPOS "http://raft_groups_1:8807,http://raft_groups_2:8808"

echo "Lanzando nodos USUARIOS..."
run_raft_node raft_users_1 8810 USUARIOS "http://raft_users_2:8811,http://raft_users_3:8812"
run_raft_node raft_users_2 8811 USUARIOS "http://raft_users_1:8810,http://raft_users_3:8812"
run_raft_node raft_users_3 8812 USUARIOS "http://raft_users_1:8810,http://raft_users_2:8811"

echo "Esperando 3s para que los nodos inicien..."
sleep 3

echo "Lanzando coordinadores (3 instancias para HA)..."

# Coordinador 1
docker run -d --name coordinator_1 --hostname coordinator_1 --network "$NETWORK" \
  -p 8700:8700 -p 8767:8767 \
  -e PYTHONPATH="/app:/app/backend" \
  -e DISABLE_DEFAULT_SHARDS=0 \
  -e SELF_COORD_URL="http://coordinator_1:8700" \
  -e COORD_PEERS="http://coordinator_2:8701,http://coordinator_3:8702" \
  -e WEBSOCKET_PORT=8767 \
  agenda_backend uvicorn distributed.coordinator.router:app --host 0.0.0.0 --port 8700

# Coordinador 2
docker run -d --name coordinator_2 --hostname coordinator_2 --network "$NETWORK" \
  -p 8701:8701 -p 8768:8768 \
  -e PYTHONPATH="/app:/app/backend" \
  -e DISABLE_DEFAULT_SHARDS=0 \
  -e SELF_COORD_URL="http://coordinator_2:8701" \
  -e COORD_PEERS="http://coordinator_1:8700,http://coordinator_3:8702" \
  -e WEBSOCKET_PORT=8768 \
  agenda_backend uvicorn distributed.coordinator.router:app --host 0.0.0.0 --port 8701

# Coordinador 3
docker run -d --name coordinator_3 --hostname coordinator_3 --network "$NETWORK" \
  -p 8702:8702 -p 8769:8769 \
  -e PYTHONPATH="/app:/app/backend" \
  -e DISABLE_DEFAULT_SHARDS=0 \
  -e SELF_COORD_URL="http://coordinator_3:8702" \
  -e COORD_PEERS="http://coordinator_1:8700,http://coordinator_2:8701" \
  -e WEBSOCKET_PORT=8769 \
  agenda_backend uvicorn distributed.coordinator.router:app --host 0.0.0.0 --port 8702

echo "Esperando 2s para coordinadores..."
sleep 2

echo "Lanzando frontend..."
docker run -d --name frontend --network "$NETWORK" \
  -p 8501:8501 \
  -e PYTHONPATH="/app/front:/app" \
  -e API_BASE_URL="http://coordinator_1:8700" \
  -e API_BASE_URLS="http://coordinator_1:8700,http://coordinator_2:8701,http://coordinator_3:8702" \
  -e WEBSOCKET_HOST="coordinator_1" \
  -e WEBSOCKET_PORT="8767" \
  agenda_frontend streamlit run front/app.py --server.port=8501 --server.address=0.0.0.0

echo ""
echo "=============================================="
echo " CLUSTER LISTO (3 Coordinadores HA)"
echo "=============================================="
echo "Frontend:      http://localhost:8501"
echo ""
echo "Coordinadores:"
echo "  - Coord 1:   http://localhost:8700  (WS: ws://localhost:8767)"
echo "  - Coord 2:   http://localhost:8701  (WS: ws://localhost:8768)"
echo "  - Coord 3:   http://localhost:8702  (WS: ws://localhost:8769)"
echo ""
echo "Verificar lideres: curl http://localhost:8700/leaders"
echo "Verificar estado:  curl http://localhost:8700/cluster/status"
echo "Circuit Breaker:   curl http://localhost:8700/circuit-breaker/status"
echo "Peers conocidos:   curl http://localhost:8700/coordinators/peers"
echo ""
echo "Para tumbar un coordinador: docker stop coordinator_1"
echo "Para tumbar un nodo RAFT:   docker stop raft_events_am_1"
echo "Para parar todo:            bash scripts/stop_single_pc.sh"
echo "=============================================="

