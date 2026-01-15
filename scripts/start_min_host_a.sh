#!/usr/bin/env bash
set -euo pipefail

# Despliegue mínimo en Host A:
# - Coordinador y frontend A
# - Nodos únicos: eventos A-M y grupos
# Host B corre eventos N-Z y usuarios (start_min_host_b.sh)
#
# Requiere: HOST_B_IP apuntando al otro host.
# Opcionales:
#   NETWORK     (default agenda_net)
#   FRONT_PORT  (default 8501)
#   WS_PORT     (default 8768, mapea al 8767 interno del coordinador)

unset SHARDS_CONFIG_JSON SHARD_GROUPS SHARD_GRUPOS SHARD_USERS SHARD_USUARIOS
unset SHARD_EVENTOS_A_M SHARD_EVENTOS_N_Z SHARD_EVENTS_A_M SHARD_EVENTS_N_Z

docker rm -f coordinator frontend_a \
  raft_events_am_1 raft_groups_1 2>/dev/null || true

HOST_B_IP=${HOST_B_IP:-}
NETWORK=${NETWORK:-agenda_net}
FRONT_PORT=${FRONT_PORT:-8501}
WS_PORT=${WS_PORT:-8768}

# Lista de coordinadores para el frontend (failover simple)
API_URLS_RAW=${FRONT_API_URLS:-"http://coordinator:8700"}
API_BASE_URLS_CONTAINER=""
IFS=',' read -ra API_URLS_ARR <<<"$API_URLS_RAW"
for url in "${API_URLS_ARR[@]}"; do
  url=$(echo "$url" | xargs)
  [[ -z "$url" ]] && continue
  API_BASE_URLS_CONTAINER+="${API_BASE_URLS_CONTAINER:+,}${url}"
done
if [[ -z "$API_BASE_URLS_CONTAINER" ]]; then
  API_BASE_URLS_CONTAINER="http://coordinator:8700"
fi
PRIMARY_API_BASE_URL=${API_BASE_URLS_CONTAINER%%,*}
WS_HOST=$(echo "$PRIMARY_API_BASE_URL" | sed -E 's#^https?://([^/:]+).*#\1#')
WEBSOCKET_PORT_CONTAINER=8767

if [[ -z "$HOST_B_IP" ]]; then
  echo "❌ Debes exportar HOST_B_IP. Ej: HOST_B_IP=192.168.171.147" >&2
  exit 1
fi

echo "➡️ Host A usando HOST_B_IP=$HOST_B_IP"

if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
  docker network create --driver overlay --attachable "$NETWORK" || docker network create "$NETWORK"
fi

SELF_IP=$(hostname -I | awk '{print $1}')
echo "🌐 Red: $NETWORK | IP local: $SELF_IP | Host B: $HOST_B_IP"

run_node() {
  local name=$1 port=$2 shard=$3
  docker run -d --name "$name" --hostname "$name" --network "$NETWORK" -p "${port}:${port}" \
    -v "${name}_data":/app/data \
    -e PYTHONPATH="/app:/app/backend" \
    -e SHARD_NAME="$shard" \
    -e NODE_ID="$name" \
    -e NODE_URL="http://${name}:${port}" \
    -e PORT="$port" \
    -e PEERS="" \
    -e COORD_URL="http://coordinator:8700" \
    agenda_backend uvicorn distributed.nodes.raft_node:app --host 0.0.0.0 --port "$port"
}

echo "🚀 Lanzando nodos mínimos en Host A..."
run_node raft_events_am_1 8801 EVENTOS_A_M
run_node raft_groups_1    8807 GRUPOS

echo "🎯 Lanzando coordinador principal..."
docker rm -f coordinator 2>/dev/null || true
docker run -d --name coordinator --network "$NETWORK" \
  -p 8700:8700 -p ${WS_PORT}:8767 \
  -e PYTHONPATH="/app:/app/backend" \
  -e SHARDS_CONFIG_JSON="" \
  -e SHARD_EVENTOS_A_M="http://raft_events_am_1:8801" \
  -e SHARD_EVENTOS_N_Z="http://raft_events_nz_1:8804" \
  -e SHARD_GROUPS="http://raft_groups_1:8807" \
  -e SHARD_USERS="http://raft_users_1:8810" \
  agenda_backend uvicorn distributed.coordinator.router:app --host 0.0.0.0 --port 8700

echo "🎨 Lanzando frontend en Host A..."
docker rm -f frontend_a 2>/dev/null || true
docker run -d --name frontend_a --hostname frontend_a --network "$NETWORK" \
  -p ${FRONT_PORT}:8501 \
  -e PYTHONPATH="/app/front:/app" \
  -e API_BASE_URL=${PRIMARY_API_BASE_URL} \
  -e API_BASE_URLS=${API_BASE_URLS_CONTAINER} \
  -e WEBSOCKET_HOST=${WS_HOST:-coordinator} \
  -e WEBSOCKET_PORT=${WEBSOCKET_PORT_CONTAINER} \
  agenda_frontend streamlit run front/app.py --server.port=8501 --server.address=0.0.0.0

echo "✅ Host A mínimo listo. Front: http://${SELF_IP}:${FRONT_PORT}"
