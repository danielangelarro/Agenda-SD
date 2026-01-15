#!/usr/bin/env bash
set -euo pipefail

# Despliegue mínimo en Host B:
# - Nodos únicos: eventos N-Z y usuarios
# - Frontend B apuntando al coordinador en Host A
#
# Requiere: COORD_IP con la IP del coordinador principal en Host A.
# Opcionales:
#   NETWORK     (default agenda_net)
#   FRONT_PORT  (default 8502)

COORD_IP=${COORD_IP:-}
NETWORK=${NETWORK:-agenda_net}
FRONT_PORT=${FRONT_PORT:-8502}

docker rm -f frontend_b \
  raft_events_nz_1 \
  raft_users_1 2>/dev/null || true

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

if [[ -z "$COORD_IP" ]]; then
  echo "❌ Debes exportar COORD_IP. Ej: COORD_IP=192.168.171.112" >&2
  exit 1
fi

if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
  docker network create --driver overlay --attachable "$NETWORK" || docker network create "$NETWORK"
fi

SELF_IP=$(hostname -I | awk '{print $1}')
echo "➡️ Host B apuntando a coordinador en $COORD_IP | red $NETWORK | IP local $SELF_IP"

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

echo "🚀 Lanzando nodos mínimos en Host B..."
run_node raft_events_nz_1 8804 EVENTOS_N_Z
run_node raft_users_1     8810 USUARIOS

echo "🎨 Lanzando frontend en Host B..."
docker rm -f frontend_b 2>/dev/null || true
docker run -d --name frontend_b --hostname frontend_b --network "$NETWORK" \
  -p ${FRONT_PORT}:8501 \
  -e PYTHONPATH="/app/front:/app" \
  -e API_BASE_URL=${PRIMARY_API_BASE_URL} \
  -e API_BASE_URLS=${API_BASE_URLS_CONTAINER} \
  -e WEBSOCKET_HOST=${WS_HOST:-coordinator} \
  -e WEBSOCKET_PORT=${WEBSOCKET_PORT_CONTAINER} \
  agenda_frontend streamlit run front/app.py --server.port=8501 --server.address=0.0.0.0

echo "✅ Host B mínimo listo. Front: http://${SELF_IP}:${FRONT_PORT}"
