#!/usr/bin/env bash
set -euo pipefail

# Arranca el host A: coordinador principal + frontend + nodos base.
# Requiere que HOST_B_IP apunte al otro host.
#
# Opcionales:
#   NETWORK     (default agenda_net)
#   FRONT_PORT  (default 8501)
#   WS_PORT     (default 8767, mapea al 8767 interno del coordinador)

# Limpiar posibles configs sucias de shards en el entorno
unset SHARDS_CONFIG_JSON SHARD_GROUPS SHARD_GRUPOS SHARD_USERS SHARD_USUARIOS
unset SHARD_EVENTOS_A_M SHARD_EVENTOS_N_Z SHARD_EVENTS_A_M SHARD_EVENTS_N_Z

# Parar y eliminar contenedores previos que vamos a reutilizar (solo 3 nodos por shard en total)
docker rm -f coordinator frontend_a \
  raft_events_am_1 raft_events_am_2 \
  raft_events_nz_1 raft_events_nz_2 \
  raft_groups_1 raft_groups_2 \
  raft_users_1 raft_users_2 2>/dev/null || true

HOST_B_IP=${HOST_B_IP:-}
NETWORK=${NETWORK:-agenda_net}
FRONT_PORT=${FRONT_PORT:-8501}
WS_PORT=${WS_PORT:-8767}
COORD_B_URL=${COORD_B_URL:-http://coordinator_b:8700}
COORD_C_URL=${COORD_C_URL:-http://coordinator_c:8700}
# Lista de coordinadores para el frontend (failover simple) sin balanceador
# Puedes ampliar la lista agregando más URLs coma-separadas en EXTRA_COORD_PEERS o FRONT_API_URLS
EXTRA_COORD_PEERS=${EXTRA_COORD_PEERS:-}
API_URLS_RAW=${FRONT_API_URLS:-"http://coordinator:8700,${COORD_B_URL},${COORD_C_URL}${EXTRA_COORD_PEERS:+,${EXTRA_COORD_PEERS}}"}
API_BASE_URLS_CONTAINER=""
IFS=',' read -ra API_URLS_ARR <<<"$API_URLS_RAW"
for url in "${API_URLS_ARR[@]}"; do
  url=$(echo "$url" | xargs)
  [[ -z "$url" ]] && continue
  host=$(echo "$url" | sed -E 's#^https?://([^/:]+).*#\1#')
  if [[ "$host" =~ ^(localhost|127\\.|::1)$ ]]; then
    url="http://coordinator:8700"
  fi
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
SELF_COORD_URL=${SELF_COORD_URL:-http://${SELF_IP}:8700}
# Lista de coordinadores conocidos para sync/discovery (opcionalmente extender con EXTRA_COORD_PEERS)
PEER_LIST=${EXTRA_COORD_PEERS:-}
if [[ -n "$COORD_B_URL" ]]; then
  PEER_LIST="${COORD_B_URL}${PEER_LIST:+,${PEER_LIST}}"
fi
if [[ -n "$COORD_C_URL" ]]; then
  PEER_LIST="${COORD_C_URL}${PEER_LIST:+,${PEER_LIST}}"
fi

# Config: 3 nodos por shard distribuidos entre hosts A/B
EVENTS_AM_NAMES=(raft_events_am_1 raft_events_am_2 raft_events_am_3)
EVENTS_AM_PORTS=(8801 8802 8803)
EVENTS_NZ_NAMES=(raft_events_nz_1 raft_events_nz_2 raft_events_nz_3)
EVENTS_NZ_PORTS=(8804 8805 8806)
GROUPS_NAMES=(raft_groups_1 raft_groups_2 raft_groups_3)
GROUPS_PORTS=(8807 8808 8809)
USERS_NAMES=(raft_users_1 raft_users_2 raft_users_3)
USERS_PORTS=(8810 8811 8812)

echo "🌐 Red: $NETWORK | IP local: $SELF_IP | Host B: $HOST_B_IP"

run_node() {
  local name=$1 port=$2 shard=$3 peers=$4 coord_url=$5 coord_urls=$6 volume=$7
  docker run -d --name "$name" --hostname "$name" --network "$NETWORK" -p "${port}:${port}" \
    -v "$volume":/app/data \
    -e PYTHONPATH="/app:/app/backend" \
    -e SHARD_NAME="$shard" \
    -e NODE_ID="$name" \
    -e NODE_URL="http://${name}:${port}" \
    -e PORT="$port" \
    -e PEERS="$peers" \
    -e COORD_URL="$coord_url" \
    -e COORD_URLS="$coord_urls" \
    agenda_backend uvicorn distributed.nodes.raft_node:app --host 0.0.0.0 --port "$port"
}

peers_for() {
  local -n names=$1 ports=$2
  local idx=$3
  local peers=()
  for j in "${!names[@]}"; do
    if [[ $j -ne $idx ]]; then
      peers+=("http://${names[$j]}:${ports[$j]}")
    fi
  done
  IFS=,; echo "${peers[*]}"
}

echo "🚀 Lanzando nodos en Host A (nodos 1-2 por shard)..."
for i in 0 1; do
  peers=$(peers_for EVENTS_AM_NAMES EVENTS_AM_PORTS "$i")
  run_node "${EVENTS_AM_NAMES[$i]}" "${EVENTS_AM_PORTS[$i]}" EVENTOS_A_M "$peers" "http://coordinator:8700" "http://coordinator:8700,${COORD_B_URL}" "raft_data_am$((i+1))"
done
for i in 0 1; do
  peers=$(peers_for EVENTS_NZ_NAMES EVENTS_NZ_PORTS "$i")
  run_node "${EVENTS_NZ_NAMES[$i]}" "${EVENTS_NZ_PORTS[$i]}" EVENTOS_N_Z "$peers" "http://coordinator:8700" "http://coordinator:8700,${COORD_B_URL}" "raft_data_nz$((i+1))"
done
for i in 0 1; do
  peers=$(peers_for GROUPS_NAMES GROUPS_PORTS "$i")
  run_node "${GROUPS_NAMES[$i]}" "${GROUPS_PORTS[$i]}" GRUPOS "$peers" "http://coordinator:8700" "http://coordinator:8700,${COORD_B_URL}" "raft_data_groups$((i+1))"
done
for i in 0 1; do
  peers=$(peers_for USERS_NAMES USERS_PORTS "$i")
  run_node "${USERS_NAMES[$i]}" "${USERS_PORTS[$i]}" USUARIOS "$peers" "http://coordinator:8700" "http://coordinator:8700,${COORD_B_URL}" "raft_data_users$((i+1))"
done

echo "🎯 Lanzando coordinador principal..."
docker rm -f coordinator 2>/dev/null || true
docker run -d --name coordinator --network "$NETWORK" \
  -p 8700:8700 -p ${WS_PORT}:8767 \
  -e PYTHONPATH="/app:/app/backend" \
  -e SHARDS_CONFIG_JSON="" \
  -e DISABLE_DEFAULT_SHARDS=1 \
  -e SELF_COORD_URL="$SELF_COORD_URL" \
  -e COORD_PEERS="$PEER_LIST" \
  -l 'traefik.enable=true' \
  -l "traefik.docker.network=$NETWORK" \
  -l 'traefik.http.routers.coordinator.rule=PathPrefix(`/`)' \
  -l 'traefik.http.routers.coordinator.entrypoints=web' \
  -l 'traefik.http.services.coordinator.loadbalancer.server.port=8700' \
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

echo "✅ Host A listo. Front: http://${SELF_IP}:${FRONT_PORT}"
