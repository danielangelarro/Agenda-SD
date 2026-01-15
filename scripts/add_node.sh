#!/usr/bin/env bash
set -euo pipefail

# Añade un nodo RAFT extra a un shard existente.
# Variables requeridas:
#   SHARD   (EVENTOS_A_M | EVENTOS_N_Z | GRUPOS | USUARIOS)
#   NODE_ID (nombre del contenedor, ej: raft_events_nz_4)
#   PORT    (puerto interno/externo, ej: 8814)
#   PEERS   (lista de URLs http://host:puerto separadas por coma)
# Opcionales:
#   NETWORK    (default agenda_net)
#   IMAGE      (default agenda_backend)
#   COORD_URL  (opcional) para autoregistro en coordinador
#   DATA_VOL   (default igual a NODE_ID)
#
# Ejemplo:
#   SHARD=EVENTOS_N_Z NODE_ID=raft_events_nz_4 PORT=8814 \
#   PEERS="http://raft_events_nz_1:8804,http://raft_events_nz_2:8805,http://raft_events_nz_3:8806" \
#   COORD_URL=http://coordinator:8700 \
#   bash scripts/add_node.sh

SHARD=${SHARD:-}
NODE_ID=${NODE_ID:-}
PORT=${PORT:-}
PEERS=${PEERS:-}
NETWORK=${NETWORK:-agenda_net}
IMAGE=${IMAGE:-agenda_backend}
COORD_URL=${COORD_URL:-}
DATA_VOL=${DATA_VOL:-$NODE_ID}

if [[ -z "$SHARD" || -z "$NODE_ID" || -z "$PORT" || -z "$PEERS" ]]; then
  echo "❌ Debes definir SHARD, NODE_ID, PORT y PEERS" >&2
  exit 1
fi

docker rm -f "$NODE_ID" >/dev/null 2>&1 || true

if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
  docker network create --driver overlay --attachable "$NETWORK" || docker network create "$NETWORK"
fi

docker run -d --name "$NODE_ID" --hostname "$NODE_ID" --network "$NETWORK" \
  -p "${PORT}:${PORT}" \
  -v "${DATA_VOL}":/app/data \
  -e PYTHONPATH="/app:/app/backend" \
  -e SHARD_NAME="$SHARD" \
  -e NODE_ID="$NODE_ID" \
  -e NODE_URL="http://${NODE_ID}:${PORT}" \
  -e PORT="$PORT" \
  -e PEERS="$PEERS" \
  -e COORD_URL="$COORD_URL" \
  "$IMAGE" uvicorn distributed.nodes.raft_node:app --host 0.0.0.0 --port "$PORT"

echo "✅ Nodo ${NODE_ID} desplegado en ${PORT} para shard ${SHARD}"
