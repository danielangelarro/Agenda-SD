#!/usr/bin/env bash
set -euo pipefail

# Levanta un balanceador Traefik que descubre coordinadores automáticamente por labels.
# Exponer puerto externo con LB_PORT (default 8702). Internamente usa 8700.
# Requiere acceso al socket Docker y que los coordinadores tengan labels traefik.enable=true.

NETWORK=${NETWORK:-agenda_net}
LB_PORT=${LB_PORT:-8702}
STATIC_SERVERS_FILE=${STATIC_SERVERS_FILE:-servers.json}

if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
  docker network create --driver overlay --attachable "$NETWORK" || docker network create "$NETWORK"
fi

docker rm -f coordinator_lb 2>/dev/null || true

# Provider file opcional (servers.json generado por watch_coordinators.sh)
FILE_ARG=()
MOUNT_ARG=()
if [[ -f "$STATIC_SERVERS_FILE" ]]; then
  FILE_PATH=$(readlink -f "$STATIC_SERVERS_FILE" 2>/dev/null || python3 - "$STATIC_SERVERS_FILE" <<'PY'
import os, sys
print(os.path.abspath(sys.argv[1]))
PY
)
  # Sanitizar router rule a PathPrefix(`/`) si quedó mal escrita
  tmp_fix="${FILE_PATH}.tmp"
  python3 - "$FILE_PATH" <<'PY'
import json, sys
path = sys.argv[1]
try:
    with open(path) as f:
        data = json.load(f)
    router = data.get("http", {}).get("routers", {}).get("coordinator")
    if router and router.get("rule") == "PathPrefix(/)":
        router["rule"] = "PathPrefix(`/`)"
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
except Exception:
    pass
PY

  FILE_ARG=(--providers.file.filename="/etc/traefik/dynamic/servers.json" --providers.file.watch=true)
  MOUNT_ARG=(-v "${FILE_PATH}:/etc/traefik/dynamic/servers.json:ro")
  echo "📂 Usando lista estática de coordinadores: $FILE_PATH"
else
  echo "ℹ️ No se encontró $STATIC_SERVERS_FILE, solo provider docker."
fi

docker run -d --name coordinator_lb --network "$NETWORK" \
  -p ${LB_PORT}:8700 \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  "${MOUNT_ARG[@]}" \
  traefik:v2.10 \
    --api.insecure=false \
    --providers.docker=true \
    --providers.docker.exposedbydefault=false \
    --providers.docker.network="${NETWORK}" \
    --entrypoints.web.transport.respondingtimeouts.idletimeout=120s \
    --entrypoints.web.transport.respondingtimeouts.readtimeout=120s \
    --entrypoints.web.transport.respondingtimeouts.writetimeout=120s \
    --entrypoints.web.address=":8700" \
    "${FILE_ARG[@]}"

echo "✅ Balanceador listo en puerto ${LB_PORT}. Apunta los frontends a http://<host>:${LB_PORT}"
