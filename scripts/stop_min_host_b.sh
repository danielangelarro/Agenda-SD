#!/usr/bin/env bash
set -euo pipefail

# Detiene y elimina el despliegue mínimo en Host B.

containers=(
  frontend_b
  raft_events_nz_1
  raft_users_1
)

stop_container() {
  local name="$1"
  if docker ps -a --format '{{.Names}}' | grep -Fxq "$name"; then
    echo "Deteniendo $name..."
    docker stop "$name" >/dev/null 2>&1 || true
    docker rm "$name" >/dev/null 2>&1 || true
    echo "Eliminado $name"
  else
    echo "$name no existe, se omite."
  fi
}

echo "Deteniendo despliegue mínimo en Host B..."
for c in "${containers[@]}"; do
  stop_container "$c"
done

echo "Host B mínimo limpio."
