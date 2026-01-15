#!/usr/bin/env bash
set -euo pipefail

# Detiene Traefik del balanceador y cualquier watcher de coordinadores.

pkill -f watch_coordinators.sh 2>/dev/null || true
docker rm -f coordinator_lb coord_watcher 2>/dev/null || true

echo "✅ Balanceador detenido/eliminado"
