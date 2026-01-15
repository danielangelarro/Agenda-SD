#!/usr/bin/env bash
set -euo pipefail

# Detiene y elimina contenedores del host B (coordinador B, frontend B y nodos 3).

pkill -f watch_coordinators.sh 2>/dev/null || true

docker rm -f coordinator_b frontend_b \
  raft_events_am_3 \
  raft_events_nz_3 \
  raft_groups_2 raft_groups_3 \
  raft_users_2 raft_users_3 \
  coordinator_lb 2>/dev/null || true

echo "✅ Contenedores de Host B detenidos/eliminados"
