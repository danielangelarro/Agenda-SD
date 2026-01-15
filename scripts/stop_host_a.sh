#!/usr/bin/env bash
set -euo pipefail

# Detiene y elimina contenedores del host A (coordinador, frontend y nodos 1-2).

pkill -f watch_coordinators.sh 2>/dev/null || true

docker rm -f coordinator frontend_a \
  raft_events_am_1 raft_events_am_2 \
  raft_events_nz_1 raft_events_nz_2 \
  raft_groups_1 raft_groups_2 \
  raft_users_1 raft_users_2 \
  coordinator_lb 2>/dev/null || true

echo "✅ Contenedores de Host A detenidos/eliminados"
