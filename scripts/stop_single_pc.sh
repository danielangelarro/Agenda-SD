#!/usr/bin/env bash
set -euo pipefail

echo "Deteniendo todos los contenedores del cluster..."

docker rm -f coordinator_1 coordinator_2 coordinator_3 frontend \
  raft_events_am_1 raft_events_am_2 raft_events_am_3 \
  raft_events_nz_1 raft_events_nz_2 raft_events_nz_3 \
  raft_groups_1 raft_groups_2 raft_groups_3 \
  raft_users_1 raft_users_2 raft_users_3 2>/dev/null || true

echo "Cluster detenido."
