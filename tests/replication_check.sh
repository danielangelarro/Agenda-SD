#!/usr/bin/env bash
set -euo pipefail

# Verifica consistencia básica de logs RAFT por shard comparando last_index/commit_index.
# Config:
#   EVENTS_AM_NODES="http://raft_events_am_1:8801,http://raft_events_am_2:8802"
#   EVENTS_NZ_NODES="http://raft_events_nz_1:8804,http://raft_events_nz_2:8805,http://raft_events_nz_3:8806"
#   GROUP_NODES="http://raft_groups_1:8807,http://raft_groups_2:8808,http://raft_groups_3:8809"
#   USER_NODES="http://raft_users_1:8810,http://raft_users_2:8811,http://raft_users_3:8812"
#   TIMEOUT=3 (segundos curl)
#
# Uso:
#   bash tests/replication_check.sh

EVENTS_AM_NODES=${EVENTS_AM_NODES:-"http://raft_events_am_1:8801,http://raft_events_am_2:8802"}
EVENTS_NZ_NODES=${EVENTS_NZ_NODES:-"http://raft_events_nz_1:8804,http://raft_events_nz_2:8805,http://raft_events_nz_3:8806"}
GROUP_NODES=${GROUP_NODES:-"http://raft_groups_1:8807,http://raft_groups_2:8808,http://raft_groups_3:8809"}
USER_NODES=${USER_NODES:-"http://raft_users_1:8810,http://raft_users_2:8811,http://raft_users_3:8812"}
TIMEOUT=${TIMEOUT:-3}

ts(){ date +"%F %T"; }
log(){ echo "[$(ts)] $*" >&2; }

check_shard(){
  local shard="$1" nodes="$2"
  log "🔍 Shard $shard"
  local max_idx=0 min_idx=999999 max_commit=0 min_commit=999999
  local divergences=0 seen=0
  IFS=',' read -r -a arr <<< "$nodes"
  for url in "${arr[@]}"; do
    [[ -z "$url" ]] && continue
    local resp
    if ! resp=$(curl -sS --max-time "$TIMEOUT" "$url/raft/log/summary" 2>/dev/null); then
      log "  ⚠️  $url unreachable"
      continue
    fi
    local li ci role
    li=$(echo "$resp" | jq -r '.last_index // 0')
    ci=$(echo "$resp" | jq -r '.commit_index // 0')
    role=$(echo "$resp" | jq -r '.role // ""')
    printf "  %-30s last=%s commit=%s role=%s\n" "$url" "$li" "$ci" "$role"
    ((seen++))
    (( li > max_idx )) && max_idx=$li
    (( li < min_idx )) && min_idx=$li
    (( ci > max_commit )) && max_commit=$ci
    (( ci < min_commit )) && min_commit=$ci
  done
  if (( seen == 0 )); then
    log "  ❌ Ningún nodo respondió"
    return
  fi
  if (( max_idx - min_idx > 1 || max_commit - min_commit > 1 )); then
    divergences=1
    log "  ⚠️ Divergencia detectada (last_index min=$min_idx max=$max_idx, commit min=$min_commit max=$max_commit)"
  else
    log "  ✅ Índices alineados (last_index min=$min_idx max=$max_idx, commit min=$min_commit max=$max_commit)"
  fi
}

check_shard "EVENTOS_A_M" "$EVENTS_AM_NODES"
check_shard "EVENTOS_N_Z" "$EVENTS_NZ_NODES"
check_shard "GRUPOS" "$GROUP_NODES"
check_shard "USUARIOS" "$USER_NODES"

log "✅ Replication check finalizado"
