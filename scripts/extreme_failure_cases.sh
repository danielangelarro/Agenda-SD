#!/usr/bin/env bash
set -euo pipefail

# Suite ampliada de fallos extremos y observabilidad para RAFT shardeado.
# Ejecutar en host A (manager). Puede orquestar host B vía SSH_B.
#
# Config:
#   COORD_URL       (default http://localhost:8700)
#   NET             (default agenda_net)
#   SSH_B           (opcional, ej: user@IP_HOST_B)
#   TOKEN           (opcional) si quieres crear un evento jerárquico de prueba
#   RUN_STRESS=1    para disparar tests/stress_extreme_scenarios.sh al final (si existe)
#   RUN_CONSISTENCY=1 para correr tests/consistency_scenarios.sh al final (si existe)
#
# Contenedores esperados:
#   Host A: coordinator, frontend_a, raft_events_am_1, raft_events_am_2,
#           raft_events_nz_1, raft_events_nz_2, raft_groups_1, raft_users_1
#   Host B: frontend_b, raft_events_nz_3, raft_groups_2, raft_groups_3,
#           raft_users_2, raft_users_3

COORD_URL=${COORD_URL:-http://localhost:8700}
NET=${NET:-agenda_net}
SSH_B=${SSH_B:-}
TOKEN=${TOKEN:-}
RUN_STRESS=${RUN_STRESS:-0}
RUN_CONSISTENCY=${RUN_CONSISTENCY:-0}

require_bin(){ command -v "$1" >/dev/null 2>&1 || { echo "❌ Falta '$1'" >&2; exit 1; }; }
require_bin docker
require_bin curl
require_bin jq

ts(){ date +"%F %T"; }
log(){ echo "[$(ts)] $*" >&2; }
curl_json(){ curl -sS --max-time 5 "$@"; }

wait_coord(){
  for i in {1..40}; do
    curl -sS --max-time 2 "${COORD_URL}/health" >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  log "❌ Coordinator no responde /health"; exit 1
}

wait_leader(){
  local shard="$1"
  for i in {1..40}; do
    local raw leader
    raw=$(curl_json "${COORD_URL}/leaders" || true)
    leader=$(echo "$raw" | jq -r --arg s "$shard" '.[$s].leader // .[$s] // empty')
    [[ -n "$leader" && "$leader" != "null" ]] && { echo "$leader"; return 0; }
    sleep 1
  done
  log "❌ Sin líder para $shard"; exit 1
}

stop_local(){ docker stop "$1" >/dev/null 2>&1 || true; }
start_local(){ docker start "$1" >/dev/null 2>&1 || true; }
stop_remote(){ [[ -n "$SSH_B" ]] && ssh -o BatchMode=yes "$SSH_B" "docker stop $1" >/dev/null 2>&1 || true; }
start_remote(){ [[ -n "$SSH_B" ]] && ssh -o BatchMode=yes "$SSH_B" "docker start $1" >/dev/null 2>&1 || true; }
disconnect_remote(){ [[ -n "$SSH_B" ]] && ssh -o BatchMode=yes "$SSH_B" "docker network disconnect $NET $1" >/dev/null 2>&1 || true; }
connect_remote(){ [[ -n "$SSH_B" ]] && ssh -o BatchMode=yes "$SSH_B" "docker network connect $NET $1" >/dev/null 2>&1 || true; }

print_status(){
  log "📊 Leaders:"
  curl_json "${COORD_URL}/leaders" | jq .
  log "📈 Cluster status:"
  curl_json "${COORD_URL}/cluster/status" | jq . || true
}

create_event_if_token(){
  [[ -z "$TOKEN" ]] && return
  local title="evt_extremo_$(date +%s)"
  log "📝 Creando evento jerárquico ($title)"
  curl -sS -X POST "${COORD_URL}/events?token=${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"title\":\"$title\",\"description\":\"auto\",\"start_time\":\"2026-03-01 10:00:00\",\"end_time\":\"2026-03-01 11:00:00\",\"group_id\":1,\"is_group_event\":true,\"is_hierarchical\":true,\"participants_ids\":[]}" \
    >/dev/null || true
}

log "🔎 Línea base"
wait_coord
print_status

log "🔥 Caso 1: fallo múltiple en shard (EVENTOS_N_Z: líder+seguidor)"
stop_local raft_events_nz_1
stop_local raft_events_nz_2
log "   Esperando líder (no debería haber quorum) y registrando estado..."
sleep 2
print_status
log "▶️ Recuperando nodos NZ"
start_local raft_events_nz_1
start_local raft_events_nz_2
wait_leader "eventos_n_z"
print_status

log "🔥 Caso 2: añadir nodo nuevo en caliente (EVENTOS_N_Z -> raft_events_nz_4)"
SHARD=EVENTOS_N_Z NODE_ID=raft_events_nz_4 PORT=8814 \
  PEERS="http://raft_events_nz_1:8804,http://raft_events_nz_2:8805,http://raft_events_nz_3:8806" \
  COORD_URL="${COORD_URL}" \
  bash "$(dirname "$0")/add_node.sh"
sleep 2
print_status

log "🔥 Caso 3: partición de red host B (split-brain)"
if [[ -n "$SSH_B" ]]; then
  for c in raft_events_nz_3 raft_groups_2 raft_groups_3 raft_users_2 raft_users_3; do
    disconnect_remote "$c"
  done
  sleep 5
  print_status
  log "🔄 Reanexando host B"
  for c in raft_events_nz_3 raft_groups_2 raft_groups_3 raft_users_2 raft_users_3; do
    connect_remote "$c"
  done
  sleep 3
  print_status
else
  log "ℹ️ SSH_B no definido; partición de host B omitida"
fi

log "🔥 Caso 4: operación con 1 nodo (dejar solo raft_events_am_1 vivo)"
stop_local raft_events_am_2
log "   Quorum perdido para AM, observa bloqueos de escritura"
sleep 2
print_status
log "▶️ Restaurando AM_2"
start_local raft_events_am_2
wait_leader "eventos_a_m"
print_status

log "🔥 Caso 5: fallo simultáneo de líderes en varios shards (EVENTOS_A_M + USERS)"
stop_local raft_events_am_1
stop_local raft_users_1
sleep 2
wait_leader "eventos_a_m"
wait_leader "users"
start_local raft_events_am_1
start_local raft_users_1
print_status

log "🔥 Caso 6: nodo rezagado (stop largo y rejoin) USERS_2"
stop_remote raft_users_2
sleep 5
start_remote raft_users_2
sleep 2
print_status

log "🔥 Caso 7: fallo del coordinador"
stop_local coordinator
sleep 2
start_local coordinator
wait_coord
print_status

log "🔥 Caso 8: evento jerárquico opcional (requiere TOKEN)"
create_event_if_token

log "🔥 Caso 9 (carga/stress opcional): RUN_STRESS=${RUN_STRESS}"
if [[ "${RUN_STRESS}" -eq 1 ]]; then
  bash "$(dirname "$0")/../tests/stress_extreme_scenarios.sh" || true
fi

log "🧪 Caso 10 (integridad post-fallos): RUN_CONSISTENCY=${RUN_CONSISTENCY}"
if [[ "${RUN_CONSISTENCY}" -eq 1 ]]; then
  CSCRIPT="$(dirname "$0")/../tests/consistency_scenarios.sh"
  if [[ -f "$CSCRIPT" ]]; then
    bash "$CSCRIPT" || true
  else
    log "ℹ️ consistency_scenarios.sh no encontrado, se omite"
  fi
fi

log "✅ Suite extreme_failure_cases completada"
