#!/usr/bin/env bash
set -euo pipefail

# Stress sencillo: crea en bucle eventos y grupos y consulta estado/líderes.
# Config:
#   COORD_URL (default http://localhost:8700)
#   ITERATIONS (default 20)
#   TOKEN (opcional) token válido para crear eventos/grupos

COORD_URL=${COORD_URL:-http://localhost:8700}
ITERATIONS=${ITERATIONS:-20}
TOKEN=${TOKEN:-}

ts(){ date +"%F %T"; }
log(){ echo "[$(ts)] $*" >&2; }
curl_json(){ curl -sS --max-time 5 "$@"; }

require_bin(){ command -v "$1" >/dev/null 2>&1 || { echo "❌ Falta '$1'" >&2; exit 1; }; }
require_bin curl
require_bin jq

create_group(){
  local name="grp_stress_$(date +%s%N)"
  if [[ -z "$TOKEN" ]]; then
    log "ℹ️ TOKEN no definido, omito crear grupo $name"
    return
  fi
  curl -sS -X POST "${COORD_URL}/groups?token=${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"${name}\",\"description\":\"stress\",\"is_hierarchical\":false,\"members\":[]}" >/dev/null || true
}

create_event(){
  local title="evt_stress_$(date +%s%N)"
  if [[ -z "$TOKEN" ]]; then
    log "ℹ️ TOKEN no definido, omito crear evento $title"
    return
  fi
  curl -sS -X POST "${COORD_URL}/events?token=${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"title\":\"${title}\",\"description\":\"stress\",\"start_time\":\"2026-04-01 10:00:00\",\"end_time\":\"2026-04-01 11:00:00\",\"group_id\":null,\"is_group_event\":false,\"is_hierarchical\":false,\"participants_ids\":[]}" >/dev/null || true
}

log "🚀 Iniciando stress (${ITERATIONS} iteraciones) contra ${COORD_URL}"
for i in $(seq 1 "$ITERATIONS"); do
  log "Iteración $i"
  curl_json "${COORD_URL}/leaders" | jq '.'
  create_group
  create_event
  curl_json "${COORD_URL}/cluster/status" | jq '.coordinator, .shards | length'
done
log "✅ Stress finalizado"
