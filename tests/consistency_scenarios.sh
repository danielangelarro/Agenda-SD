#!/usr/bin/env bash
set -euo pipefail

# Suite de consistencia ligera: verifica líderes y hace lecturas básicas.
# Config:
#   COORD_URL (default http://localhost:8700)
#   TOKEN (opcional) para consultar eventos/grupos

COORD_URL=${COORD_URL:-http://localhost:8700}
TOKEN=${TOKEN:-}

ts(){ date +"%F %T"; }
log(){ echo "[$(ts)] $*" >&2; }
curl_json(){ curl -sS --max-time 5 "$@"; }

require_bin(){ command -v "$1" >/dev/null 2>&1 || { echo "❌ Falta '$1'" >&2; exit 1; }; }
require_bin curl
require_bin jq

log "📊 Líderes:"
curl_json "${COORD_URL}/leaders" | jq '.'

log "📈 Salud de cluster:"
curl_json "${COORD_URL}/cluster/status" | jq '.'

if [[ -n "$TOKEN" ]]; then
  log "📅 Eventos detallados:"
  curl_json "${COORD_URL}/events/detailed?token=${TOKEN}" | jq 'length'
  log "👥 Grupos:"
  curl_json "${COORD_URL}/groups?token=${TOKEN}" | jq '.'
fi

log "✅ Consistencia básica verificada"
