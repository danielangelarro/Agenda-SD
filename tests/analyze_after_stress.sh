#!/usr/bin/env bash
set -euo pipefail

# Análisis rápido post-stress: líderes, salud de cluster y conteo de eventos por shard.
# Config:
#   COORD_URL (default http://localhost:8700)
#   TOKEN (opcional) para listar eventos detallados

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

log "📈 Cluster status:"
curl_json "${COORD_URL}/cluster/status" | jq '.'

if [[ -n "$TOKEN" ]]; then
  log "📅 Conteo de eventos detallados (vía /events/detailed)"
  curl_json "${COORD_URL}/events/detailed?token=${TOKEN}" | jq 'length'
fi

log "✅ Análisis post-stress completado"
