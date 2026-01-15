#!/usr/bin/env bash
set -euo pipefail

# Genera/actualiza servers.json para Traefik con los coordinadores vivos.
# Usa SEEDS (coma separada) como puntos de partida y recopila /health y /leaders.

OUT=${OUT:-$(pwd)/servers.json}
SEEDS=${SEEDS:-}
INTERVAL=${INTERVAL:-5}

if [[ -z "$SEEDS" ]]; then
  echo "❌ Debes exportar SEEDS con las URLs de coordinadores conocidos, ej:" >&2
  echo "   SEEDS=\"http://192.168.20.112:8700,http://192.168.20.147:8701\" bash $0" >&2
  exit 1
fi

echo "▶️ Watcher de coordinadores. Archivo: $OUT | Intervalo: ${INTERVAL}s"
echo "   Seeds: $SEEDS"

while true; do
  python3 - "$OUT" "$SEEDS" <<'PYCODE'
import sys, json, urllib.request
from urllib.error import URLError

out, seeds_raw = sys.argv[1], sys.argv[2]
seeds = [s.strip() for s in seeds_raw.split(",") if s.strip()]
urls = set(seeds)

def fetch_json(url, timeout=2):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.load(resp)

live = set()
for seed in list(urls):
    try:
        data = fetch_json(f"{seed}/health")
        if data.get("service") == "coordinator":
            live.add(seed)
    except Exception:
        continue
    # Descubrir peers coordinadores si el seed expone el endpoint
    try:
        data = fetch_json(f"{seed}/coordinators/peers", timeout=3)
        for c in data.get("coordinators", []):
            if isinstance(c, str) and c.startswith("http"):
                urls.add(c.strip())
    except Exception:
        pass

# Filtrar por /health de los descubiertos
final = set()
for u in urls:
    try:
        data = fetch_json(f"{u}/health", timeout=2)
        if data.get("service") == "coordinator":
            final.add(u)
    except Exception:
        continue

if not final:
    final = set(seeds)

servers = [{"url": u} for u in sorted(final)]
config = {
    "http": {
        "routers": {
            "coordinator": {"rule": "PathPrefix(`/`)", "service": "coordinators"}
        },
        "services": {
            "coordinators": {
                "loadBalancer": {"servers": servers, "passHostHeader": True}
            }
        },
    }
}
with open(out, "w") as f:
    json.dump(config, f, indent=2)
PYCODE
  sleep "$INTERVAL"
done
