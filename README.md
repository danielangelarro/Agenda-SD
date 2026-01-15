# Agenda Distribuida – Manual del Sistema

Agenda colaborativa con consistencia fuerte, sharding por dominio y notificaciones en tiempo real. Todo el plano de datos usa RAFT (líder + réplicas) y un coordinador stateless que enruta peticiones al líder correcto.

## Qué hay en el repo
```
Agenda/
├── distributed/
│   ├── coordinator/          # Servicio FastAPI que descubre líderes y enruta
│   ├── nodes/                # Nodos RAFT (FastAPI + SQLite por nodo)
│   ├── shared/raft.py        # Core RAFT (Bully, AppendEntries, sync)
│   ├── start_cluster.py      # Arranque local simple (dev)
├── front/                    # Frontend Streamlit
├── backend/                  # API monolítica (modo no distribuido)
├── tests/                    # Scripts de consistencia y failover
├── run_local.sh              # Stack compose local
├── deploy_swarn.sh           # Despliegue en Docker Swarm
└── monitor_logs.sh           # Seguimiento de logs de contenedores
```

## Arquitectura distribuida
- **Shards RAFT (4 dominios)**  
  - `events_a_m`: eventos cuyo creador empieza A‑M  
  - `events_n_z`: eventos cuyo creador empieza N‑Z  
  - `groups`: gestión de grupos  
  - `users`: usuarios y autenticación  
- **RAFT**: elección de líder con Bully, replicación de log, quorum dinámico, curación de réplicas rezagadas (`shared/raft.py`).  
- **Coordinador**: consulta `/raft/state`, cachea líder, reintenta en caso de fallo, enruta lecturas a cualquier réplica. Es stateless, puedes correr múltiples instancias detrás de un balanceador.  
- **Persistencia**: cada nodo guarda `data/<NODE_ID>_state.json` (estado RAFT) y una base SQLite por shard.  
- **Notificaciones**: WebSockets para eventos/invitaciones en tiempo real (frontend escucha y muestra).  

## Requisitos
- Docker (sin docker compose).  
- Docker Swarm opcional para despliegue distribuido.  
- Python 3.10+ si corres servicios fuera de contenedores.  

## Arranque manual (sin docker compose)
Ejemplo mínimo lanzando un nodo RAFT, coordinador y frontend con `docker run`:
```bash
# Nodo RAFT eventos A-M (líder inicial)
docker run -d --name raft_events_am_1 \
  -e SHARD_NAME=EVENTOS_A_M \
  -e NODE_ID=http://raft_events_am_1:8801 \
  -e NODE_URL=http://raft_events_am_1:8801 \
  -e PORT=8801 \
  -e PEERS=http://raft_events_am_2:8802,http://raft_events_am_3:8803 \
  -p 8801:8801 agenda-raft-node:latest

# Coordinador (stateless)
docker run -d --name coordinator \
  -e SHARD_EVENTS_A_M="http://raft_events_am_1:8801,http://raft_events_am_2:8802,http://raft_events_am_3:8803" \
  -e SHARD_EVENTS_N_Z="http://raft_events_nz_1:8804,http://raft_events_nz_2:8805,http://raft_events_nz_3:8806" \
  -e SHARD_GROUPS="http://raft_groups_1:8807,http://raft_groups_2:8808,http://raft_groups_3:8809" \
  -e SHARD_USERS="http://raft_users_1:8810,http://raft_users_2:8811,http://raft_users_3:8812" \
  -p 8700:80 coordinator:latest

# Frontend
docker run -d --name frontend -e API_BASE_URL=http://coordinator:8700 -p 8501:8501 agenda-frontend:latest
```
Repite `docker run` para cada nodo de cada shard. Usa `monitor_logs.sh` para seguir logs de contenedores.

## Configuración de shards (escala sin tocar código)
Coordinador acepta nodos por entorno:
- `SHARDS_CONFIG_JSON`: JSON con listas por shard. Ej:
  ```json
  {"events_a_m":["http://raft_events_am_1:8801","http://raft_events_am_2:8802","http://raft_events_am_4:8813"]}
  ```
- Variables por shard (coma separada): `SHARD_EVENTS_A_M`, `SHARD_EVENTS_N_Z`, `SHARD_GROUPS`, `SHARD_USERS`.

Nodos RAFT:
- `SHARD_NAME` (EVENTOS_A_M | EVENTOS_N_Z | GRUPOS | USUARIOS)  
- `NODE_ID`, `NODE_URL`, `PORT`  
- `PEERS` (URLs de los demás nodos del shard, separados por coma)  
- `REPLICATION_FACTOR` (opcional; cuántos seguidores empujar activamente, <= total de nodos)  
- `COORD_URL` (opcional): si se define, el nodo se autorregistra en el coordinador (`/admin/shards/add`) al arrancar, sin intervención manual.

## Endpoints clave
- **Coordinador** (`distributed/coordinator/router.py`):  
  - `POST /events | /groups | /users` → redirige al líder del shard.  
  - `GET /leaders` → líder actual por shard.  
  - `GET /cluster/status` → salud de todos los nodos.  
  - `GET /health` → salud del coordinador.  
- **Nodos RAFT** (`distributed/nodes/raft_node.py` y variantes):  
  - `POST /events | /groups | /users` (solo líder; followers devuelven `leader`).  
  - `GET /raft/state`, `GET /raft/log/summary`, `GET /raft/sync`, `POST /raft/append_entries`, `POST /raft/bully/*`.  
  - `GET /health` → estado del nodo.  

## Flujo de escritura
1. Cliente/Frontend llama al coordinador.  
2. Coordinador descubre o usa líder cacheado.  
3. Líder agrega entrada al log y replica a peers según `replication_factor`.  
4. Con quorum, avanza `commit_index`, aplica a SQLite y responde.  
5. Réplicas aplican al confirmar commit; nodos rezagados se curan con AppendEntries o `/raft/sync`.  

## Cómo tumbar y levantar nodos (manual)
- Detener un nodo: `docker stop <nombre_contenedor>` (ej. `docker stop raft_events_am_1`).  
- Arrancar de nuevo: `docker start <nombre_contenedor>`. 
- Reiniciar limpio: `docker rm -f <nombre_contenedor>` y recrea con el mismo comando `docker run` (mantén el volumen de datos si quieres conservar estado).  
- Ver rol actual: `curl http://localhost:8801/raft/state`.  
- Ver líderes por shard: `curl http://localhost:8700/leaders`.  

## Cómo escalar añadiendo réplicas (manual)
1. Lanza un contenedor RAFT extra con env vars (`SHARD_NAME`, `NODE_ID`, `NODE_URL`, `PORT`, `PEERS`). Ejemplo:
   ```bash
   docker run -d --name raft_events_am_4 \
     -e SHARD_NAME=EVENTOS_A_M \
     -e NODE_ID=http://raft_events_am_4:8813 \
     -e NODE_URL=http://raft_events_am_4:8813 \
     -e PORT=8813 \
     -e PEERS=http://raft_events_am_1:8801,http://raft_events_am_2:8802,http://raft_events_am_3:8803 \
     -p 8813:8813 agenda-raft-node:latest
   ```
2. Actualiza el coordinador con la URL nueva (`SHARD_EVENTS_A_M` o `SHARDS_CONFIG_JSON`) y reinícialo.  
3. Ajusta `REPLICATION_FACTOR` en nodos si quieres replicar activamente a más seguidores.  
4. El nuevo nodo se auto-sincroniza (`/raft/sync`) y entra al shard sin downtime.  

## Alta disponibilidad del coordinador
- Levanta 2+ instancias del coordinador con la misma config y ponlas detrás de un balanceador (Traefik/Nginx/DNS RR). Es stateless; la caída de una instancia no corta el servicio.  

## Pruebas y monitoreo
- Estado RAFT en vivo: `bash tests/monitor_raft_state.sh 2` (consulta `/raft/state` cada 2s).  
- Failover automático: `bash tests/failover_autotest.sh` (requiere puertos 8700 y 8801-8803).  
- Consistencia: `tests/consistency_scenarios.sh`, `tests/consistency_suite.sh`.  
- Logs en vivo: `bash monitor_logs.sh`.  

### Simular fallos (manual)
```bash
# Caer líder shard A-M
docker stop raft_events_am_1
# Observar reelección en otros nodos
curl http://localhost:8802/raft/state
curl http://localhost:8803/raft/state
# Reincorporar
docker start raft_events_am_1
```

## Variables de entorno comunes
- **Coordinador**: `SHARDS_CONFIG_JSON`, `SHARD_EVENTS_A_M`, `SHARD_EVENTS_N_Z`, `SHARD_GROUPS`, `SHARD_USERS`.  
- **Nodos RAFT**: `SHARD_NAME`, `NODE_ID`, `NODE_URL`, `PORT`, `PEERS`, `REPLICATION_FACTOR`.  
- **Frontend**: `API_BASE_URL`, `WEBSOCKET_HOST`, `WEBSOCKET_PORT`.  
- **Backend monolito**: `DB_PATH`, `WEBSOCKET_HOST`, `WEBSOCKET_PORT`.  

## Comandos útiles
- Ver líderes: `curl http://localhost:8700/leaders`  
- Estado de cluster: `curl http://localhost:8700/cluster/status`  
- Estado de un nodo: `curl http://localhost:8801/raft/state`  
- Leer eventos shard A-M directo: `curl http://localhost:8801/events`  

## Limpieza
- Compose: `docker compose down -v`  
- Swarm: `cleanup_swarm.sh` o bloque de `archivos/escenarios y pruebas.txt` para `docker rm -f` de `raft_*`, `backend`, `coordinator`, `frontend`.  

## Operación y consideraciones
- Quorum dinámico: líder puede degradar quorum si hay menos peers vivos (más disponibilidad, menor garantía temporal).  
- Rejoin seguro: nodos que vuelven se curan con AppendEntries o `/raft/sync`.  
- Persistencia: monta volúmenes en `data/` si quieres durabilidad entre reinicios.  

///////////////////////////////////////////////////////////////
### Agregar y tumbar el nodo 
Tumbar un nodo (ejemplos)

Parar sin borrar datos: docker stop raft_events_am_1
Arrancar de nuevo: docker start raft_events_am_1
Borrado forzado (para recrearlo limpio):
docker rm -f raft_events_am_1
Luego lo recreas con el mismo docker run que usaste inicialmente.
Agregar un nodo (con autoregistro al coordinador)
Ejemplo para un cuarto nodo en el shard A‑M, con autoregistro:
 ```bash
docker run -d --name raft_events_am_4 \
  -e SHARD_NAME=EVENTOS_A_M \
  -e NODE_ID=http://raft_events_am_4:8813 \
  -e NODE_URL=http://raft_events_am_4:8813 \
  -e PORT=8813 \
  -e PEERS=http://raft_events_am_1:8801,http://raft_events_am_2:8802,http://raft_events_am_3:8803 \
  -e COORD_URL=http://coordinator:80 \
  -p 8813:8813 agenda-raft-node:latest
   ```
