from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import httpx
import asyncio
import logging
import os
import json
from typing import Optional, List
import websockets
import threading
import asyncio

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("coordinator")

app = FastAPI(title="Coordinador RAFT - Docker Swarm", version="3.0")

# =========================================================
# 🔔 Gestor de notificaciones WebSocket (minimal)
# =========================================================

class WSManager:
    def __init__(self):
        self.connections: dict[int, websockets.WebSocketServerProtocol] = {}
        self.lock = asyncio.Lock()
        self.host = os.getenv("WEBSOCKET_HOST", "0.0.0.0")
        self.port = int(os.getenv("WEBSOCKET_PORT", "8767"))
        self.server = None

    async def handler(self, websocket):
        """Recibe auth y registra conexión por user_id."""
        try:
            auth_msg = await websocket.recv()
            try:
                data = json.loads(auth_msg)
            except Exception:
                await websocket.close()
                return
            if data.get("type") != "auth":
                await websocket.close()
                return
            user_id = int(data.get("user_id", 0))
            # Token se acepta de forma laxa (coordinador ya valida en API)
            await websocket.send(json.dumps({"type": "auth_success"}))
            async with self.lock:
                # Reemplazar conexión previa si existe
                old = self.connections.pop(user_id, None)
                if old:
                    try:
                        await old.close()
                    except Exception:
                        pass
                self.connections[user_id] = websocket
            # Mantener viva la conexión
            async for msg in websocket:
                # ignorar, solo pings/acks
                try:
                    payload = json.loads(msg)
                    if payload.get("type") == "ping":
                        await websocket.send(json.dumps({"type": "pong"}))
                except Exception:
                    continue
        except Exception:
            pass
        finally:
            # limpiar
            async with self.lock:
                for uid, ws in list(self.connections.items()):
                    if ws is websocket:
                        self.connections.pop(uid, None)

    async def send_to_user(self, user_id: int, message: dict):
        async with self.lock:
            ws = self.connections.get(int(user_id))
        if not ws:
            return False
        try:
            await ws.send(json.dumps(message))
            return True
        except Exception:
            return False

    async def broadcast(self, user_ids: List[int], message: dict):
        for uid in user_ids:
            await self.send_to_user(uid, message)

    async def start(self):
        self.server = await websockets.serve(self.handler, self.host, self.port)
        logger.info(f"🔔 WebSocket server escuchando en ws://{self.host}:{self.port}")

ws_manager = WSManager()
PEER_COORDINATORS = [
    url.strip()
    for url in (os.getenv("COORD_PEERS") or "").split(",")
    if url.strip()
]

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(ws_manager.start())
    if PEER_COORDINATORS:
        asyncio.create_task(periodic_peer_sync())


@app.get("/coordinators/peers")
async def coordinators_peers():
    """Devuelve lista de coordinadores conocidos (incluyéndonos). Útil para discovery dinámico."""
    peers = list(PEER_COORDINATORS) if PEER_COORDINATORS else []
    self_url = os.getenv("SELF_COORD_URL")
    if self_url:
        peers.append(self_url)
    return {"coordinators": peers}

# =========================================================
# 🌐 CONFIGURACIÓN DE SHARDS (dinámica para escalar)
# =========================================================
DEFAULT_SHARDS = {
    # Claves en español para evitar duplicados events_* / eventos_*
    "eventos_a_m": [
        "http://raft_events_am_1:8801",
        "http://raft_events_am_2:8802",
        "http://raft_events_am_3:8803",
    ],
    "eventos_n_z": [
        "http://raft_events_nz_1:8804",
        "http://raft_events_nz_2:8805",
        "http://raft_events_nz_3:8806",
    ],
    "groups": [
        "http://raft_groups_1:8807",
        "http://raft_groups_2:8808",
        "http://raft_groups_3:8809",
    ],
    "users": [
        "http://raft_users_1:8810",
        "http://raft_users_2:8811",
        "http://raft_users_3:8812",
    ],
}


def _parse_nodes(raw: str):
    """Convierte una lista separada por comas en URLs limpias."""
    return [node.strip() for node in raw.split(",") if node.strip()]

def _is_valid_node_url(node: str) -> bool:
    return isinstance(node, str) and (node.startswith("http://") or node.startswith("https://"))

def _filter_valid_nodes(nodes: list[str]) -> list[str]:
    """Descarta entradas que no sean URLs http(s) válidas."""
    valid = []
    for n in nodes:
        if _is_valid_node_url(n):
            valid.append(n)
    return valid

def _union_nodes(*node_lists: list[list[str]]) -> list[str]:
    """Une listas de nodos, filtra inválidos y quita duplicados."""
    merged = []
    for lst in node_lists:
        if not lst:
            continue
        merged.extend(lst)
    uniq = []
    for n in _filter_valid_nodes(merged):
        if n not in uniq:
            uniq.append(n)
    return uniq

ALIAS_TO_CANONICAL = {
    "events_a_m": "eventos_a_m",
    "events_n_z": "eventos_n_z",
    "grupos": "groups",
    "usuarios": "users",
}

def _canonical_shard(name: str) -> str:
    """Normaliza nombre de shard a la clave canónica."""
    if not name:
        return name
    key = name.lower()
    return ALIAS_TO_CANONICAL.get(key, key)


def load_shards_from_env() -> dict:
    """Permite definir shards y nodos vía variables de entorno para escalar sin tocar código.

    Prioridad:
    1) SHARDS_CONFIG_JSON = '{"eventos_a_m":["http://...","http://..."], ...}'
    2) Variables SHARD_EVENTOS_A_M, SHARD_EVENTOS_N_Z, SHARD_GRUPOS, SHARD_USUARIOS (coma separadas)
    3) DEFAULT_SHARDS hardcodeado
    """
    disable_defaults = os.getenv("DISABLE_DEFAULT_SHARDS", "").lower() in ("1", "true", "yes")

    # Opción 1: JSON completo
    cfg_json = os.getenv("SHARDS_CONFIG_JSON")
    if cfg_json:
        try:
            data = json.loads(cfg_json)
            shards = {k: _parse_nodes(",".join(v)) if isinstance(v, list) else _parse_nodes(str(v)) for k, v in data.items()}
            return {k: v for k, v in shards.items() if v}
        except Exception as e:
            logger.warning(f"No pude parsear SHARDS_CONFIG_JSON, uso defaults: {e}")

    # Opción 2: variables por shard
    env_overrides = {
        # Soporta vars nuevas y legacy en inglés para compatibilidad
        "eventos_a_m": os.getenv("SHARD_EVENTOS_A_M") or os.getenv("SHARD_EVENTS_A_M"),
        "eventos_n_z": os.getenv("SHARD_EVENTOS_N_Z") or os.getenv("SHARD_EVENTS_N_Z"),
        "groups": os.getenv("SHARD_GROUPS") or os.getenv("SHARD_GRUPOS"),
        "users": os.getenv("SHARD_USERS") or os.getenv("SHARD_USUARIOS"),
    }
    shards = {}
    for shard, raw in env_overrides.items():
        if raw:
            nodes = _parse_nodes(raw)
            if nodes:
                shards[shard] = nodes

    # Normalizar claves legacy -> canónicas
    normalized = {}
    for shard, nodes in shards.items():
        canonical = _canonical_shard(shard)
        normalized.setdefault(canonical, [])
        normalized[canonical].extend(nodes)
    shards = normalized

    # Completar con defaults cuando no hay override (a menos que se desactiven)
    if not disable_defaults:
        for shard, nodes in DEFAULT_SHARDS.items():
            shards.setdefault(shard, nodes)

    # Filtrar nodos inválidos (no http/https)
    for shard, nodes in list(shards.items()):
        shards[shard] = _filter_valid_nodes(nodes)

    return shards

SHARDS = load_shards_from_env()
NODES_PER_SHARD = {k: len(v) for k, v in SHARDS.items()}

# Cache local de líderes detectados
LEADER_CACHE = {}
NODES_PER_SHARD = {k: len(v) for k, v in SHARDS.items()}

# =========================================================
# 🔐 Autenticación y validación de sesión
# =========================================================

async def validate_token(token: str) -> dict:
    """Valida un token contra el shard de usuarios."""
    if not token:
        raise HTTPException(status_code=401, detail="Sesión inválida o expirada")

    for node_url in SHARDS["users"]:
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(f"{node_url}/auth/validate", params={"token": token})
                data = resp.json()
                if data.get("valid"):
                    return data
        except Exception:
            continue

    raise HTTPException(status_code=401, detail="Sesión inválida o expirada")

async def get_username_by_id(user_id: int) -> Optional[str]:
    """Consulta cualquier nodo de usuarios para resolver username."""
    for node_url in SHARDS["users"]:
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(f"{node_url}/users/{user_id}")
                data = resp.json()
                if data.get("username"):
                    return data["username"]
        except Exception:
            continue
    return None

async def get_group_name(group_id: int) -> Optional[str]:
    """Consulta shard de grupos para obtener el nombre."""
    for node_url in SHARDS["groups"]:
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(f"{node_url}/groups/{group_id}/info")
                data = resp.json()
                if data.get("name"):
                    return data["name"]
        except Exception:
            continue
    return None

async def _get_group_member_ids(group_id: int) -> list[int]:
    """Obtiene IDs de miembros de un grupo."""
    for node_url in SHARDS["groups"]:
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(f"{node_url}/groups/{group_id}/members")
                data = resp.json()
                if isinstance(data, list):
                    return [int(x[0]) if isinstance(x, (list, tuple)) else int(x.get("user_id")) for x in data]
        except Exception:
            continue
    return []

# =========================================================
# 🔧 Helpers para modificar shards en caliente
# =========================================================

def set_shard_nodes(shard: str, nodes: list[str]):
    """Actualiza nodos de un shard en memoria y limpia caché de líder."""
    canonical = _canonical_shard(shard)
    clean = _filter_valid_nodes(nodes)
    SHARDS[canonical] = clean
    NODES_PER_SHARD[canonical] = len(clean)
    LEADER_CACHE.pop(canonical, None)

def add_node_to_shard(shard: str, node_url: str):
    if not _is_valid_node_url(node_url):
        logger.warning(f"Rechazando nodo inválido para shard {shard}: {node_url}")
        return
    canonical = _canonical_shard(shard)
    nodes = _filter_valid_nodes(SHARDS.get(canonical, []))
    if node_url not in nodes:
        nodes.append(node_url)
    set_shard_nodes(canonical, nodes)

def replace_node_in_shard(shard: str, old_url: str, new_url: str):
    if not _is_valid_node_url(new_url):
        logger.warning(f"Rechazando nodo inválido para shard {shard}: {new_url}")
        return
    canonical = _canonical_shard(shard)
    nodes = _filter_valid_nodes(SHARDS.get(canonical, []))
    updated = []
    for n in nodes:
        if n == old_url:
            updated.append(new_url)
        else:
            updated.append(n)
    if new_url not in updated:
        updated.append(new_url)
    set_shard_nodes(canonical, updated)

async def propagate_peers_to_shard(shard: str):
    """Envía la lista completa de peers a cada nodo del shard."""
    canonical = _canonical_shard(shard)
    nodes = SHARDS.get(canonical, [])
    if not nodes:
        return
    rep = NODES_PER_SHARD.get(canonical, len(nodes)) or len(nodes)
    async with httpx.AsyncClient(timeout=5.0) as client:
        for target in nodes:
            peers = [n for n in nodes if n != target]
            try:
                await client.post(f"{target}/admin/peers/update", json={"peers": peers, "replication_factor": rep})
            except Exception as e:
                logger.warning(f"⚠️ No se pudo propagar peers a {target}: {e}")

# =========================================================
# 🧠 Funciones auxiliares
# =========================================================
def get_shard_for_user(username: str) -> str:
    """Determina el shard correcto según el nombre de usuario"""
    if not username:
        raise HTTPException(status_code=400, detail="Usuario inválido")
    
    first = username[0].lower()
    if 'a' <= first <= 'm':
        return "eventos_a_m"
    elif 'n' <= first <= 'z':
        return "eventos_n_z"
    else:
        return "eventos_a_m"  # Por defecto

async def get_leader(shard_name: str) -> str:
    """Devuelve la URL del líder actual del shard"""
    shard_name = _canonical_shard(shard_name)
    # Si tenemos líder cacheado, lo validamos rápido
    if shard_name in LEADER_CACHE:
        leader_url = LEADER_CACHE[shard_name]
        if await validate_leader(leader_url):
            return leader_url
        else:
            LEADER_CACHE.pop(shard_name, None)

    # Consultar los nodos RAFT del shard en paralelo
    tasks = [check_node_role(node_url) for node_url in SHARDS[shard_name]]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for result in results:
        if isinstance(result, dict) and result.get("is_leader"):
            LEADER_CACHE[shard_name] = result["url"]
            logger.info(f"✅ Líder encontrado para {shard_name}: {result['url']}")
            return result["url"]

    raise HTTPException(
        status_code=503, 
        detail=f"No se encontró líder activo para el shard {shard_name}"
    )

async def check_node_role(node_url: str) -> dict:
    """Verifica el rol de un nodo específico"""
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(f"{node_url}/raft/state")
            data = resp.json()
            role = data.get("role", "").lower()
            is_leader = "leader" in role
            return {"url": node_url, "is_leader": is_leader, "role": role}
    except Exception as e:
        logger.warning(f"❌ Error consultando {node_url}: {e}")
        return {"url": node_url, "is_leader": False, "error": str(e)}

async def validate_leader(url: str) -> bool:
    """Verifica si un líder cacheado sigue activo"""
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            resp = await client.get(f"{url}/raft/state")
            data = resp.json()
            role = data.get("role", "").lower()
            return "leader" in role
    except Exception:
        return False

async def prune_missing_nodes():
    """Elimina de SHARDS los nodos que ya no responden (p.ej. contenedor borrado)."""
    changed = False
    async with httpx.AsyncClient(timeout=2.0) as client:
        for shard, nodes in list(SHARDS.items()):
            reachable = []
            for node in nodes:
                try:
                    await client.get(f"{node}/health")
                    reachable.append(node)
                except Exception:
                    logger.warning(f"Pruning nodo inalcanzable en {shard}: {node}")
            if len(reachable) != len(nodes):
                set_shard_nodes(shard, reachable)
                changed = True
    return changed

async def sync_from_peers():
    """Intenta sincronizar shards desde otros coordinadores listados en COORD_PEERS."""
    if not PEER_COORDINATORS:
        return
    async with httpx.AsyncClient(timeout=3.0) as client:
        for peer in PEER_COORDINATORS:
            try:
                resp = await client.get(f"{peer}/leaders")
                if resp.status_code != 200:
                    continue
                data = resp.json()
                for shard, info in data.items():
                    nodes = info.get("nodes") or []
                    if nodes:
                        merged = _union_nodes(SHARDS.get(_canonical_shard(shard), []), nodes)
                        set_shard_nodes(shard, merged)
            except Exception as e:
                logger.warning(f"No se pudo sincronizar shards desde {peer}: {e}")

async def periodic_peer_sync():
    """Sincroniza con peers y prunea nodos caídos de forma periódica."""
    while True:
        try:
            await sync_from_peers()
            await prune_missing_nodes()
        except Exception as e:
            logger.warning(f"Error en sync periódica: {e}")
        await asyncio.sleep(15)

# =========================================================
# 📦 Modelos Pydantic
# =========================================================
class EventCreate(BaseModel):
    title: str
    description: str
    start_time: str
    end_time: str
    group_id: Optional[int] = None
    is_group_event: bool = False
    participants_ids: Optional[List[int]] = None
    is_hierarchical: bool = False

class GroupCreate(BaseModel):
    name: str
    description: str
    is_hierarchical: bool = False
    members: Optional[List[int]] = None

class UserCreate(BaseModel):
    username: str
    password: str
    email: str | None = None

class AuthRegister(BaseModel):
    username: str
    password: str
    email: str | None = None

class AuthLogin(BaseModel):
    username: str
    password: str

# =========================================================
# 🎯 Endpoints principales
# =========================================================

@app.post("/auth/register")
async def auth_register(user: AuthRegister):
    """Registro de usuario (delegado al shard de usuarios)."""
    try:
        leader_url = await get_leader("users")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{leader_url}/auth/register", json=user.dict())
        data = resp.json()
        if data.get("error"):
            raise HTTPException(status_code=400, detail=data["error"])
        return {"message": "Usuario registrado exitosamente"}
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.warning(f"⚠️ Error en registro, intentando nuevo líder: {e}")
        LEADER_CACHE.pop("users", None)
        new_leader = await get_leader("users")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{new_leader}/auth/register", json=user.dict())
        data = resp.json()
        if data.get("error"):
            raise HTTPException(status_code=400, detail=data["error"])
        return {"message": "Usuario registrado exitosamente"}

@app.get("/auth/validate")
async def auth_validate(token: str):
    """Valida token y devuelve user_id/username."""
    return await validate_token(token)

@app.post("/auth/login")
async def auth_login(user: AuthLogin):
    """Login de usuario y emisión de token (delegado al shard de usuarios)."""
    try:
        leader_url = await get_leader("users")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{leader_url}/auth/login", json=user.dict())
        data = resp.json()
        if data.get("error"):
            status_code = data.get("status_code", 401 if "credenciales" in data.get("error", "").lower() else 400)
            raise HTTPException(status_code=status_code, detail=data["error"])
        return data
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.warning(f"⚠️ Error en login, intentando nuevo líder: {e}")
        LEADER_CACHE.pop("users", None)
        new_leader = await get_leader("users")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{new_leader}/auth/login", json=user.dict())
        data = resp.json()
        if data.get("error"):
            status_code = data.get("status_code", 401 if "credenciales" in data.get("error", "").lower() else 400)
            raise HTTPException(status_code=status_code, detail=data["error"])
        return data

@app.post("/events")
async def create_event(event: EventCreate, token: str):
    user_data = await validate_token(token)
    username = user_data.get("username")
    user_id = user_data.get("user_id")
    shard_name = get_shard_for_user(username)

    payload = event.dict()
    payload["creator"] = username
    payload["creator_id"] = user_id
    payload["creator_username"] = username
    # Validar grupo y miembros si aplica
    members = None
    if event.group_id:
        members = await _get_group_member_ids(event.group_id)
        if user_id not in members:
            raise HTTPException(status_code=400, detail="No perteneces al grupo")
        if event.participants_ids:
            invalid = [pid for pid in event.participants_ids if pid not in members]
            if invalid:
                raise HTTPException(status_code=400, detail="Hay participantes que no pertenecen al grupo")
        # Si es jerárquico, forzar que todos los miembros queden como participantes (sin aceptar manual)
        if event.is_hierarchical:
            payload["participants_ids"] = [uid for uid in members]  # incluye creador, se marcará aceptado

    # Lista de participantes finales para notificar
    participants = payload.get("participants_ids") or []

    try:
        leader_url = await get_leader(shard_name)
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{leader_url}/events", json=payload)
        data = resp.json()
        if data.get("error"):
            raise HTTPException(status_code=400, detail=data["error"])
        # Notificar a participantes (informativo; jerárquicos ya quedan aceptados)
        for pid in participants:
            if pid != user_id:
                asyncio.create_task(ws_manager.send_to_user(pid, {
                    "type": "event_invitation",
                    "event_id": data.get("event_id"),
                    "title": event.title,
                    "start_time": event.start_time,
                    "end_time": event.end_time
                }))
        return data
    except HTTPException as he:
        raise he
    except Exception as e:
        # Reintento forzado con nuevo descubrimiento de líder
        logger.warning(f"⚠️ Error con líder actual, buscando nuevo líder: {e}")
        LEADER_CACHE.pop(shard_name, None)
        new_leader = await get_leader(shard_name)
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{new_leader}/events", json=payload)
        data = resp.json()
        if data.get("error"):
            raise HTTPException(status_code=400, detail=data["error"])
        for pid in event.participants_ids or []:
            if pid != user_id:
                asyncio.create_task(ws_manager.send_to_user(pid, {
                    "type": "event_invitation",
                    "event_id": data.get("event_id"),
                    "title": event.title,
                    "start_time": event.start_time,
                    "end_time": event.end_time
                }))
        return data

@app.post("/groups")
async def create_group(group: GroupCreate, token: str):
    user_data = await validate_token(token)
    username = user_data.get("username")
    user_id = user_data.get("user_id")
    payload = group.dict()
    payload["creator_id"] = user_id
    payload["creator_username"] = username
    try:
        leader_url = await get_leader("groups")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{leader_url}/groups", json=payload)
            return resp.json()
    except Exception as e:
        logger.warning(f"⚠️ Error con líder actual, buscando nuevo líder: {e}")
        LEADER_CACHE.pop("groups", None)
        new_leader = await get_leader("groups")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{new_leader}/groups", json=payload)
            return resp.json()

@app.post("/users")
async def create_user(user: UserCreate):
    """Compatibilidad con contrato antiguo: delega a /auth/register."""
    try:
        leader_url = await get_leader("users")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{leader_url}/auth/register", json=user.dict())
        data = resp.json()
        if data.get("error"):
            raise HTTPException(status_code=400, detail=data["error"])
        return {"message": "Usuario registrado exitosamente"}
    except Exception as e:
        logger.warning(f"⚠️ Error con líder actual, buscando nuevo líder: {e}")
        LEADER_CACHE.pop("users", None)
        new_leader = await get_leader("users")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{new_leader}/auth/register", json=user.dict())
        data = resp.json()
        if data.get("error"):
            raise HTTPException(status_code=400, detail=data["error"])
        return {"message": "Usuario registrado exitosamente"}

def _iter_event_shards():
    return ["eventos_a_m", "eventos_n_z"] if "eventos_a_m" in SHARDS else [k for k in SHARDS.keys() if "evento" in k or "events" in k]

@app.get("/events")
async def list_events(token: str):
    """Lista eventos del usuario autenticado."""
    user_data = await validate_token(token)
    user_id = user_data.get("user_id")
    
    events = []
    for shard in _iter_event_shards():
        for node_url in SHARDS[shard]:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.get(f"{node_url}/events", params={"user_id": user_id})
                    data = resp.json()
                    if isinstance(data, list):
                        events.extend(data)
                        break
            except Exception:
                continue
    # Enriquecer con nombres de grupo si aplica
    enriched = []
    for ev in events:
        if ev.get("group_id") and not ev.get("group_name"):
            ev["group_name"] = await get_group_name(ev["group_id"])
        enriched.append(ev)
    return enriched

@app.get("/groups")
async def list_groups(token: str):
    """Lista grupos - consulta cualquier nodo del shard (requiere sesión)."""
    user_data = await validate_token(token)
    user_id = user_data.get("user_id")
    for node_url in SHARDS["groups"]:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{node_url}/groups", params={"user_id": user_id})
                return resp.json()
        except Exception:
            continue
    
    raise HTTPException(status_code=503, detail="No hay nodos disponibles para consulta")

@app.get("/users")
async def list_users(token: str):
    """Lista usuarios - consulta cualquier nodo del shard (requiere sesión)."""
    await validate_token(token)
    for node_url in SHARDS["users"]:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{node_url}/users")
                return resp.json()
        except Exception:
            continue
    
    raise HTTPException(status_code=503, detail="No hay nodos disponibles para consulta")

@app.post("/groups/invite")
async def invite_user_to_group(group_id: int, invited_user_id: int, token: str):
    user_data = await validate_token(token)
    inviter_id = user_data.get("user_id")
    invited_username = await get_username_by_id(invited_user_id)
    payload = {
        "group_id": group_id,
        "invited_user_id": invited_user_id,
        "invited_username": invited_username,
        "inviter_id": inviter_id,
    }
    try:
        leader_url = await get_leader("groups")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{leader_url}/groups/invite", json=payload)
        data = resp.json()
        if data.get("error"):
            raise HTTPException(status_code=400, detail=data["error"])
        return {"message": data.get("message", "Invitación enviada")}
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.warning(f"⚠️ Error invitando, reintentando: {e}")
        LEADER_CACHE.pop("groups", None)
        new_leader = await get_leader("groups")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{new_leader}/groups/invite", json=payload)
        data = resp.json()
        if data.get("error"):
            raise HTTPException(status_code=400, detail=data["error"])
        return {"message": data.get("message", "Invitación enviada")}

@app.get("/groups/invitations")
async def pending_group_invitations(token: str):
    user_data = await validate_token(token)
    user_id = user_data.get("user_id")
    for node_url in SHARDS["groups"]:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{node_url}/groups/invitations", params={"user_id": user_id})
                return resp.json()
        except Exception:
            continue
    raise HTTPException(status_code=503, detail="No hay nodos disponibles para consulta")

@app.get("/groups/invitations/count")
async def pending_group_invitations_count(token: str):
    user_data = await validate_token(token)
    user_id = user_data.get("user_id")
    for node_url in SHARDS["groups"]:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{node_url}/groups/invitations/count", params={"user_id": user_id})
                return resp.json()
        except Exception:
            continue
    raise HTTPException(status_code=503, detail="No hay nodos disponibles para consulta")

@app.post("/groups/invitations/respond")
async def respond_group_invitation(invitation_id: int, response: str, token: str):
    await validate_token(token)
    payload = {"invitation_id": invitation_id, "response": response}
    try:
        leader_url = await get_leader("groups")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{leader_url}/groups/invitations/respond", json=payload)
        data = resp.json()
        if data.get("error"):
            raise HTTPException(status_code=400, detail=data["error"])
        # Notificación sencilla al usuario que respondió (eco)
        asyncio.create_task(ws_manager.broadcast([payload.get("invitation_id")], {
            "type": "group_invitation",
            "invitation_id": invitation_id,
            "status": response
        }))
        return {"message": data.get("message", "Respuesta registrada")}
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.warning(f"⚠️ Error respondiendo invitación, reintentando: {e}")
        LEADER_CACHE.pop("groups", None)
        new_leader = await get_leader("groups")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{new_leader}/groups/invitations/respond", json=payload)
        data = resp.json()
        if data.get("error"):
            raise HTTPException(status_code=400, detail=data["error"])
        return {"message": data.get("message", "Respuesta registrada")}

@app.get("/groups/{group_id}/members")
async def list_group_members(group_id: int, token: str):
    await validate_token(token)
    for node_url in SHARDS["groups"]:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{node_url}/groups/{group_id}/members")
                return resp.json()
        except Exception:
            continue
    raise HTTPException(status_code=503, detail="No hay nodos disponibles para consulta")

@app.get("/groups/{group_id}/info")
async def get_group_info(group_id: int, token: str):
    await validate_token(token)
    for node_url in SHARDS["groups"]:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{node_url}/groups/{group_id}/info")
                data = resp.json()
                if data:
                    return data
        except Exception:
            continue
    raise HTTPException(status_code=404, detail="Grupo no encontrado")

@app.get("/groups/{group_id}/agendas")
async def get_group_agendas(group_id: int, token: str, start_date: str, end_date: str):
    """
    Ver agendas del grupo en un intervalo [start_date, end_date].
    Fechas esperadas: 'YYYY-MM-DD HH:MM:SS'
    """
    user_data = await validate_token(token)
    viewer_id = user_data.get("user_id")

    # Verificar que el usuario pertenece al grupo
    members = await _get_group_member_ids(group_id)
    if viewer_id not in members:
        raise HTTPException(status_code=403, detail="No perteneces a este grupo")

    # Obtener información del grupo para saber si es jerárquico
    group_info = None
    for node_url in SHARDS["groups"]:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{node_url}/groups/{group_id}/info")
                group_info = resp.json()
                if group_info:
                    break
        except Exception:
            continue

    if not group_info:
        raise HTTPException(status_code=404, detail="Grupo no encontrado")

    is_hierarchical = group_info.get("is_hierarchical", False)

    # Obtener miembros con sus roles
    members_with_roles = []
    for node_url in SHARDS["groups"]:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{node_url}/groups/{group_id}/members")
                members_data = resp.json()
                if isinstance(members_data, list):
                    members_with_roles = members_data
                    break
        except Exception:
            continue

    # Determinar miembros accesibles según jerarquía
    accessible_members = []
    viewer_is_leader = False

    for member in members_with_roles:
        if isinstance(member, (list, tuple)):
            member_id = int(member[0])
            username = member[1] if len(member) > 1 else str(member_id)
            is_leader = bool(member[2]) if len(member) > 2 else False
        else:
            member_id = int(member.get("user_id", 0))
            username = member.get("username", str(member_id))
            is_leader = bool(member.get("is_leader", False))

        if member_id == viewer_id:
            viewer_is_leader = is_leader

        # Lógica de acceso
        if member_id == viewer_id:
            accessible_members.append((member_id, username))
        elif not is_hierarchical:
            # Grupo no jerárquico: todos pueden ver a todos
            accessible_members.append((member_id, username))
        elif viewer_is_leader and not is_leader:
            # Líder puede ver agendas de miembros regulares
            accessible_members.append((member_id, username))

    if not accessible_members:
        raise HTTPException(status_code=403, detail="No tienes permisos para ver agendas de este grupo")

    # Construir resultado
    group_agendas = {}

    for member_id, username in accessible_members:
        # Obtener eventos del usuario en el rango de fechas desde todos los shards de eventos
        events = []
        for shard in _iter_event_shards():
            for node_url in SHARDS[shard]:
                try:
                    async with httpx.AsyncClient(timeout=5.0) as client:
                        resp = await client.get(f"{node_url}/events/detailed", params={"user_id": member_id, "filter_type": "accepted"})
                        data = resp.json()
                        if isinstance(data, list):
                            events.extend(data)
                            break
                except Exception:
                    continue

        # Filtrar por rango de fechas y aplicar privacidad
        filtered_events = []
        for event in events:
            start_time = event.get("start_time", "")
            end_time = event.get("end_time", "")

            # Verificar que el evento está en el rango solicitado
            if start_time >= start_date and end_time <= end_date:
                # Solo aceptados
                if event.get("is_accepted", 0) != 1:
                    continue

                # Privacidad: si el evento no corresponde al grupo consultado, ocultar detalles
                event_group_id = event.get("group_id")
                is_group_event = event.get("is_group_event", False)
                show_details = bool(is_group_event) and event_group_id == group_id

                safe_title = event.get("title") if show_details else "Ocupado"
                safe_description = event.get("description") if show_details else None

                filtered_events.append({
                    'id': event.get('id'),
                    'title': safe_title,
                    'description': safe_description,
                    'start_time': start_time,
                    'end_time': end_time,
                    'is_group_event': is_group_event,
                    'group_name': event.get('group_name'),
                    'creator_id': event.get('creator_id'),
                    'is_private': not show_details,
                })

        group_agendas[username] = {
            'user_id': member_id,
            'events': filtered_events
        }

    return group_agendas

@app.get("/groups/{group_id}/availability/common")
async def get_common_availability(group_id: int, token: str, start_date: str, end_date: str, duration_hours: float = 1.0):
    """Horarios comunes disponibles para todo el grupo."""
    from datetime import datetime, timedelta

    user_data = await validate_token(token)
    viewer_id = user_data.get("user_id")

    # Validar acceso al grupo (misma regla que agendas)
    members = await _get_group_member_ids(group_id)
    if viewer_id not in members:
        raise HTTPException(status_code=403, detail="No perteneces a este grupo")

    if not members:
        return []

    # Convertir fechas a datetime (asumiendo formato 'YYYY-MM-DD HH:MM:SS' o 'YYYY-MM-DD')
    try:
        if len(start_date.split()) == 1:
            start_dt = datetime.strptime(f"{start_date} 00:00:00", '%Y-%m-%d %H:%M:%S')
        else:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')

        if len(end_date.split()) == 1:
            end_dt = datetime.strptime(f"{end_date} 23:59:59", '%Y-%m-%d %H:%M:%S')
        else:
            end_dt = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato de fecha inválido. Use 'YYYY-MM-DD' o 'YYYY-MM-DD HH:MM:SS'")

    # Definir horario laboral (9:00 - 18:00)
    work_start_hour = 9
    work_end_hour = 18

    # Generar slots candidatos
    slots = []
    current = start_dt.replace(hour=work_start_hour, minute=0, second=0)
    slot_duration = timedelta(hours=duration_hours)

    while current <= end_dt:
        # Solo considerar horarios dentro del horario laboral
        if work_start_hour <= current.hour < work_end_hour:
            slot_end = current + slot_duration

            # Verificar que el slot completo está dentro del horario laboral
            if slot_end.hour <= work_end_hour:
                slots.append({
                    'start': current,
                    'end': slot_end
                })

        current += timedelta(minutes=30)

        # Saltar al siguiente día si pasamos las 18:00
        if current.hour >= work_end_hour:
            current = current.replace(hour=work_start_hour, minute=0) + timedelta(days=1)

    # Obtener eventos de todos los miembros
    member_events = {}
    for member_id in members:
        events = []
        for shard in _iter_event_shards():
            for node_url in SHARDS[shard]:
                try:
                    async with httpx.AsyncClient(timeout=5.0) as client:
                        resp = await client.get(f"{node_url}/events/detailed", params={"user_id": member_id, "filter_type": "accepted"})
                        data = resp.json()
                        if isinstance(data, list):
                            events.extend(data)
                            break
                except Exception:
                    continue
        member_events[member_id] = events

    # Filtrar slots que están libres para TODOS los miembros
    available_slots = []

    for slot in slots:
        is_available_for_all = True

        for member_id in members:
            # Verificar si el miembro tiene conflictos en este slot
            has_conflict = False
            for event in member_events.get(member_id, []):
                if event.get("is_accepted", 0) != 1:
                    continue

                try:
                    event_start = datetime.strptime(event.get("start_time", ""), '%Y-%m-%d %H:%M:%S')
                    event_end = datetime.strptime(event.get("end_time", ""), '%Y-%m-%d %H:%M:%S')

                    # Verificar solapamiento
                    if not (slot['end'] <= event_start or slot['start'] >= event_end):
                        has_conflict = True
                        break
                except (ValueError, TypeError):
                    continue

            if has_conflict:
                is_available_for_all = False
                break

        if is_available_for_all:
            available_slots.append({
                'start_time': slot['start'].strftime('%Y-%m-%d %H:%M:%S'),
                'end_time': slot['end'].strftime('%Y-%m-%d %H:%M:%S')
            })

    return available_slots

@app.put("/groups/{group_id}")
async def update_group(group_id: int, update: dict, token: str):
    user_data = await validate_token(token)
    user_id = user_data.get("user_id")

    # Agregar user_id al payload
    update["user_id"] = user_id

    try:
        leader_url = await get_leader("groups")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.put(f"{leader_url}/groups/{group_id}", json=update)
        data = resp.json()
        if data.get("error"):
            raise HTTPException(status_code=400, detail=data["error"])
        return {"message": data.get("message", "Grupo actualizado")}
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.warning(f"⚠️ Error actualizando grupo, reintentando: {e}")
        LEADER_CACHE.pop("groups", None)
        new_leader = await get_leader("groups")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.put(f"{new_leader}/groups/{group_id}", json=update)
        data = resp.json()
        if data.get("error"):
            raise HTTPException(status_code=400, detail=data["error"])
        return {"message": data.get("message", "Grupo actualizado")}

@app.delete("/groups/{group_id}")
async def delete_group(group_id: int, token: str):
    user_data = await validate_token(token)
    user_id = user_data.get("user_id")
    try:
        leader_url = await get_leader("groups")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.delete(f"{leader_url}/groups/{group_id}", params={"user_id": user_id})
        data = resp.json()
        if data.get("error"):
            raise HTTPException(status_code=400, detail=data["error"])
        return {"message": data.get("message", "Grupo eliminado")}
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.warning(f"⚠️ Error eliminando grupo, reintentando: {e}")
        LEADER_CACHE.pop("groups", None)
        new_leader = await get_leader("groups")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.delete(f"{new_leader}/groups/{group_id}", params={"user_id": user_id})
        data = resp.json()
        if data.get("error"):
            raise HTTPException(status_code=400, detail=data["error"])
        return {"message": data.get("message", "Grupo eliminado")}

@app.delete("/groups/{group_id}/members/{member_id}")
async def remove_group_member(group_id: int, member_id: int, token: str):
    user_data = await validate_token(token)
    requester_id = user_data.get("user_id")
    try:
        leader_url = await get_leader("groups")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.delete(
                f"{leader_url}/groups/{group_id}/members/{member_id}",
                params={"requester_id": requester_id}
            )
        data = resp.json()
        if data.get("error"):
            raise HTTPException(status_code=400, detail=data["error"])
        return {"message": data.get("message", "Miembro eliminado")}
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.warning(f"⚠️ Error eliminando miembro, reintentando: {e}")
        LEADER_CACHE.pop("groups", None)
        new_leader = await get_leader("groups")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.delete(
                f"{new_leader}/groups/{group_id}/members/{member_id}",
                params={"requester_id": requester_id}
            )
        data = resp.json()
        if data.get("error"):
            raise HTTPException(status_code=400, detail=data["error"])
        return {"message": data.get("message", "Miembro eliminado")}

@app.get("/events/detailed")
async def list_events_detailed(token: str, filter_type: str = "all"):
    user_data = await validate_token(token)
    user_id = user_data.get("user_id")
    events = []
    for shard in _iter_event_shards():
        for node_url in SHARDS[shard]:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.get(f"{node_url}/events/detailed", params={"user_id": user_id, "filter_type": filter_type})
                    data = resp.json()
                    if isinstance(data, list):
                        events.extend(data)
                        break
            except Exception:
                continue
    # Enriquecer y filtrar
    now_ts = asyncio.get_event_loop().time()
    enriched = []
    for ev in events:
        if ev.get("group_id") and not ev.get("group_name"):
            ev["group_name"] = await get_group_name(ev["group_id"])
        enriched.append(ev)

    if filter_type == "upcoming":
        filtered = [e for e in enriched if _is_future(e.get("start_time"), now_ts)]
    elif filter_type == "past":
        filtered = [e for e in enriched if not _is_future(e.get("start_time"), now_ts)]
    elif filter_type == "pending":
        filtered = [e for e in enriched if not e.get("is_creator") and int(e.get("is_accepted", 0)) == 0]
    elif filter_type == "created":
        filtered = [e for e in enriched if e.get("is_creator")]
    else:
        filtered = enriched
    return filtered

@app.get("/events/invitations")
async def pending_event_invitations(token: str):
    user_data = await validate_token(token)
    user_id = user_data.get("user_id")
    invitations = []
    for shard in _iter_event_shards():
        for node_url in SHARDS[shard]:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.get(f"{node_url}/events/invitations", params={"user_id": user_id})
                    data = resp.json()
                    if isinstance(data, list):
                        invitations.extend(data)
                        break
            except Exception:
                continue
    enriched = []
    for inv in invitations:
        # inv tuple: (event_id, title, description, start_time, end_time, creator_name, group_name, is_group_event, group_id)
        if isinstance(inv, (list, tuple)) and len(inv) >= 9:
            event_id, title, desc, start_time, end_time, creator_name, group_name, is_group_event, group_id = inv
            if not group_name and group_id:
                group_name = await get_group_name(group_id)
            enriched.append((event_id, title, desc, start_time, end_time, creator_name, group_name, is_group_event, group_id))
        else:
            enriched.append(inv)
    return enriched

@app.get("/events/invitations/count")
async def pending_event_invitations_count(token: str):
    user_data = await validate_token(token)
    user_id = user_data.get("user_id")
    total = 0
    for shard in _iter_event_shards():
        for node_url in SHARDS[shard]:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.get(f"{node_url}/events/invitations/count", params={"user_id": user_id})
                    data = resp.json()
                    total += data.get("count", 0)
                    break
            except Exception:
                continue
    return {"count": total}

@app.post("/events/invitations/respond")
async def respond_event_invitation(event_id: int, accepted: bool, token: str):
    user_data = await validate_token(token)
    user_id = user_data.get("user_id")
    payload = {"event_id": event_id, "user_id": user_id, "accepted": bool(accepted)}
    # Intentar en ambos shards hasta que uno responda OK
    last_error = None
    for shard in _iter_event_shards():
        try:
            leader_url = await get_leader(shard)
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(f"{leader_url}/events/invitations/respond", json=payload)
            data = resp.json()
            if data.get("error"):
                last_error = data.get("error")
                continue
            # Notificar creador si aceptan/declinan
            if accepted:
                asyncio.create_task(ws_manager.broadcast([user_id], {
                    "type": "event_accepted",
                    "event_id": event_id
                }))
            else:
                asyncio.create_task(ws_manager.broadcast([user_id], {
                    "type": "event_declined",
                    "event_id": event_id
                }))
            return {"message": data.get("message", "Respuesta registrada")}
        except Exception as e:
            last_error = str(e)
            LEADER_CACHE.pop(shard, None)
            continue
    raise HTTPException(status_code=400, detail=last_error or "No se pudo registrar respuesta")

@app.put("/events/{event_id}")
async def update_event(event_id: int, update: dict, token: str):
    user_data = await validate_token(token)
    payload = {"participants_ids": update.get("participants_ids")}
    payload["requester_id"] = user_data.get("user_id")
    for key in ["title", "description", "start_time", "end_time"]:
        if key in update:
            payload[key] = update[key]
    # Validar grupo y miembros si aplica
    if update.get("group_id"):
        members = await _get_group_member_ids(update.get("group_id"))
        if payload["requester_id"] not in members:
            raise HTTPException(status_code=400, detail="No perteneces al grupo")
        if payload["participants_ids"]:
            invalid = [pid for pid in payload["participants_ids"] if pid not in members]
            if invalid:
                raise HTTPException(status_code=400, detail="Hay participantes que no pertenecen al grupo")
    last_error = None
    for shard in _iter_event_shards():
        try:
            leader_url = await get_leader(shard)
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.put(f"{leader_url}/events/{event_id}", json=payload)
            data = resp.json()
            if data.get("error"):
                last_error = data.get("error")
                continue
            # Notificar participantes sobre reprogramación
            for pid in payload.get("participants_ids") or []:
                asyncio.create_task(ws_manager.send_to_user(pid, {
                    "type": "event_updated",
                    "event_id": event_id,
                    "title": update.get("title"),
                    "start_time": update.get("start_time"),
                    "end_time": update.get("end_time"),
                }))
            return {"message": data.get("message", "Evento actualizado")}
        except Exception as e:
            last_error = str(e)
            LEADER_CACHE.pop(shard, None)
            continue
    raise HTTPException(status_code=400, detail=last_error or "No se pudo actualizar evento")

@app.delete("/events/{event_id}")
async def cancel_event(event_id: int, token: str):
    user_data = await validate_token(token)
    user_id = user_data.get("user_id")
    last_error = None
    for shard in _iter_event_shards():
        try:
            leader_url = await get_leader(shard)
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.delete(f"{leader_url}/events/{event_id}", params={"user_id": user_id})
            data = resp.json()
            if data.get("error"):
                last_error = data.get("error")
                continue
            # Notificar participantes
            asyncio.create_task(ws_manager.broadcast([user_id], {
                "type": "event_cancelled",
                "event_id": event_id
            }))
            return {"message": data.get("message", "Evento cancelado")}
        except Exception as e:
            last_error = str(e)
            LEADER_CACHE.pop(shard, None)
            continue
    raise HTTPException(status_code=400, detail=last_error or "No se pudo cancelar evento")

@app.delete("/events/{event_id}/leave")
async def leave_event(event_id: int, token: str):
    user_data = await validate_token(token)
    user_id = user_data.get("user_id")
    last_error = None
    for shard in _iter_event_shards():
        try:
            leader_url = await get_leader(shard)
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.delete(f"{leader_url}/events/{event_id}/leave", params={"user_id": user_id})
            data = resp.json()
            if data.get("error"):
                last_error = data.get("error")
                continue
            return {"message": data.get("message", "Has salido del evento")}
        except Exception as e:
            last_error = str(e)
            LEADER_CACHE.pop(shard, None)
            continue
    raise HTTPException(status_code=400, detail=last_error or "No se pudo salir del evento")

@app.get("/events/{event_id}/details")
async def event_details(event_id: int, token: str):
    await validate_token(token)
    for shard in _iter_event_shards():
        for node_url in SHARDS[shard]:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.get(f"{node_url}/events/{event_id}/details", params={"user_id": 0})
                    data = resp.json()
                    if data:
                        if data.get("group_id") and not data.get("group_name"):
                            data["group_name"] = await get_group_name(data["group_id"])
                        # Enriquecer participantes con usernames desde shard usuarios si falta
                        for p in data.get("participants", []):
                            if not p.get("username") and p.get("user_id"):
                                uname = await get_username_by_id(p["user_id"])
                                if uname:
                                    p["username"] = uname
                        return data
            except Exception:
                continue
    raise HTTPException(status_code=404, detail="Evento no encontrado")

@app.get("/events/conflicts")
async def get_event_conflicts(token: str, limit: int = 50):
    user_data = await validate_token(token)
    user_id = user_data.get("user_id")
    conflicts = []
    for shard in _iter_event_shards():
        for node_url in SHARDS[shard]:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.get(f"{node_url}/events/conflicts", params={"user_id": user_id, "limit": limit})
                    data = resp.json()
                    if isinstance(data, list):
                        conflicts.extend(data)
                        break
            except Exception:
                continue
    return conflicts

# =========================================================
# 🕒 Utilidades de tiempo para filtros
# =========================================================
def _parse_dt(dt_str: str):
    try:
        from datetime import datetime
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").timestamp()
    except Exception:
        return None

def _is_future(dt_str: str, now_ts: float):
    ts = _parse_dt(dt_str)
    return ts is None or ts >= now_ts

@app.get("/leaders")
async def get_leaders():
    """Lista los líderes actuales por shard"""
    await prune_missing_nodes()
    results = {}
    for shard in SHARDS.keys():
        try:
            leader_url = await get_leader(shard)
            results[shard] = {
                "leader": leader_url,
                "nodes": SHARDS[shard],
                "status": "active"
            }
        except Exception as e:
            results[shard] = {
                "leader": "No disponible",
                "nodes": SHARDS[shard],
                "status": "error",
                "error": str(e)
            }
    return results

@app.get("/health")
async def health_check():
    """Endpoint de salud del coordinador"""
    return {
        "status": "healthy",
        "service": "coordinator",
        "total_shards": len(SHARDS),
        "nodes_per_shard": NODES_PER_SHARD,
        "timestamp": asyncio.get_event_loop().time()
    }

@app.get("/cluster/status")
async def cluster_status():
    """Estado completo del cluster"""
    shard_status = {}
    
    for shard_name, nodes in SHARDS.items():
        node_status = {}
        for node_url in nodes:
            try:
                async with httpx.AsyncClient(timeout=3.0) as client:
                    resp = await client.get(f"{node_url}/health")
                    node_status[node_url] = resp.json()
            except Exception as e:
                node_status[node_url] = {"status": "unreachable", "error": str(e)}
        
        shard_status[shard_name] = node_status
    
    return {
        "coordinator": "healthy",
        "shards": shard_status,
        "total_nodes": sum(len(nodes) for nodes in SHARDS.values())
    }

@app.post("/admin/shards/add")
async def admin_add_node(data: dict):
    """Agrega un nodo a un shard en caliente (actualiza en memoria y limpia caché)."""
    shard = _canonical_shard(data.get("shard"))
    node_url = data.get("node_url")
    if not shard or not node_url:
        raise HTTPException(status_code=400, detail="Se requiere shard y node_url")
    if not _is_valid_node_url(node_url):
        raise HTTPException(status_code=400, detail="node_url debe empezar con http:// o https://")
    add_node_to_shard(shard, node_url)
    await propagate_peers_to_shard(shard)
    return {"status": "ok", "shards": SHARDS}

@app.post("/admin/shards/replace")
async def admin_replace_node(data: dict):
    """Reemplaza un nodo en un shard en caliente."""
    shard = _canonical_shard(data.get("shard"))
    old_url = data.get("old_url")
    new_url = data.get("new_url")
    if not shard or not old_url or not new_url:
        raise HTTPException(status_code=400, detail="Se requiere shard, old_url y new_url")
    replace_node_in_shard(shard, old_url, new_url)
    await propagate_peers_to_shard(shard)
    return {"status": "ok", "shards": SHARDS}

@app.get("/")
def root():
    return {
        "message": "🧭 Coordinador RAFT - Docker Swarm", 
        "total_shards": len(SHARDS),
        "nodes_per_shard": NODES_PER_SHARD,
        "tolerance": "Depende de quórum; a más réplicas, más fallos tolerados",
        "endpoints": {
            "create_event": "POST /events",
            "create_group": "POST /groups", 
            "create_user": "POST /users",
            "list_events": "GET /events",
            "list_groups": "GET /groups",
            "list_users": "GET /users",
            "leaders": "GET /leaders",
            "cluster_status": "GET /cluster/status"
        }
    }
