from fastapi import FastAPI, Request
import sqlite3
import os
import asyncio
import logging
import json
import httpx
import bcrypt
import secrets
from datetime import datetime
from shared.raft import RaftNode

# Leer configuración básica
SHARD_NAME = os.getenv("SHARD_NAME", "DEFAULT_SHARD").upper().strip()
NODE_ID = os.getenv("NODE_ID", "node0")
PORT = int(os.getenv("PORT", "8800"))
PEERS = [peer.strip() for peer in os.getenv("PEERS", "").split(",") if peer.strip()]
NODE_URL = os.getenv("NODE_URL", f"http://localhost:{PORT}")
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "0") or 0)
COORD_URL = os.getenv("COORD_URL")
COORD_URLS = os.getenv("COORD_URLS")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(f"raft_{NODE_ID}")

app = FastAPI(title=f"Shard {SHARD_NAME} - {NODE_ID}")

# Base de datos local
os.makedirs("data", exist_ok=True)

if "EVENTOS" in SHARD_NAME:
    DB_PATH = os.path.join("data", f"events_{NODE_ID}.db")
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        description TEXT,
        creator_id INTEGER NOT NULL,
        creator_username TEXT NOT NULL,
        start_time TEXT NOT NULL,
        end_time TEXT NOT NULL,
        group_id INTEGER,
        is_group_event INTEGER DEFAULT 0,
        is_hierarchical_event INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    # Migración simple para agregar columna si falta
    cursor.execute("PRAGMA table_info(events)")
    ecols = [r[1] for r in cursor.fetchall()]
    if "is_hierarchical_event" not in ecols:
        cursor.execute("ALTER TABLE events ADD COLUMN is_hierarchical_event INTEGER DEFAULT 0")
        conn.commit()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS event_participants (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        username TEXT,
        is_accepted INTEGER DEFAULT 0,
        UNIQUE(event_id, user_id)
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS event_conflicts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        reason TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()
elif "GRUPOS" in SHARD_NAME:
    DB_PATH = os.path.join("data", f"groups_{NODE_ID}.db")
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS groups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        description TEXT,
        is_hierarchical INTEGER DEFAULT 0,
        creator_id INTEGER,
        creator_username TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    # Migración simple: agregar columna si falta
    cursor.execute("PRAGMA table_info(groups)")
    cols = [r[1] for r in cursor.fetchall()]
    if "is_hierarchical" not in cols:
        cursor.execute("ALTER TABLE groups ADD COLUMN is_hierarchical INTEGER DEFAULT 0")
        conn.commit()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS group_members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        username TEXT,
        is_leader INTEGER DEFAULT 0,
        UNIQUE(group_id, user_id)
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS group_invitations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id INTEGER NOT NULL,
        invited_user_id INTEGER NOT NULL,
        invited_username TEXT,
        inviter_id INTEGER NOT NULL,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(group_id, invited_user_id)
    );
    """)
    conn.commit()
elif "USUARIOS" in SHARD_NAME:
    DB_PATH = os.path.join("data", f"users_{NODE_ID}.db")
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        email TEXT UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()
else:
    raise ValueError(f"Shard desconocido: {SHARD_NAME}")


async def apply_log_entry(entry):
    try:
        data = json.loads(entry.command)
    except Exception:
        return
    t = data.get("type")
    p = data.get("payload", {})

    if t == "CREATE_USER" and "USUARIOS" in SHARD_NAME:
        try:
            cursor.execute("INSERT INTO users (username, password_hash, email) VALUES (?, ?, ?)",
                           (p.get("username"), p.get("password_hash"), p.get("email")))
            conn.commit()
        except sqlite3.IntegrityError:
            pass
    elif t == "CREATE_SESSION" and "USUARIOS" in SHARD_NAME:
        try:
            cursor.execute("INSERT OR REPLACE INTO sessions (token, user_id, created_at) VALUES (?, ?, ?)",
                           (p.get("token"), p.get("user_id"), p.get("created_at") or datetime.utcnow().isoformat()))
            conn.commit()
        except Exception:
            pass
    elif t == "CREATE_GROUP" and "GRUPOS" in SHARD_NAME:
        cursor.execute(
            "INSERT INTO groups (name, description, is_hierarchical, creator_id, creator_username) VALUES (?, ?, ?, ?, ?)",
            (
                p.get("name"),
                p.get("description"),
                1 if p.get("is_hierarchical") else 0,
                p.get("creator_id"),
                p.get("creator_username"),
            )
        )
        gid = cursor.lastrowid
        cursor.execute(
            "INSERT OR IGNORE INTO group_members (group_id, user_id, username, is_leader) VALUES (?, ?, ?, 1)",
            (gid, p.get("creator_id"), p.get("creator_username"))
        )
        # Crear invitaciones para miembros iniciales (opcional) enviados en payload
        for mid in p.get("members") or []:
            if mid == p.get("creator_id"):
                continue
            cursor.execute(
                """
                INSERT OR IGNORE INTO group_invitations (group_id, invited_user_id, invited_username, inviter_id, status)
                VALUES (?, ?, ?, ?, 'pending')
                """,
                (gid, mid, "", p.get("creator_id")),
            )
        conn.commit()
    elif t == "INVITE_USER" and "GRUPOS" in SHARD_NAME:
        cursor.execute("""
            INSERT OR REPLACE INTO group_invitations (group_id, invited_user_id, invited_username, inviter_id, status)
            VALUES (?, ?, ?, ?, 'pending')
        """, (p.get("group_id"), p.get("invited_user_id"), p.get("invited_username"), p.get("inviter_id")))
        conn.commit()
    elif t == "RESPOND_INVITATION" and "GRUPOS" in SHARD_NAME:
        cursor.execute("UPDATE group_invitations SET status=? WHERE id=?", (p.get("response"), p.get("invitation_id")))
        if p.get("response") == "accepted":
            cursor.execute("SELECT group_id, invited_user_id, invited_username FROM group_invitations WHERE id=?",
                           (p.get("invitation_id"),))
            row = cursor.fetchone()
            if row:
                cursor.execute(
                    "INSERT OR IGNORE INTO group_members (group_id, user_id, username, is_leader) VALUES (?, ?, ?, 0)",
                    (row[0], row[1], row[2])
                )
        conn.commit()
    elif t == "CREATE_EVENT" and "EVENTOS" in SHARD_NAME:
        cursor.execute("""
            INSERT INTO events (title, description, creator_id, creator_username, start_time, end_time, group_id, is_group_event, is_hierarchical_event)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (p.get("title"), p.get("description"), p.get("creator_id"), p.get("creator_username"),
              p.get("start_time"), p.get("end_time"), p.get("group_id"),
              1 if p.get("is_group_event") else 0,
              1 if p.get("is_hierarchical") or p.get("is_hierarchical_event") else 0))
        eid = cursor.lastrowid
        # creador aceptado
        cursor.execute(
            "INSERT OR REPLACE INTO event_participants (event_id, user_id, username, is_accepted) VALUES (?, ?, ?, 1)",
            (eid, p.get("creator_id"), p.get("creator_username"))
        )
        for pid in p.get("participants_ids") or []:
            cursor.execute(
                "INSERT OR IGNORE INTO event_participants (event_id, user_id, is_accepted) VALUES (?, ?, ?)",
                (
                    eid,
                    pid,
                    1 if (p.get("is_hierarchical") or p.get("is_hierarchical_event")) else 0,
                ),
            )
        conn.commit()
    elif t == "RESPOND_EVENT_INVITATION" and "EVENTOS" in SHARD_NAME:
        cursor.execute(
            "UPDATE event_participants SET is_accepted=? WHERE event_id=? AND user_id=?",
            (1 if p.get("accepted") else 0, p.get("event_id"), p.get("user_id"))
        )
        conn.commit()
    elif t == "UPDATE_GROUP" and "GRUPOS" in SHARD_NAME:
        # Actualizar nombre y/o descripción del grupo
        name = p.get("name")
        description = p.get("description")
        group_id = p.get("group_id")
        if name and description is not None:
            cursor.execute("UPDATE groups SET name=?, description=? WHERE id=?", (name, description, group_id))
        elif name:
            cursor.execute("UPDATE groups SET name=? WHERE id=?", (name, group_id))
        elif description is not None:
            cursor.execute("UPDATE groups SET description=? WHERE id=?", (description, group_id))
        conn.commit()
    elif t == "DELETE_GROUP" and "GRUPOS" in SHARD_NAME:
        # Eliminar grupo y sus relaciones
        group_id = p.get("group_id")
        cursor.execute("DELETE FROM group_invitations WHERE group_id=?", (group_id,))
        cursor.execute("DELETE FROM group_members WHERE group_id=?", (group_id,))
        cursor.execute("DELETE FROM groups WHERE id=?", (group_id,))
        conn.commit()
    elif t == "DELETE_MEMBER" and "GRUPOS" in SHARD_NAME:
        # Eliminar un miembro del grupo
        group_id = p.get("group_id")
        member_id = p.get("member_id")
        cursor.execute("DELETE FROM group_members WHERE group_id=? AND user_id=?", (group_id, member_id))
        conn.commit()
    elif t == "UPDATE_EVENT" and "EVENTOS" in SHARD_NAME:
        # Actualizar un evento
        event_id = p.get("event_id")
        title = p.get("title")
        description = p.get("description")
        start_time = p.get("start_time")
        end_time = p.get("end_time")

        # Construir la query de actualización dinámicamente
        updates = []
        params = []

        if title is not None:
            updates.append("title = ?")
            params.append(title)
        if description is not None:
            updates.append("description = ?")
            params.append(description)
        if start_time is not None:
            updates.append("start_time = ?")
            params.append(start_time)
        if end_time is not None:
            updates.append("end_time = ?")
            params.append(end_time)

        if updates:
            params.append(event_id)
            query = f"UPDATE events SET {', '.join(updates)} WHERE id = ?"
            cursor.execute(query, tuple(params))

        # Si cambió el horario, resetear aceptación de participantes (excepto creador)
        time_changed = p.get("time_changed", False)
        if time_changed:
            cursor.execute("SELECT creator_id FROM events WHERE id=?", (event_id,))
            row = cursor.fetchone()
            if row:
                creator_id = row[0]
                cursor.execute(
                    "UPDATE event_participants SET is_accepted = 0 WHERE event_id = ? AND user_id != ?",
                    (event_id, creator_id)
                )

        conn.commit()


raft = RaftNode(
    node_id=NODE_ID,
    peers=PEERS,
    state_file=f"data/{NODE_ID}_state.json",
    heartbeat_interval=1.0,
    election_timeout_range=(2.0, 4.0),
    state_machine_callback=apply_log_entry,
    self_url=NODE_URL,
    replication_factor=REPLICATION_FACTOR or None,
)


@app.on_event("startup")
async def startup():
    asyncio.create_task(raft.start())
    if COORD_URL:
        asyncio.create_task(register_in_coordinator())


async def register_in_coordinator():
    urls = []
    if COORD_URLS:
        urls.extend([u.strip() for u in COORD_URLS.split(",") if u.strip()])
    if COORD_URL:
        urls.append(COORD_URL)
    urls = [u for u in urls if u]
    if not urls:
        return

    payload = {"shard": SHARD_NAME.lower(), "node_url": NODE_URL}
    seen_ok = {url: False for url in urls}
    # Reintentar siempre: si un coordinador se reinicia, nos volvemos a registrar
    while True:
        for url in urls:
            try:
                async with httpx.AsyncClient(timeout=3.0) as client:
                    resp = await client.post(f"{url}/admin/shards/add", json=payload)
                    if resp.status_code == 200 and not seen_ok[url]:
                        logger.info(f"📣 Nodo {NODE_ID} registrado en coordinador {url}")
                        seen_ok[url] = True
            except Exception as e:
                logger.warning(f"Error registrando en coordinador {url}: {e}")
                seen_ok[url] = False
        await asyncio.sleep(10)


# ========= Endpoints de aplicación según shard =========

@app.post("/admin/peers/update")
async def admin_update_peers(data: dict):
    peers = data.get("peers") or []
    repl = data.get("replication_factor")
    await raft.update_peers(peers, replication_factor=repl)
    return {"status": "ok", "peers": peers, "replication_factor": raft.replication_factor}

if "USUARIOS" in SHARD_NAME:
    def _get_user(username: str):
        cursor.execute("SELECT id, username, password_hash FROM users WHERE username = ?", (username,))
        return cursor.fetchone()

    @app.post("/auth/register")
    async def auth_register(user: dict):
        if not raft.is_leader():
            return {"error": "No soy el líder", "leader": raft.leader_id}
        username = (user.get("username") or "").strip()
        password = user.get("password") or ""
        email = user.get("email")
        if not username or not password:
            return {"error": "Usuario y contraseña son requeridos"}
        cursor.execute("SELECT 1 FROM users WHERE username=?", (username,))
        if cursor.fetchone():
            return {"error": "El nombre de usuario ya existe"}
        password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        cmd = json.dumps({"type": "CREATE_USER", "payload": {"username": username, "password_hash": password_hash, "email": email}})
        entry = raft.append_log(cmd)
        replicated = await raft.replicate_log(entry)
        if not replicated:
            return {"error": "No se pudo replicar el usuario en la mayoría de nodos"}
        await raft.apply_to_state_machine(entry)
        raft.last_applied = max(raft.last_applied, entry.index)
        raft.save_state()
        cursor.execute("SELECT id FROM users WHERE username=?", (username,))
        row = cursor.fetchone()
        return {"message": "Usuario registrado exitosamente", "user_id": row[0] if row else None}

    @app.post("/auth/login")
    async def auth_login(user: dict):
        if not raft.is_leader():
            return {"error": "No soy el líder", "leader": raft.leader_id}
        username = (user.get("username") or "").strip()
        password = user.get("password") or ""
        if not username or not password:
            return {"error": "Usuario y contraseña son requeridos"}
        db_user = _get_user(username)
        if not db_user:
            return {"error": "Credenciales inválidas", "status_code": 401}
        _, _, stored_hash = db_user
        try:
            valid = bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8") if isinstance(stored_hash, str) else stored_hash)
        except Exception:
            valid = False
        if not valid:
            return {"error": "Credenciales inválidas", "status_code": 401}
        user_id = db_user[0]
        token = secrets.token_hex(16)
        cmd = json.dumps({"type": "CREATE_SESSION", "payload": {"token": token, "user_id": user_id, "created_at": datetime.utcnow().isoformat()}})
        entry = raft.append_log(cmd)
        replicated = await raft.replicate_log(entry)
        if not replicated:
            return {"error": "No se pudo replicar la sesión en la mayoría de nodos"}
        await raft.apply_to_state_machine(entry)
        raft.last_applied = max(raft.last_applied, entry.index)
        raft.save_state()
        return {"token": token, "user_id": user_id}

    @app.get("/auth/validate")
    def auth_validate(token: str):
        cursor.execute("SELECT s.user_id, u.username FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token=?", (token,))
        row = cursor.fetchone()
        if not row:
            return {"valid": False}
        return {"valid": True, "user_id": row[0], "username": row[1]}

    @app.post("/users")
    async def create_user_legacy(user: dict):
        return await auth_register(user)

    @app.get("/users")
    def list_users():
        cursor.execute("SELECT id, username FROM users")
        return [(r[0], r[1]) for r in cursor.fetchall()]

    @app.get("/users/{user_id}")
    def get_user(user_id: int):
        cursor.execute("SELECT id, username FROM users WHERE id = ?", (user_id,))
        row = cursor.fetchone()
        if not row:
            return {}
        return {"id": row[0], "username": row[1]}

elif "GRUPOS" in SHARD_NAME:
    @app.post("/groups")
    async def create_group(group: dict):
        if not raft.is_leader():
            return {"error": "No soy el líder", "leader": raft.leader_id}
        cmd = json.dumps({"type": "CREATE_GROUP", "payload": group})
        entry = raft.append_log(cmd)
        replicated = await raft.replicate_log(entry)
        if not replicated:
            return {"error": "No se pudo replicar el grupo en la mayoría de nodos"}
        await raft.apply_to_state_machine(entry)
        raft.last_applied = max(raft.last_applied, entry.index)
        raft.save_state()
        cursor.execute("SELECT id FROM groups WHERE name=? ORDER BY id DESC LIMIT 1", (group.get("name"),))
        row = cursor.fetchone()
        return {"status": "ok", "message": f"Grupo '{group.get('name')}' creado", "group_id": row[0] if row else None}

    @app.get("/groups")
    def list_groups(user_id: int):
        cursor.execute("""
            SELECT g.id, g.name, g.description, g.is_hierarchical, g.creator_id
            FROM groups g
            JOIN group_members gm ON gm.group_id = g.id
            WHERE gm.user_id = ?
        """, (user_id,))
        return [{
            "id": r[0],
            "name": r[1],
            "description": r[2],
            "is_hierarchical": bool(r[3]),
            "creator_id": r[4],
        } for r in cursor.fetchall()]

    @app.get("/groups/{group_id}/members")
    def group_members(group_id: int):
        cursor.execute("SELECT user_id, COALESCE(username, CAST(user_id AS TEXT)), is_leader FROM group_members WHERE group_id=?", (group_id,))
        return [(r[0], r[1], r[2]) for r in cursor.fetchall()]

    @app.get("/groups/{group_id}/info")
    def group_info(group_id: int):
        cursor.execute("SELECT id, name, description, is_hierarchical, creator_id FROM groups WHERE id=?", (group_id,))
        row = cursor.fetchone()
        if not row:
            return {}
        return {
            "id": row[0],
            "name": row[1],
            "description": row[2],
            "is_hierarchical": bool(row[3]),
            "creator_id": row[4]
        }

    @app.post("/groups/invite")
    async def invite_user(invite: dict):
        if not raft.is_leader():
            return {"error": "No soy el líder", "leader": raft.leader_id}
        cmd = json.dumps({"type": "INVITE_USER", "payload": invite})
        entry = raft.append_log(cmd)
        replicated = await raft.replicate_log(entry)
        if not replicated:
            return {"error": "No se pudo replicar la invitación en la mayoría de nodos"}
        await raft.apply_to_state_machine(entry)
        raft.last_applied = max(raft.last_applied, entry.index)
        raft.save_state()
        return {"status": "ok", "message": "Invitación enviada"}

    @app.get("/groups/invitations")
    def pending_invitations(user_id: int):
        cursor.execute("""
            SELECT id, group_id, invited_user_id, invited_username, inviter_id, status
            FROM group_invitations
            WHERE invited_user_id=? AND status='pending'
        """, (user_id,))
        rows = cursor.fetchall()
        return [{"id": r[0], "group_id": r[1], "invited_user_id": r[2], "invited_username": r[3], "inviter_id": r[4], "status": r[5]} for r in rows]

    @app.get("/groups/invitations/count")
    def pending_invitations_count(user_id: int):
        cursor.execute("SELECT COUNT(1) FROM group_invitations WHERE invited_user_id=? AND status='pending'", (user_id,))
        row = cursor.fetchone()
        return {"count": row[0] if row else 0}

    @app.post("/groups/invitations/respond")
    async def respond_invitation(data: dict):
        if not raft.is_leader():
            return {"error": "No soy el líder", "leader": raft.leader_id}
        cmd = json.dumps({"type": "RESPOND_INVITATION", "payload": data})
        entry = raft.append_log(cmd)
        replicated = await raft.replicate_log(entry)
        if not replicated:
            return {"error": "No se pudo replicar la respuesta"}
        await raft.apply_to_state_machine(entry)
        raft.last_applied = max(raft.last_applied, entry.index)
        raft.save_state()
        return {"status": "ok", "message": "Respuesta registrada"}

    @app.put("/groups/{group_id}")
    async def update_group(group_id: int, update: dict):
        """Actualizar nombre y/o descripción del grupo"""
        if not raft.is_leader():
            return {"error": "No soy el líder", "leader": raft.leader_id}

        # Verificar que el grupo existe y obtener su creador
        cursor.execute("SELECT creator_id FROM groups WHERE id=?", (group_id,))
        row = cursor.fetchone()
        if not row:
            return {"error": "Grupo no encontrado"}

        user_id = update.get("user_id")
        if not user_id:
            return {"error": "user_id requerido"}

        # Verificar que el usuario es líder del grupo
        cursor.execute("SELECT is_leader FROM group_members WHERE group_id=? AND user_id=?", (group_id, user_id))
        member = cursor.fetchone()
        if not member or not member[0]:
            return {"error": "Solo los líderes pueden editar el grupo"}

        # Preparar payload
        payload = {"group_id": group_id}
        if update.get("name"):
            payload["name"] = update["name"]
        if "description" in update:
            payload["description"] = update["description"]

        cmd = json.dumps({"type": "UPDATE_GROUP", "payload": payload})
        entry = raft.append_log(cmd)
        replicated = await raft.replicate_log(entry)
        if not replicated:
            return {"error": "No se pudo replicar la actualización"}
        await raft.apply_to_state_machine(entry)
        raft.last_applied = max(raft.last_applied, entry.index)
        raft.save_state()
        return {"status": "ok", "message": "Grupo actualizado exitosamente"}

    @app.delete("/groups/{group_id}")
    async def delete_group(group_id: int, user_id: int):
        """Eliminar un grupo completamente"""
        if not raft.is_leader():
            return {"error": "No soy el líder", "leader": raft.leader_id}

        # Verificar que el grupo existe y obtener su creador
        cursor.execute("SELECT creator_id FROM groups WHERE id=?", (group_id,))
        row = cursor.fetchone()
        if not row:
            return {"error": "Grupo no encontrado"}

        creator_id = row[0]
        if creator_id != user_id:
            return {"error": "Solo el creador del grupo puede eliminarlo"}

        payload = {"group_id": group_id}
        cmd = json.dumps({"type": "DELETE_GROUP", "payload": payload})
        entry = raft.append_log(cmd)
        replicated = await raft.replicate_log(entry)
        if not replicated:
            return {"error": "No se pudo replicar la eliminación"}
        await raft.apply_to_state_machine(entry)
        raft.last_applied = max(raft.last_applied, entry.index)
        raft.save_state()
        return {"status": "ok", "message": "Grupo eliminado exitosamente"}

    @app.delete("/groups/{group_id}/members/{member_id}")
    async def delete_member(group_id: int, member_id: int, requester_id: int):
        """Eliminar un miembro del grupo"""
        if not raft.is_leader():
            return {"error": "No soy el líder", "leader": raft.leader_id}

        # Verificar que el solicitante es líder del grupo
        cursor.execute("SELECT is_leader FROM group_members WHERE group_id=? AND user_id=?", (group_id, requester_id))
        requester = cursor.fetchone()
        if not requester or not requester[0]:
            return {"error": "Solo los líderes pueden eliminar miembros"}

        # No permitir que el líder se elimine a sí mismo
        if requester_id == member_id:
            return {"error": "No puedes eliminarte a ti mismo del grupo"}

        payload = {"group_id": group_id, "member_id": member_id}
        cmd = json.dumps({"type": "DELETE_MEMBER", "payload": payload})
        entry = raft.append_log(cmd)
        replicated = await raft.replicate_log(entry)
        if not replicated:
            return {"error": "No se pudo replicar la eliminación del miembro"}
        await raft.apply_to_state_machine(entry)
        raft.last_applied = max(raft.last_applied, entry.index)
        raft.save_state()
        return {"status": "ok", "message": "Miembro eliminado exitosamente"}

elif "EVENTOS" in SHARD_NAME:
    @app.post("/events")
    async def create_event(event: dict):
        if not raft.is_leader():
            return {"error": "No soy el líder", "leader": raft.leader_id}
        cmd = json.dumps({"type": "CREATE_EVENT", "payload": event})
        entry = raft.append_log(cmd)
        replicated = await raft.replicate_log(entry)
        if not replicated:
            return {"error": "No se pudo replicar el evento en la mayoría de nodos"}
        await raft.apply_to_state_machine(entry)
        raft.last_applied = max(raft.last_applied, entry.index)
        raft.save_state()
        cursor.execute("SELECT id FROM events WHERE title=? AND creator_id=? ORDER BY id DESC LIMIT 1",
                       (event.get("title"), event.get("creator_id")))
        row = cursor.fetchone()
        return {"status": "ok", "message": f"Evento '{event.get('title')}' replicado y guardado en {SHARD_NAME}", "node": NODE_ID, "event_id": row[0] if row else None}

    @app.get("/events")
    def list_events(user_id: int):
        cursor.execute("""
            SELECT e.id, e.title, e.description, e.start_time, e.end_time, e.creator_id, e.creator_username, e.group_id, e.is_group_event, e.is_hierarchical_event
            FROM events e JOIN event_participants ep ON ep.event_id = e.id
            WHERE ep.user_id = ?
        """, (user_id,))
        rows = cursor.fetchall()
        return [{
            "id": r[0], "title": r[1], "description": r[2], "start_time": r[3], "end_time": r[4],
            "creator_id": r[5], "creator_name": r[6], "group_id": r[7],
            "is_group_event": bool(r[8]),
            "is_hierarchical_event": bool(r[9])
        } for r in rows]

    @app.get("/events/detailed")
    def list_events_detailed(user_id: int, filter_type: str = "all"):
        cursor.execute("""
            SELECT e.id, e.title, e.description, e.start_time, e.end_time, e.creator_id, e.creator_username,
                   e.group_id, e.is_group_event, ep.is_accepted, e.is_hierarchical_event
            FROM events e JOIN event_participants ep ON ep.event_id = e.id
            WHERE ep.user_id = ?
        """, (user_id,))
        rows = cursor.fetchall()
        events = []
        for r in rows:
            events.append({
                "id": r[0], "title": r[1], "description": r[2], "start_time": r[3], "end_time": r[4],
                "creator_id": r[5], "creator_name": r[6], "group_id": r[7], "group_name": None,
                "is_group_event": bool(r[8]), "is_accepted": int(r[9]), "is_creator": int(user_id) == int(r[5]),
                "is_hierarchical_event": bool(r[10])
            })
        return events

    @app.get("/events/invitations")
    def pending_event_invitations(user_id: int):
        cursor.execute("""
            SELECT e.id, e.title, e.description, e.start_time, e.end_time, e.creator_username, e.group_id, e.is_group_event
            FROM events e JOIN event_participants ep ON ep.event_id = e.id
            WHERE ep.user_id = ? AND ep.is_accepted = 0
        """, (user_id,))
        rows = cursor.fetchall()
        result = []
        for r in rows:
            result.append((r[0], r[1], r[2], r[3], r[4], r[5], None, bool(r[7]), r[6]))
        return result

    @app.get("/events/invitations/count")
    def pending_event_invitations_count(user_id: int):
        cursor.execute("SELECT COUNT(1) FROM event_participants WHERE user_id = ? AND is_accepted = 0", (user_id,))
        row = cursor.fetchone()
        return {"count": row[0] if row else 0}

    @app.post("/events/invitations/respond")
    async def respond_event_invitation(data: dict):
        if not raft.is_leader():
            return {"error": "No soy el líder", "leader": raft.leader_id}
        cmd = json.dumps({"type": "RESPOND_EVENT_INVITATION", "payload": data})
        entry = raft.append_log(cmd)
        replicated = await raft.replicate_log(entry)
        if not replicated:
            return {"error": "No se pudo replicar la respuesta"}
        await raft.apply_to_state_machine(entry)
        raft.last_applied = max(raft.last_applied, entry.index)
        raft.save_state()
        return {"status": "ok", "message": "Respuesta registrada"}

    @app.get("/events/{event_id}/details")
    def event_details(event_id: int, user_id: int):
        cursor.execute("""
            SELECT id, title, description, start_time, end_time, creator_id, creator_username, group_id, is_group_event
            FROM events WHERE id = ?
        """, (event_id,))
        ev = cursor.fetchone()
        if not ev:
            return {}
        cursor.execute("SELECT user_id, COALESCE(username, CAST(user_id AS TEXT)), is_accepted FROM event_participants WHERE event_id=?",
                       (event_id,))
        participants = [{"user_id": r[0], "username": r[1], "is_accepted": bool(r[2])} for r in cursor.fetchall()]
        return {
            "id": ev[0], "title": ev[1], "description": ev[2], "start_time": ev[3], "end_time": ev[4],
            "creator_id": ev[5], "creator_name": ev[6], "group_id": ev[7], "group_name": None,
            "is_group_event": bool(ev[8]), "participants": participants
        }

    @app.put("/events/{event_id}")
    async def update_event(event_id: int, update: dict):
        """Actualizar/replanificar un evento"""
        if not raft.is_leader():
            return {"error": "No soy el líder", "leader": raft.leader_id}

        # Verificar que el evento existe y obtener su creador
        cursor.execute("SELECT creator_id, start_time, end_time FROM events WHERE id=?", (event_id,))
        row = cursor.fetchone()
        if not row:
            return {"error": "Evento no encontrado"}

        creator_id = row[0]
        old_start = row[1]
        old_end = row[2]

        requester_id = update.get("requester_id")
        if not requester_id:
            return {"error": "requester_id requerido"}

        # Solo el creador puede editar
        if int(creator_id) != int(requester_id):
            return {"error": "Solo el creador puede modificar este evento"}

        # Preparar payload
        payload = {"event_id": event_id}
        time_changed = False

        if "title" in update:
            payload["title"] = update["title"]
        if "description" in update:
            payload["description"] = update["description"]
        if "start_time" in update:
            payload["start_time"] = update["start_time"]
            if update["start_time"] != old_start:
                time_changed = True
        if "end_time" in update:
            payload["end_time"] = update["end_time"]
            if update["end_time"] != old_end:
                time_changed = True

        payload["time_changed"] = time_changed

        cmd = json.dumps({"type": "UPDATE_EVENT", "payload": payload})
        entry = raft.append_log(cmd)
        replicated = await raft.replicate_log(entry)
        if not replicated:
            return {"error": "No se pudo replicar la actualización"}
        await raft.apply_to_state_machine(entry)
        raft.last_applied = max(raft.last_applied, entry.index)
        raft.save_state()
        return {"status": "ok", "message": "Evento actualizado exitosamente"}

    @app.get("/events/conflicts")
    def event_conflicts(user_id: int, limit: int = 50):
        cursor.execute("""
            SELECT ec.id, ec.event_id, e.title, e.start_time, e.end_time, ec.reason, ec.created_at
            FROM event_conflicts ec LEFT JOIN events e ON e.id = ec.event_id
            WHERE ec.user_id = ?
            ORDER BY ec.created_at DESC
            LIMIT ?
        """, (user_id, limit))
        rows = cursor.fetchall()
        return [(r[0], r[1], r[2], r[3], r[4], r[5], r[6]) for r in rows]


# ========== Endpoints RAFT comunes ==========
@app.get("/raft/state")
def state():
    return {
        "role": raft.role.value if hasattr(raft.role, 'value') else str(raft.role),
        "term": raft.current_term,
        "leader": raft.leader_id,
        "node_id": NODE_ID,
        "shard": SHARD_NAME
    }

@app.post("/raft/request_vote")
async def request_vote(req: Request):
    data = await req.json()
    return await raft.handle_vote_request(
        data["term"],
        data["candidate_id"],
        data.get("last_log_index", 0),
        data.get("last_log_term", 0)
    )

@app.post("/raft/append_entries")
async def append_entries(req: Request):
    data = await req.json()
    return await raft.receive_append_entries(
        data["term"],
        data["leader_id"],
        data.get("entries", []),
        data.get("prev_log_index", 0),
        data.get("prev_log_term", 0),
        data.get("leader_commit", 0)
    )

@app.post("/raft/heartbeat")
async def heartbeat(req: Request):
    data = await req.json()
    await raft.receive_heartbeat(data["term"], data["leader_id"])
    return {"status": "ok"}

@app.get("/raft/sync")
def sync_log(follower: str):
    return {"missing_entries": [e.to_dict() for e in raft.log]}

@app.get("/raft/log/summary")
def log_summary():
    last_index = len(raft.log)
    last_term = raft.log[-1].term if raft.log else 0
    return {
        "last_index": last_index,
        "last_term": last_term,
        "commit_index": raft.commit_index,
        "node_id": NODE_ID,
        "role": raft.role.value if hasattr(raft.role, 'value') else str(raft.role)
    }

@app.get("/raft/log/full")
def log_full():
    """Devuelve el log completo para reconciliación."""
    return {"entries": [e.to_dict() for e in raft.log], "commit_index": raft.commit_index, "term": raft.current_term}

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "node_id": NODE_ID,
        "role": raft.role.value if hasattr(raft.role, 'value') else str(raft.role),
        "shard": SHARD_NAME,
        "is_leader": raft.is_leader(),
        "term": raft.current_term
    }

@app.get("/")
def root():
    return {
        "message": f"Nodo RAFT {NODE_ID} - Shard {SHARD_NAME}",
        "role": raft.role.value if hasattr(raft.role, 'value') else str(raft.role),
        "is_leader": raft.is_leader(),
        "peers": PEERS
    }
