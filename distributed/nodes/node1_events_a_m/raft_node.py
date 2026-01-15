from fastapi import FastAPI, Request
import sqlite3, os, asyncio
from shared.raft import RaftNode

SHARD_NAME = "EVENTOS A-M"
NODE_ID = "node1"
PORT = 8801
PEERS = [
    "http://localhost:8802",
    "http://localhost:8803"
]

app = FastAPI(title=f"Shard {SHARD_NAME} - {NODE_ID}")

# === Base de datos local ===
os.makedirs("data", exist_ok=True)
DB_PATH = os.path.join("data", "fsm.db")
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    description TEXT,
    creator TEXT,
    start_time TEXT,
    end_time TEXT
)
""")
conn.commit()

# === Motor RAFT ===
raft = RaftNode(node_id=NODE_ID, peers=PEERS, state_file=f"data/{NODE_ID}_state.json")

@app.on_event("startup")
async def startup():
    asyncio.create_task(raft.start())

# === Endpoints de eventos (solo líder escribe) ===
@app.post("/events")
async def create_event(event: dict):
    if not raft.is_leader():
        return {"error": "No soy el líder", "leader": raft.leader_id}

    # Crear entrada de log
    entry = raft.append_log(f"CREATE_EVENT:{event['title']}")

    # Replicar a seguidores
    replicated = await raft.replicate_log(entry)

    if not replicated:
        return {"error": "No se pudo replicar el evento en la mayoría de nodos"}

    # Aplicar a la FSM (base SQLite del líder)
    cursor.execute("""
        INSERT INTO events (title, description, creator, start_time, end_time)
        VALUES (?, ?, ?, ?, ?)
    """, (event["title"], event["description"], event["creator"], event["start_time"], event["end_time"]))
    conn.commit()

    return {"status": "ok", "message": f"Evento '{event['title']}' replicado y guardado en {SHARD_NAME}"}

@app.get("/events")
def list_events():
    cursor.execute("SELECT id, title, creator, start_time FROM events")
    rows = cursor.fetchall()
    return [{"id": r[0], "title": r[1], "creator": r[2], "start_time": r[3]} for r in rows]

# === Endpoints RAFT ===
@app.get("/raft/state")
def state():
    return {"role": raft.role, "term": raft.current_term, "leader": raft.leader_id}

@app.post("/raft/request_vote")
async def request_vote(req: Request):
    data = await req.json()
    return await raft.handle_vote_request(data["term"], data["candidate_id"])

@app.post("/raft/heartbeat")
async def heartbeat(req: Request):
    data = await req.json()
    await raft.receive_heartbeat(data["term"], data["leader_id"])
    return {"status": "ok"}

@app.post("/raft/append_entries")
async def append_entries(req: Request):
    data = await req.json()
    return await raft.receive_append_entries(data["term"], data["leader_id"], data["entry"])

@app.get("/raft/sync")
def sync_log(follower: str):
    """Devuelve entradas del log al seguidor que se reconecta"""
    return {"missing_entries": [e.to_dict() for e in raft.log]}
