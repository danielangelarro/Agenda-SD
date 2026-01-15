from fastapi import FastAPI, Request
import sqlite3, os, asyncio
from shared.raft import RaftNode

SHARD_NAME = "GRUPOS"
NODE_ID = "node3"
PORT = 8803
PEERS = [
    "http://localhost:8801",
    "http://localhost:8802"
]

app = FastAPI(title=f"Shard {SHARD_NAME} - {NODE_ID}")

os.makedirs("data", exist_ok=True)
DB_PATH = os.path.join("data", "fsm.db")
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    description TEXT
)
""")
conn.commit()

raft = RaftNode(node_id=NODE_ID, peers=PEERS, state_file=f"data/{NODE_ID}_state.json")


@app.on_event("startup")
async def startup():
    asyncio.create_task(raft.start())

@app.post("/groups")
async def create_group(group: dict):
    if not raft.is_leader():
        return {"error": "No soy el líder", "leader": raft.leader_id}

    cursor.execute("INSERT INTO groups (name, description) VALUES (?, ?)", (group["name"], group["description"]))
    conn.commit()
    raft.append_log(f"CREATE_GROUP:{group['name']}")
    return {"status": "ok", "message": f"Grupo '{group['name']}' creado en {SHARD_NAME}"}

@app.get("/groups")
def list_groups():
    cursor.execute("SELECT id, name, description FROM groups")
    return [{"id": r[0], "name": r[1], "description": r[2]} for r in cursor.fetchall()]

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
