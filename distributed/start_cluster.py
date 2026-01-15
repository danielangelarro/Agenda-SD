import subprocess
import time
import sys
import os
import signal
import atexit

# Configuración de todos los nodos
NODES_CONFIG = [
    # Shard Eventos A-M (3 nodos)
    {"name": "node1", "shard": "events_a_m", "port": 8801, "file": "raft_node_events_am_1.py", 
     "peers": ["http://localhost:8802", "http://localhost:8803"]},
    {"name": "node2", "shard": "events_a_m", "port": 8802, "file": "raft_node_events_am_2.py", 
     "peers": ["http://localhost:8801", "http://localhost:8803"]},
    {"name": "node3", "shard": "events_a_m", "port": 8803, "file": "raft_node_events_am_3.py", 
     "peers": ["http://localhost:8801", "http://localhost:8802"]},
    
    # Shard Eventos N-Z (3 nodos)
    {"name": "node4", "shard": "events_n_z", "port": 8804, "file": "raft_node_events_nz_1.py", 
     "peers": ["http://localhost:8805", "http://localhost:8806"]},
    {"name": "node5", "shard": "events_n_z", "port": 8805, "file": "raft_node_events_nz_2.py", 
     "peers": ["http://localhost:8804", "http://localhost:8806"]},
    {"name": "node6", "shard": "events_n_z", "port": 8806, "file": "raft_node_events_nz_3.py", 
     "peers": ["http://localhost:8804", "http://localhost:8805"]},
    
    # Shard Grupos (3 nodos)
    {"name": "node7", "shard": "groups", "port": 8807, "file": "raft_node_groups_1.py", 
     "peers": ["http://localhost:8808", "http://localhost:8809"]},
    {"name": "node8", "shard": "groups", "port": 8808, "file": "raft_node_groups_2.py", 
     "peers": ["http://localhost:8807", "http://localhost:8809"]},
    {"name": "node9", "shard": "groups", "port": 8809, "file": "raft_node_groups_3.py", 
     "peers": ["http://localhost:8807", "http://localhost:8808"]},
    
    # Shard Usuarios (3 nodos)
    {"name": "node10", "shard": "users", "port": 8810, "file": "raft_node_users_1.py", 
     "peers": ["http://localhost:8811", "http://localhost:8812"]},
    {"name": "node11", "shard": "users", "port": 8811, "file": "raft_node_users_2.py", 
     "peers": ["http://localhost:8810", "http://localhost:8812"]},
    {"name": "node12", "shard": "users", "port": 8812, "file": "raft_node_users_3.py", 
     "peers": ["http://localhost:8810", "http://localhost:8811"]},
]

processes = []

def create_node_script(node_config):
    """Crea el archivo Python para un nodo específico"""
    template = '''from fastapi import FastAPI, Request
import sqlite3, os, asyncio
from shared.raft import RaftNode

SHARD_NAME = "{shard_name}"
NODE_ID = "{node_id}"
PORT = {port}
PEERS = {peers}

app = FastAPI(title=f"Shard {{SHARD_NAME}} - {{NODE_ID}}")

# Base de datos local
os.makedirs("data", exist_ok=True)
DB_PATH = os.path.join("data", "{db_name}")
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
cursor = conn.cursor()

# Esquema específico por shard
{schema}

conn.commit()

# Motor RAFT
raft = RaftNode(
    node_id=NODE_ID, 
    peers=PEERS, 
    state_file=f"data/{{NODE_ID}}_state.json",
    heartbeat_interval=1.0,
    election_timeout_range=(2.0, 4.0)
)

@app.on_event("startup")
async def startup():
    asyncio.create_task(raft.start())

{endpoints}

# Endpoints RAFT
@app.get("/raft/state")
def state():
    return {{
        "role": raft.role.value if hasattr(raft.role, 'value') else str(raft.role),
        "term": raft.current_term, 
        "leader": raft.leader_id,
        "node_id": NODE_ID,
        "shard": SHARD_NAME
    }}

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
    return {{"status": "ok"}}

@app.get("/raft/sync")
def sync_log(follower: str):
    return {{"missing_entries": [e.to_dict() for e in raft.log]}}

@app.get("/health")
async def health():
    return {{
        "status": "healthy",
        "node_id": NODE_ID,
        "role": raft.role.value if hasattr(raft.role, 'value') else str(raft.role),
        "shard": SHARD_NAME,
        "is_leader": raft.is_leader()
    }}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
'''

    # Configurar esquema y endpoints según el shard
    if "events" in node_config["shard"]:
        schema = '''cursor.execute("""
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    description TEXT,
    creator TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_time TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")'''
        endpoints = '''
@app.post("/events")
async def create_event(event: dict):
    if not raft.is_leader():
        return {"error": "No soy el líder", "leader": raft.leader_id}

    entry = raft.append_log(f"CREATE_EVENT:{event['title']}")
    replicated = await raft.replicate_log(entry)

    if not replicated:
        return {"error": "No se pudo replicar el evento"}

    cursor.execute("""
        INSERT INTO events (title, description, creator, start_time, end_time)
        VALUES (?, ?, ?, ?, ?)
    """, (event["title"], event["description"], event["creator"], 
          event["start_time"], event["end_time"]))
    conn.commit()

    return {
        "status": "ok", 
        "message": f"Evento creado en {SHARD_NAME}",
        "node": NODE_ID
    }

@app.get("/events")
def list_events():
    cursor.execute("SELECT id, title, creator, start_time, end_time FROM events")
    return [{
        "id": r[0], "title": r[1], "creator": r[2], 
        "start_time": r[3], "end_time": r[4]
    } for r in cursor.fetchall()]'''
    
    elif "groups" in node_config["shard"]:
        schema = '''cursor.execute("""
CREATE TABLE IF NOT EXISTS groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    creator TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")'''
        endpoints = '''
@app.post("/groups")
async def create_group(group: dict):
    if not raft.is_leader():
        return {"error": "No soy el líder", "leader": raft.leader_id}

    entry = raft.append_log(f"CREATE_GROUP:{group['name']}")
    replicated = await raft.replicate_log(entry)

    if not replicated:
        return {"error": "No se pudo replicar el grupo"}

    cursor.execute(
        "INSERT INTO groups (name, description, creator) VALUES (?, ?, ?)", 
        (group["name"], group["description"], group.get("creator", "system"))
    )
    conn.commit()

    return {
        "status": "ok", 
        "message": f"Grupo creado en {SHARD_NAME}",
        "node": NODE_ID
    }

@app.get("/groups")
def list_groups():
    cursor.execute("SELECT id, name, description, creator FROM groups")
    return [{
        "id": r[0], "name": r[1], "description": r[2], "creator": r[3]
    } for r in cursor.fetchall()]'''
    
    else:  # users
        schema = '''cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")'''
        endpoints = '''
@app.post("/users")
async def create_user(user: dict):
    if not raft.is_leader():
        return {"error": "No soy el líder", "leader": raft.leader_id}

    entry = raft.append_log(f"CREATE_USER:{user['username']}")
    replicated = await raft.replicate_log(entry)

    if not replicated:
        return {"error": "No se pudo replicar el usuario"}

    try:
        cursor.execute(
            "INSERT INTO users (username, email) VALUES (?, ?)", 
            (user["username"], user["email"])
        )
        conn.commit()
    except sqlite3.IntegrityError:
        return {"error": "Usuario o email ya existen"}

    return {
        "status": "ok", 
        "message": f"Usuario creado en {SHARD_NAME}",
        "node": NODE_ID
    }

@app.get("/users")
def list_users():
    cursor.execute("SELECT id, username, email FROM users")
    return [{
        "id": r[0], "username": r[1], "email": r[2]
    } for r in cursor.fetchall()]'''

    # Renderizar template
    script_content = template.format(
        shard_name=node_config["shard"],
        node_id=node_config["name"],
        port=node_config["port"],
        peers=node_config["peers"],
        db_name=f"{node_config['shard']}_{node_config['name']}.db",
        schema=schema,
        endpoints=endpoints
    )

    # Guardar archivo
    with open(node_config["file"], "w") as f:
        f.write(script_content)

def start_node(node_config):
    """Inicia un nodo individual"""
    try:
        # Crear el script del nodo
        create_node_script(node_config)
        
        # Ejecutar el nodo
        cmd = [sys.executable, node_config["file"]]
        process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        processes.append(process)
        print(f"✅ Iniciado {node_config['name']} en puerto {node_config['port']} (PID: {process.pid})")
        
        # Log en tiempo real
        def log_output(pipe, prefix):
            for line in pipe:
                print(f"{prefix} {line.strip()}")
        
        # asyncio.create_task(log_output(process.stdout, f"[{node_config['name']}]"))
        # asyncio.create_task(log_output(process.stderr, f"[{node_config['name']} ERROR]"))
        
        return True
    except Exception as e:
        print(f"❌ Error iniciando {node_config['name']}: {e}")
        return False

def start_coordinator():
    """Inicia el coordinador"""
    try:
        cmd = [sys.executable, "router.py"]
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        processes.append(process)
        print(f"✅ Coordinador iniciado en puerto 8000 (PID: {process.pid})")
        return True
    except Exception as e:
        print(f"❌ Error iniciando coordinador: {e}")
        return False

def stop_cluster():
    """Detiene todos los procesos"""
    print("\n🛑 Deteniendo cluster...")
    for process in processes:
        try:
            process.terminate()
            process.wait(timeout=5)
        except:
            try:
                process.kill()
            except:
                pass
    print("✅ Todos los procesos detenidos")

def signal_handler(sig, frame):
    """Maneja señales de terminación"""
    stop_cluster()
    sys.exit(0)

def main():
    # Registrar manejadores de señales
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    atexit.register(stop_cluster)

    print("🚀 Iniciando Cluster RAFT con Tolerancia Completa...")
    print(f"📊 Total de nodos: {len(NODES_CONFIG)}")
    print(f"🔢 Shards: 4 (Eventos A-M, Eventos N-Z, Grupos, Usuarios)")
    print(f"🛡️  Tolerancia: 1 fallo por shard (3 nodos por shard)")
    print("-" * 50)
    
    # Crear directorio shared si no existe
    os.makedirs("shared", exist_ok=True)
    
    # Iniciar nodos RAFT
    successful_nodes = 0
    for node in NODES_CONFIG:
        if start_node(node):
            successful_nodes += 1
        time.sleep(1)  # Delay entre inicios
    
    print("-" * 50)
    print(f"📈 Nodos iniciados: {successful_nodes}/{len(NODES_CONFIG)}")
    
    # Esperar a que los nodos se estabilicen
    print("⏳ Esperando estabilización del cluster (8 segundos)...")
    time.sleep(8)
    
    # Iniciar coordinador
    print("-" * 50)
    if start_coordinator():
        print("🎉 Cluster iniciado exitosamente!")
        print("📍 Coordinador: http://localhost:8000")
        print("📋 Endpoints disponibles:")
        print("   POST /events     - Crear evento")
        print("   POST /groups     - Crear grupo") 
        print("   POST /users      - Crear usuario")
        print("   GET  /leaders    - Ver líderes actuales")
        print("   GET  /health     - Estado del coordinador")
        print("\n⏹️  Presiona Ctrl+C para detener el cluster")
    else:
        print("❌ Error iniciando coordinador")
        stop_cluster()
        return
    
    # Mantener el script corriendo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo cluster...")
        stop_cluster()

if __name__ == "__main__":
    main()