import asyncio
import aiohttp
import json
import os
import random
import time
from enum import Enum
from typing import List, Optional, Dict, Any
import logging
from urllib.parse import urlparse

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("raft")

class RaftRole(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

class LogEntry:
    """Una entrada en el log de RAFT"""
    def __init__(self, term: int, command: str, index: int = None):
        self.term = term
        self.command = command
        self.index = index

    def to_dict(self):
        return {"term": self.term, "command": self.command, "index": self.index}

    @staticmethod
    def from_dict(d):
        return LogEntry(term=d["term"], command=d["command"], index=d.get("index"))

class RaftNode:
    """
    Nodo RAFT con consenso completo y tolerancia a fallos
    """

    def __init__(self, node_id: str, peers: List[str], state_file: str, 
                 heartbeat_interval: float = 1.0, election_timeout_range: tuple = (2.0, 4.0),
                 state_machine_callback=None, self_url: Optional[str] = None,
                 replication_factor: Optional[int] = None):
        self.node_id = node_id
        self.peers = peers
        self.state_file = state_file
        self.state_machine_callback = state_machine_callback
        self.self_url = self_url or node_id
        
        # Estado RAFT persistente
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        
        # Estado RAFT volátil
        self.commit_index = 0
        self.last_applied = 0
        self.role = RaftRole.FOLLOWER
        self.leader_id: Optional[str] = None
        self.votes_received = set()
        
        # Para líderes
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        self.peer_health: Dict[str, float] = {}
        self.peer_health_window = 5.0  # segundos para considerar un peer "vivo"
        self.replication_factor = max(1, min(len(peers) + 1, replication_factor or len(peers) + 1))
        self._set_peers(peers, persist=False)

        # Prioridad para Bully (derivada del URL/ID numérico)
        self.priority = self._priority_of_url(self.self_url)
        self._last_preempt_attempt = 0.0
        self._last_preempt_attempt = 0.0
        
        # Configuración de timeouts
        self.heartbeat_interval = heartbeat_interval
        self.election_timeout_range = election_timeout_range
        self.election_timeout = self._random_election_timeout()
        self.last_heartbeat_time = time.time()
        
        # Bloqueo para operaciones concurrentes
        self._lock = asyncio.Lock()
        
        # Cargar estado persistente
        self.load_state()
        
        # Inicializar índices de replicación si es líder
        if self.role == RaftRole.LEADER:
            self._init_leader_state()

    def _random_election_timeout(self) -> float:
        min_timeout, max_timeout = self.election_timeout_range
        return random.uniform(min_timeout, max_timeout)

    def _init_leader_state(self):
        """Inicializa el estado específico del líder"""
        for peer in self.peers:
            self.next_index[peer] = len(self.log) + 1
            self.match_index[peer] = 0

    # ====================================================
    # Persistencia
    # ====================================================

    def save_state(self):
        """Guarda el estado persistente en disco"""
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        state = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": [e.to_dict() for e in self.log],
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "peers": self.peers,
            "replication_factor": self.replication_factor,
        }
        try:
            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.error(f"Error guardando estado: {e}")

    def load_state(self):
        """Carga el estado persistente desde disco"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r") as f:
                    state = json.load(f)
                self.current_term = state.get("current_term", 0)
                self.voted_for = state.get("voted_for")
                
                # Reconstruir log con índices
                log_data = state.get("log", [])
                self.log = []
                for i, entry_data in enumerate(log_data):
                    entry = LogEntry.from_dict(entry_data)
                    entry.index = i + 1
                    self.log.append(entry)
                
                self.commit_index = state.get("commit_index", 0)
                self.last_applied = state.get("last_applied", 0)
                loaded_peers = state.get("peers")
                if loaded_peers:
                    self._set_peers(loaded_peers, persist=False)
                rep = state.get("replication_factor")
                if rep:
                    self.replication_factor = max(1, rep)
                logger.info(f"✅ Estado cargado: término {self.current_term}, {len(self.log)} entradas")
            except Exception as e:
                logger.error(f"Error cargando estado: {e}")

    # ====================================================
    # Estado interno
    # ====================================================

    def is_leader(self) -> bool:
        return self.role == RaftRole.LEADER

    def _healthy_peers(self) -> List[str]:
        """Peers con los que tuvimos contacto reciente."""
        now = time.time()
        return [p for p, ts in self.peer_health.items() if ts and (now - ts) <= self.peer_health_window]

    def _target_peers(self) -> List[str]:
        """Selecciona el subconjunto al que se replicará activamente."""
        healthy = self._healthy_peers()
        unhealthy = [p for p in self.peers if p not in healthy]
        ordered = healthy + unhealthy
        return ordered[: max(0, self.replication_factor - 1)]

    def _priority_of_url(self, url: str) -> int:
        """Deriva prioridad solo desde el puerto del URL; fallback a dígitos si no hay puerto."""
        if not url:
            return 0
        try:
            parsed = urlparse(url if "://" in url else f"http://{url}")
            if parsed.port:
                return int(parsed.port)
            host_digits = "".join([c for c in (parsed.hostname or "") if c.isdigit()])
            if host_digits:
                return int(host_digits)
        except Exception:
            pass
        digits = "".join([c for c in url if c.isdigit()])
        return int(digits) if digits else 0

    def _higher_priority_peers(self) -> List[str]:
        """Peers con prioridad mayor que la nuestra (Bully)."""
        my_prio = self.priority
        return [p for p in self.peers if self._priority_of_url(p) > my_prio]

    def _is_highest_priority(self) -> bool:
        """True si somos el mayor (o empatados) en prioridad conocida."""
        all_prios = [self.priority] + [self._priority_of_url(p) for p in self.peers]
        return self.priority >= max(all_prios) if all_prios else True

    async def _maybe_preempt_as_highest(self):
        """Si somos el de mayor prioridad conocido, arranca elección para liderar."""
        await asyncio.sleep(0)  # cede control para permitir setup de loops
        if not self.is_leader() and self._is_highest_priority():
            now = time.time()
            if now - self._last_preempt_attempt > (self.election_timeout / 2):
                self._last_preempt_attempt = now
                await self._start_bully_election()

    async def _maybe_challenge_lower_priority_leader(self, leader_id: str):
        """Si el líder visto tiene menor prioridad que nosotros, forzamos elección."""
        if not leader_id:
            return
        leader_prio = self._priority_of_url(leader_id)
        if self.priority > leader_prio:
            now = time.time()
            if now - self._last_preempt_attempt > (self.election_timeout / 2):
                self._last_preempt_attempt = now
                await self._start_bully_election()

    def _set_peers(self, peers: List[str], persist: bool = True):
        """Actualiza peers en memoria (y opcionalmente persiste)."""
        filtered = [p for p in peers if p and p != self.self_url]
        self.peers = filtered
        # Mantener salud previa si existe
        self.peer_health = {p: self.peer_health.get(p, 0.0) for p in self.peers}
        # Ajustar factor de replicación sin exceder cluster
        self.replication_factor = max(1, min(len(self.peers) + 1, self.replication_factor or len(self.peers) + 1))
        # Reconfigurar estructuras de líder
        if self.is_leader():
            self.next_index = {p: len(self.log) + 1 for p in self.peers}
            self.match_index = {p: 0 for p in self.peers}
        if persist:
            self.save_state()

    async def update_peers(self, peers: List[str], replication_factor: Optional[int] = None):
        """Actualiza peers de forma dinámica con lock."""
        async with self._lock:
            if replication_factor:
                self.replication_factor = max(1, replication_factor)
            self._set_peers(peers, persist=True)

    def _quorum_size(self) -> int:
        """Quórum dinámico basado en peers activos.

        - Si solo hay 1 nodo vivo, quórum=1 para seguir operando (sin garantías fuertes).
        - A medida que se suman peers vivos, el quórum crece.
        """
        cluster_size = 1 + len(self._healthy_peers())  # self + peers alcanzables
        return max(1, (cluster_size // 2) + 1)

    def reset_election_timer(self):
        """Reinicia el temporizador de elección"""
        self.last_heartbeat_time = time.time()
        self.election_timeout = self._random_election_timeout()

    # ====================================================
    # Ciclo principal
    # ====================================================

    async def start(self):
        """Inicia las tareas del nodo RAFT"""
        logger.info(f"🚀 Iniciando nodo {self.node_id} como {self.role}")
        asyncio.create_task(self._election_loop())
        asyncio.create_task(self._heartbeat_loop())
        asyncio.create_task(self._apply_committed_entries())
        asyncio.create_task(self._consistency_loop())
        # Si somos el de mayor prioridad conocido, forzamos elección al arrancar para liderar.
        asyncio.create_task(self._maybe_preempt_as_highest())

    async def _election_loop(self):
        """Maneja las elecciones de líder"""
        while True:
            await asyncio.sleep(0.1)
            
            if self.role == RaftRole.LEADER:
                continue

            # Verificar timeout de elección
            if time.time() - self.last_heartbeat_time > self.election_timeout:
                await self._start_bully_election()

    async def _heartbeat_loop(self):
        """Loop para enviar heartbeats (solo líderes)"""
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            if self.role == RaftRole.LEADER:
                await self._broadcast_heartbeat()

    async def _consistency_loop(self):
        """Verifica salud de peers y cura réplicas rezagadas."""
        while True:
            await asyncio.sleep(5)
            if not self.is_leader():
                continue
            # Empuja estado a peers vivos y detecta rezagos
            for peer in self.peers:
                await self._sync_peer_state(peer)
                # Además, si el peer tiene entradas que nosotros no, intente reconciliarlas
                await self._reconcile_from_peer(peer)

    async def _apply_committed_entries(self):
        """Aplica las entradas comprometidas a la máquina de estado"""
        while True:
            await asyncio.sleep(0.2)
            await self._drain_committed_entries()

    # ====================================================
    # Elecciones de líder
    # ====================================================

    async def _start_election(self):
        """Compat: elección RAFT, ya no usada (Bully)."""
        return

    async def _start_bully_election(self):
        """Elección de líder usando Bully."""
        async with self._lock:
            self.role = RaftRole.CANDIDATE
            self.leader_id = None
            self.current_term += 1
            self.reset_election_timer()
            self.save_state()

        higher_peers = self._higher_priority_peers()
        if not higher_peers:
            # Somos el de mayor prioridad, nos coronamos
            await self._become_leader_bully()
            return

        logger.info(f"🗳️ {self.node_id} inicia Bully contra {len(higher_peers)} peers mayores")
        any_alive = False
        for peer in higher_peers:
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3)) as session:
                    payload = {"candidate_id": self.node_id, "candidate_url": self.self_url, "priority": self.priority}
                    async with session.post(f"{peer}/raft/bully/challenge", json=payload) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("alive"):
                                any_alive = True
                                self.peer_health[peer] = time.time()
                                logger.info(f"⚔️ {self.node_id} encontró peer mayor vivo: {peer}")
            except Exception as e:
                logger.warning(f"Challenge a {peer} falló: {e}")
                self.peer_health[peer] = 0.0

        if any_alive:
            # Esperamos un anuncio de victoria; si no llega, volvemos a intentar
            await asyncio.sleep(self.election_timeout)
            if self.leader_id:
                logger.info(f"🙌 {self.node_id} reconoce líder {self.leader_id}")
                return
            # Nadie se proclamó, nos coronamos
        await self._become_leader_bully()

    async def _become_leader_bully(self):
        async with self._lock:
            self.role = RaftRole.LEADER
            self.leader_id = self.self_url or self.node_id
            self._init_leader_state()
            self.reset_election_timer()
            self.save_state()
        logger.info(f"👑 {self.node_id} (prio {self.priority}) se proclama líder (Bully)")
        # Antes de aceptar clientes, intenta recuperar el log más avanzado de los peers
        await self._recover_from_peers()
        self._init_leader_state()  # re-inicializa índices tras posible cambio de log
        await self._announce_victory()
        await self._broadcast_heartbeat()

    async def _request_vote(self, peer: str) -> bool:
        """Solicita voto a un peer específico"""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2)) as session:
                data = {
                    "term": self.current_term,
                    "candidate_id": self.node_id,
                    "last_log_index": len(self.log),
                    "last_log_term": self.log[-1].term if self.log else 0
                }
                async with session.post(f"{peer}/raft/request_vote", json=data) as resp:
                    result = await resp.json()
                    self.peer_health[peer] = time.time()
                    if result.get("vote_granted", False):
                        self.votes_received.add(peer)
                        return True
        except Exception as e:
            logger.warning(f"Error solicitando voto a {peer}: {e}")
            if peer in self.peer_health:
                self.peer_health[peer] = 0.0
        return False

    # ====================================================
    # Heartbeats y replicación
    # ====================================================

    async def _broadcast_heartbeat(self):
        """Envía heartbeats a todos los seguidores"""
        if not self.is_leader():
            return

        peers = self._target_peers()
        tasks = [asyncio.create_task(self._send_append_entries(peer)) for peer in peers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _announce_victory(self):
        """Difunde que somos líder (Bully)."""
        for peer in self.peers:
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3)) as session:
                    payload = {"leader_id": self.node_id, "leader_url": self.self_url, "priority": self.priority}
                    async with session.post(f"{peer}/raft/bully/victory", json=payload) as resp:
                        if resp.status == 200:
                            self.peer_health[peer] = time.time()
            except Exception as e:
                logger.warning(f"No pude anunciar victoria a {peer}: {e}")
                self.peer_health[peer] = 0.0

    async def _send_append_entries(self, peer: str):
        """Envía AppendEntries RPC a un seguidor"""
        try:
            next_idx = self.next_index.get(peer, 1)
            prev_log_index = next_idx - 1
            prev_log_term = 0
            
            if prev_log_index > 0 and prev_log_index <= len(self.log):
                prev_log_term = self.log[prev_log_index - 1].term
            
            entries = []
            if next_idx <= len(self.log):
                entries = [entry.to_dict() for entry in self.log[next_idx - 1:]]

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3)) as session:
                data = {
                    "term": self.current_term,
                    "leader_id": self.self_url or self.node_id,
                    "prev_log_index": prev_log_index,
                    "prev_log_term": prev_log_term,
                    "entries": entries,
                    "leader_commit": self.commit_index
                }
                async with session.post(f"{peer}/raft/append_entries", json=data) as resp:
                    result = await resp.json()
                    
                    if result.get("success", False):
                        # Actualizar índices de replicación
                        self.next_index[peer] = len(self.log) + 1
                        self.match_index[peer] = len(self.log)
                        self.peer_health[peer] = time.time()

                        # Verificar si podemos comprometer nuevas entradas
                        await self._update_commit_index()
                    else:
                        # Retroceder next_index
                        if self.next_index[peer] > 1:
                            self.next_index[peer] -= 1
                        # Respondió pero sin éxito: sigue estando vivo
                        self.peer_health[peer] = time.time()
        except Exception as e:
            logger.warning(f"Error enviando AppendEntries a {peer}: {e}")
            if peer in self.peer_health:
                self.peer_health[peer] = 0.0

    async def _update_commit_index(self):
        """Actualiza el commit_index basado en las réplicas"""
        if not self.is_leader():
            return

        # Encontrar el índice más alto que está replicado en la mayoría
        for n in range(len(self.log), self.commit_index, -1):
            count = 1  # El líder cuenta
            for peer in self.peers:
                if self.match_index.get(peer, 0) >= n:
                    count += 1
            
            if count >= self._quorum_size():
                if n > self.commit_index and self.log[n-1].term == self.current_term:
                    self.commit_index = n
                    self.save_state()
                break

    # ====================================================
    # API para aplicaciones
    # ====================================================

    def append_log(self, command: str) -> LogEntry:
        """Agrega una nueva entrada al log (solo líder)"""
        if not self.is_leader():
            raise Exception("Solo el líder puede agregar entradas al log")
        
        entry = LogEntry(self.current_term, command, index=len(self.log) + 1)
        self.log.append(entry)
        self.save_state()
        return entry

    async def replicate_log(self, entry: LogEntry) -> bool:
        """Replica una entrada a la mayoría de nodos"""
        if not self.is_leader():
            return False

        # La entrada ya está en el log del líder, ahora replicarla
        success_count = 1  # El líder cuenta
        
        target_peers = self._target_peers()
        tasks = []
        for peer in target_peers:
            task = asyncio.create_task(self._replicate_entry_to_peer(peer, entry))
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count += sum(1 for r in results if r is True)

        # Verificar mayoría (degradando si hay menos nodos vivos)
        majority_required = self._quorum_size()
        
        has_majority = success_count >= majority_required
        
        if has_majority:
            # Actualizar commit_index si es mayor que el actual
            entry_index = entry.index
            if entry_index > self.commit_index:
                self.commit_index = entry_index
                self.save_state()
            logger.info(f"✅ Entrada {entry_index} replicada en mayoría")
            # Empujar commit_index actualizado inmediatamente a seguidores
            await self._broadcast_heartbeat()
        else:
            logger.warning(f"⚠️ Entrada {entry.index} no alcanzó mayoría ({success_count}/{majority_required})")

        return has_majority

    async def _replicate_entry_to_peer(self, peer: str, entry: LogEntry) -> bool:
        """Replica una entrada específica a un peer"""
        try:
            # Usamos next_index para enviar todas las entradas faltantes en el peer
            next_idx = self.next_index.get(peer, len(self.log) + 1)
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                while next_idx > 0:
                    prev_log_index = next_idx - 1
                    prev_log_term = self.log[prev_log_index - 1].term if 0 < prev_log_index <= len(self.log) else 0
                    entries = [e.to_dict() for e in self.log[next_idx - 1:]]
                    data = {
                        "term": self.current_term,
                        "leader_id": self.node_id,
                        "prev_log_index": prev_log_index,
                        "prev_log_term": prev_log_term,
                        "entries": entries,
                        "leader_commit": self.commit_index
                    }
                    async with session.post(f"{peer}/raft/append_entries", json=data) as resp:
                        result = await resp.json()
                        if result.get("success", False):
                            self.next_index[peer] = len(self.log) + 1
                            self.match_index[peer] = len(self.log)
                            self.peer_health[peer] = time.time()
                            return True
                        # Respondió pero no aceptó: sigue vivo, decrementamos y reintentamos
                        self.peer_health[peer] = time.time()
                    # Si falla, retroceder next_idx para buscar punto común
                    if next_idx <= 1:
                        break
                    next_idx -= 1
        except Exception as e:
            logger.warning(f"Error replicando a {peer}: {e}")
            if peer in self.peer_health:
                self.peer_health[peer] = 0.0
            return False

    async def _sync_peer_state(self, peer: str):
        """Empuja las entradas faltantes a un peer para curar rezagos."""
        if not self.is_leader() or not self.log:
            return
        try:
            # Intenta replicar desde el último índice conocido del peer
            await self._replicate_entry_to_peer(peer, self.log[-1])
        except Exception as e:
            logger.warning(f"Error curando peer {peer}: {e}")

    async def _reconcile_from_peer(self, peer: str):
        """Trae entradas que el peer tenga y el líder no, y las replica al resto.

        Política simple: se consideran nuevas las entradas cuyo par (term, command)
        no exista en nuestro log. Se incorporan en el líder y luego se replican
        al resto. Esto permite que un nodo aislado aporte escrituras al reunirse.
        """
        if not self.is_leader():
            return
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                async with session.get(f"{peer}/raft/log/full") as resp:
                    if resp.status != 200:
                        return
                    data = await resp.json()
                    peer_entries = data.get("entries", [])
        except Exception as e:
            logger.warning(f"Error obteniendo log completo de {peer}: {e}")
            return

        if not peer_entries:
            return

        existing_keys = {(e.term, e.command) for e in self.log}
        new_entries = []
        for entry_data in peer_entries:
            key = (entry_data.get("term"), entry_data.get("command"))
            if key in existing_keys:
                continue
            entry = LogEntry.from_dict(entry_data)
            new_entries.append(entry)
            existing_keys.add(key)

        if not new_entries:
            return

        logger.info(f"🔄 Reconciliando {len(new_entries)} entradas nuevas desde {peer}")
        for entry in new_entries:
            # Incorporar entrada al líder
            entry.index = len(self.log) + 1
            self.log.append(entry)
            self.commit_index = max(self.commit_index, entry.index)
            self.last_applied = self.commit_index
            self.save_state()
            # Aplicar al estado local
            await self.apply_to_state_machine(entry)
            # Replicar al resto
            await self.replicate_log(entry)

    async def _recover_from_peers(self):
        """Cuando nos volvemos líder, buscamos el log más avanzado en los peers y lo adoptamos."""
        best_log = None
        best_summary = {
            "last_index": len(self.log),
            "last_term": self.log[-1].term if self.log else 0,
            "commit_index": self.commit_index,
        }
        for peer in self.peers:
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                    async with session.get(f"{peer}/raft/log/summary") as resp:
                        if resp.status != 200:
                            continue
                        summary = await resp.json()
                        peer_last = summary.get("last_index", 0)
                        peer_term = summary.get("last_term", 0)
                        peer_commit = summary.get("commit_index", 0)
                        # Escogemos el log más avanzado (mayor índice, o mayor término a mismo índice)
                        better = False
                        if peer_last > best_summary["last_index"]:
                            better = True
                        elif peer_last == best_summary["last_index"] and peer_term > best_summary["last_term"]:
                            better = True
                        if not better:
                            continue
                        async with session.get(f"{peer}/raft/sync?follower={self.node_id}") as sync_resp:
                            if sync_resp.status != 200:
                                continue
                            data = await sync_resp.json()
                            entries = data.get("missing_entries", [])
                            candidate_log = []
                            for i, entry_data in enumerate(entries):
                                entry = LogEntry.from_dict(entry_data)
                                entry.index = i + 1
                                candidate_log.append(entry)
                            if candidate_log:
                                best_log = candidate_log
                                best_summary = {
                                    "last_index": peer_last,
                                    "last_term": peer_term,
                                    "commit_index": min(peer_commit, len(candidate_log)),
                                }
                        self.peer_health[peer] = time.time()
            except Exception as e:
                logger.warning(f"Error recuperando log de {peer}: {e}")
                self.peer_health[peer] = 0.0

        if best_log:
            async with self._lock:
                self.log = best_log
                self.commit_index = best_summary.get("commit_index", len(best_log))
                self.last_applied = min(self.commit_index, len(self.log))
                self.save_state()
            await self._drain_committed_entries()

    # ====================================================
    # Handlers para requests RAFT
    # ====================================================

    async def handle_vote_request(self, term: int, candidate_id: str, 
                                last_log_index: int, last_log_term: int) -> dict:
        """Maneja RequestVote RPC"""
        async with self._lock:
            # Actualizar término si es necesario
            if term > self.current_term:
                self.current_term = term
                self.role = RaftRole.FOLLOWER
                self.voted_for = None
                self.save_state()

            vote_granted = False
            
            # Verificar condiciones para otorgar voto
            if (term == self.current_term and 
                (self.voted_for is None or self.voted_for == candidate_id)):
                
                # Verificar que el log del candidato está al menos tan actualizado como el nuestro
                our_last_log_term = self.log[-1].term if self.log else 0
                our_last_log_index = len(self.log)
                
                if (last_log_term > our_last_log_term or 
                    (last_log_term == our_last_log_term and last_log_index >= our_last_log_index)):
                    
                    self.voted_for = candidate_id
                    vote_granted = True
                    self.reset_election_timer()
                    self.save_state()

            return {
                "term": self.current_term,
                "vote_granted": vote_granted,
                "node_id": self.node_id
            }

    async def receive_append_entries(self, term: int, leader_id: str, 
                                   entries: List[dict], prev_log_index: int, 
                                   prev_log_term: int, leader_commit: int) -> dict:
        """Maneja AppendEntries RPC"""
        apply_now = False
        async with self._lock:
            # Actualizar término si es necesario
            if term > self.current_term:
                self.current_term = term
                self.role = RaftRole.FOLLOWER
                self.voted_for = None

            success = False

            if term < self.current_term:
                return {"term": self.current_term, "success": False}

            # Resetear temporizador de elección
            self.reset_election_timer()
            self.role = RaftRole.FOLLOWER
            # Preferimos la URL si viene (para respetar prioridad por puerto)
            self.leader_id = leader_id or self.leader_id
            # Si tenemos más prioridad que el líder actual, gatilla reelección.
            asyncio.create_task(self._maybe_challenge_lower_priority_leader(leader_id))

            # Verificar consistencia del log
            if prev_log_index > 0:
                if prev_log_index > len(self.log) or \
                   (prev_log_index <= len(self.log) and self.log[prev_log_index - 1].term != prev_log_term):
                    return {"term": self.current_term, "success": False}

            # Aplicar entradas
            if entries:
                # Eliminar entradas conflictivas
                if prev_log_index < len(self.log):
                    self.log = self.log[:prev_log_index]
                
                # Agregar nuevas entradas
                for entry_data in entries:
                    entry = LogEntry.from_dict(entry_data)
                    entry.index = len(self.log) + 1
                    self.log.append(entry)

            # Actualizar commit_index
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log))
            if self.last_applied < self.commit_index:
                apply_now = True

            success = True
            self.save_state()

            response = {
                "term": self.current_term,
                "success": success,
                "node_id": self.node_id
            }

        if apply_now:
            # Aplica inmediatamente los commits recién anunciados para evitar lags tras failover
            await self._drain_committed_entries()

        return response

    async def receive_heartbeat(self, term: int, leader_id: str):
        """Maneja heartbeat simple"""
        async with self._lock:
            if term >= self.current_term:
                self.current_term = term
                self.role = RaftRole.FOLLOWER
                self.leader_id = leader_id or self.leader_id
                self.reset_election_timer()
                self.save_state()
                # Si vemos heartbeats de un líder con menor prioridad, forzamos elección.
                asyncio.create_task(self._maybe_challenge_lower_priority_leader(leader_id))

    # ====================================================
    # Bully handlers
    # ====================================================

    async def handle_bully_challenge(self, candidate_id: str, candidate_url: str, candidate_priority: int) -> dict:
        """Responder a un challenge Bully. Si tenemos mayor prioridad, lanzamos nuestra elección."""
        my_prio = self.priority
        if my_prio > candidate_priority:
            # Tengo más prioridad, inicio elección (o reafirmo liderazgo)
            asyncio.create_task(self._start_bully_election())
        return {"alive": True, "priority": my_prio, "leader": self.leader_id}

    async def handle_bully_victory(self, leader_id: str, leader_url: str, priority: int) -> dict:
        """Aceptar victoria de otro nodo."""
        async with self._lock:
            self.role = RaftRole.FOLLOWER
            self.leader_id = leader_url or leader_id
            self.current_term = max(self.current_term, priority)
            self.reset_election_timer()
            self.save_state()
        return {"status": "ok", "ack": True}

    # ====================================================
    # Sincronización
    # ====================================================

    async def request_log_sync(self):
        """Solicita sincronización de log al líder"""
        if not self.leader_id or self.is_leader():
            return

        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                async with session.get(f"{self.leader_id}/raft/sync?follower={self.node_id}") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        missing_entries = data.get("missing_entries", [])
                        for entry_data in missing_entries:
                            entry = LogEntry.from_dict(entry_data)
                            if entry.index > len(self.log):
                                self.log.append(entry)
                        self.save_state()
                        logger.info(f"✅ Sincronizadas {len(missing_entries)} entradas desde líder")
        except Exception as e:
            logger.warning(f"Error sincronizando con líder: {e}")

    # ====================================================
    # Aplicación a máquina de estado
    # ====================================================

    async def _drain_committed_entries(self):
        """Aplica todas las entradas pendientes hasta commit_index."""
        async with self._lock:
            while self.last_applied < self.commit_index and self.last_applied < len(self.log):
                entry = self.log[self.last_applied]
                await self.apply_to_state_machine(entry)
                self.last_applied += 1
            self.save_state()

    async def apply_to_state_machine(self, entry: LogEntry):
        """Aplica una entrada comprometida a la máquina de estado"""
        if self.state_machine_callback:
            try:
                await self.state_machine_callback(entry)
            except Exception as e:
                logger.error(f"Error aplicando entrada en estado: {e}")
        else:
            logger.info(f"📥 [{self.node_id}] Aplicando: {entry.command} (índice {entry.index})")
