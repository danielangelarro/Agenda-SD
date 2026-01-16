"""
Cola de operaciones pendientes para sincronización offline.

Este módulo implementa una cola FIFO persistente para operaciones de escritura
realizadas mientras el usuario está offline, que se sincronizarán cuando
la conexión esté disponible.
"""

import sqlite3
import json
import uuid
import threading
from datetime import datetime
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, asdict
from enum import Enum


class OperationStatus(Enum):
    """Estados posibles de una operación pendiente."""
    PENDING = "pending"         # En espera de sincronización
    PROCESSING = "processing"   # Sincronizándose actualmente
    COMPLETED = "completed"     # Sincronizada exitosamente
    FAILED = "failed"           # Falló permanentemente (excedió reintentos)
    CANCELLED = "cancelled"     # Cancelada por el usuario


class OperationType(Enum):
    """Tipos de operaciones soportadas."""
    # Eventos
    CREATE_EVENT = "create_event"
    UPDATE_EVENT = "update_event"
    DELETE_EVENT = "delete_event"
    RESPOND_EVENT_INVITATION = "respond_event_invitation"
    LEAVE_EVENT = "leave_event"
    
    # Grupos
    CREATE_GROUP = "create_group"
    UPDATE_GROUP = "update_group"
    DELETE_GROUP = "delete_group"
    INVITE_TO_GROUP = "invite_to_group"
    RESPOND_GROUP_INVITATION = "respond_group_invitation"
    REMOVE_GROUP_MEMBER = "remove_group_member"


@dataclass
class PendingOperation:
    """Representa una operación pendiente de sincronizar."""
    id: str                      # UUID único
    operation: str               # Tipo de operación (OperationType value)
    entity_type: str             # 'event', 'group', 'invitation'
    payload: Dict[str, Any]      # Datos de la operación
    created_at: datetime         # Cuándo se creó
    status: str                  # OperationStatus value
    attempts: int = 0            # Número de reintentos
    max_attempts: int = 5        # Máximo de reintentos antes de marcar como failed
    last_error: Optional[str] = None  # Último error si hubo
    last_attempt_at: Optional[datetime] = None  # Última vez que se intentó
    local_id: Optional[str] = None  # ID local de la entidad afectada
    server_id: Optional[int] = None  # ID del servidor (si ya se conoce)
    priority: int = 0            # Prioridad (mayor = más urgente)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convierte a diccionario serializable."""
        return {
            'id': self.id,
            'operation': self.operation,
            'entity_type': self.entity_type,
            'payload': self.payload,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'status': self.status,
            'attempts': self.attempts,
            'max_attempts': self.max_attempts,
            'last_error': self.last_error,
            'last_attempt_at': self.last_attempt_at.isoformat() if self.last_attempt_at else None,
            'local_id': self.local_id,
            'server_id': self.server_id,
            'priority': self.priority,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PendingOperation':
        """Crea una instancia desde un diccionario."""
        return cls(
            id=data['id'],
            operation=data['operation'],
            entity_type=data['entity_type'],
            payload=data['payload'] if isinstance(data['payload'], dict) else json.loads(data['payload']),
            created_at=datetime.fromisoformat(data['created_at']) if data.get('created_at') else datetime.now(),
            status=data.get('status', OperationStatus.PENDING.value),
            attempts=data.get('attempts', 0),
            max_attempts=data.get('max_attempts', 5),
            last_error=data.get('last_error'),
            last_attempt_at=datetime.fromisoformat(data['last_attempt_at']) if data.get('last_attempt_at') else None,
            local_id=data.get('local_id'),
            server_id=data.get('server_id'),
            priority=data.get('priority', 0),
        )
    
    def is_retriable(self) -> bool:
        """Indica si la operación puede reintentarse."""
        return self.attempts < self.max_attempts and self.status != OperationStatus.CANCELLED.value


class PendingOperationsQueue:
    """
    Cola persistente de operaciones pendientes.
    
    Implementa una cola FIFO con prioridades para gestionar operaciones
    de escritura que se realizaron offline y necesitan sincronizarse.
    """
    
    def __init__(self, db_path: str):
        """
        Inicializa la cola de operaciones pendientes.
        
        Args:
            db_path: Ruta a la base de datos SQLite (puede ser la misma
                    que OfflineStorage para compartir conexión)
        """
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        
        self._init_schema()
        
        # Listeners para eventos de la cola
        self._listeners: List[callable] = []
    
    def _init_schema(self):
        """Inicializa el esquema de la tabla de operaciones pendientes."""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pending_operations (
                    id TEXT PRIMARY KEY,
                    operation TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'pending',
                    attempts INTEGER DEFAULT 0,
                    max_attempts INTEGER DEFAULT 5,
                    last_error TEXT,
                    last_attempt_at TIMESTAMP,
                    local_id TEXT,
                    server_id INTEGER,
                    priority INTEGER DEFAULT 0
                )
            """)
            
            # Índices para consultas frecuentes
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_pending_status 
                ON pending_operations(status)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_pending_priority_created 
                ON pending_operations(priority DESC, created_at ASC)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_pending_local_id 
                ON pending_operations(local_id)
            """)
            
            self._conn.commit()
    
    # =========================================================================
    # OPERACIONES DE COLA
    # =========================================================================
    
    def enqueue(self, operation: str, entity_type: str, payload: Dict[str, Any],
                local_id: str = None, priority: int = 0) -> str:
        """
        Encola una nueva operación pendiente.
        
        Args:
            operation: Tipo de operación (ej: 'create_event')
            entity_type: Tipo de entidad ('event', 'group', etc.)
            payload: Datos de la operación
            local_id: ID local de la entidad afectada
            priority: Prioridad (mayor = más urgente)
            
        Returns:
            ID único de la operación encolada
        """
        op_id = str(uuid.uuid4())
        now = datetime.now().isoformat()
        
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                INSERT INTO pending_operations 
                (id, operation, entity_type, payload, created_at, status, local_id, priority)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                op_id,
                operation,
                entity_type,
                json.dumps(payload),
                now,
                OperationStatus.PENDING.value,
                local_id,
                priority
            ))
            self._conn.commit()
        
        self._notify_listeners('enqueued', op_id)
        return op_id
    
    def dequeue(self) -> Optional[PendingOperation]:
        """
        Obtiene y marca como 'processing' la siguiente operación pendiente.
        
        La operación se selecciona basándose en prioridad (mayor primero)
        y luego por orden de creación (FIFO).
        
        Returns:
            La siguiente operación a procesar o None si no hay
        """
        with self._lock:
            cursor = self._conn.cursor()
            
            # Obtener la siguiente operación pendiente
            cursor.execute("""
                SELECT * FROM pending_operations 
                WHERE status = ?
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
            """, (OperationStatus.PENDING.value,))
            
            row = cursor.fetchone()
            if not row:
                return None
            
            # Marcar como processing
            now = datetime.now().isoformat()
            cursor.execute("""
                UPDATE pending_operations 
                SET status = ?, last_attempt_at = ?, attempts = attempts + 1
                WHERE id = ?
            """, (OperationStatus.PROCESSING.value, now, row['id']))
            self._conn.commit()
            
            return PendingOperation.from_dict(dict(row))
    
    def peek(self) -> Optional[PendingOperation]:
        """
        Obtiene la siguiente operación pendiente SIN modificarla.
        
        Returns:
            La siguiente operación o None si no hay
        """
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                SELECT * FROM pending_operations 
                WHERE status = ?
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
            """, (OperationStatus.PENDING.value,))
            
            row = cursor.fetchone()
            if row:
                return PendingOperation.from_dict(dict(row))
            return None
    
    def mark_completed(self, operation_id: str, server_id: int = None) -> None:
        """
        Marca una operación como completada exitosamente.
        
        Args:
            operation_id: ID de la operación
            server_id: ID asignado por el servidor (opcional)
        """
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                UPDATE pending_operations 
                SET status = ?, server_id = ?
                WHERE id = ?
            """, (OperationStatus.COMPLETED.value, server_id, operation_id))
            self._conn.commit()
        
        self._notify_listeners('completed', operation_id)
    
    def mark_failed(self, operation_id: str, error: str, 
                    permanent: bool = False) -> None:
        """
        Marca una operación como fallida.
        
        Args:
            operation_id: ID de la operación
            error: Mensaje de error
            permanent: Si True, marca como FAILED; si False, vuelve a PENDING
                      para reintento (si no excedió max_attempts)
        """
        with self._lock:
            cursor = self._conn.cursor()
            
            # Obtener operación actual
            cursor.execute("SELECT * FROM pending_operations WHERE id = ?", (operation_id,))
            row = cursor.fetchone()
            
            if not row:
                return
            
            op = PendingOperation.from_dict(dict(row))
            
            if permanent or not op.is_retriable():
                new_status = OperationStatus.FAILED.value
            else:
                new_status = OperationStatus.PENDING.value
            
            cursor.execute("""
                UPDATE pending_operations 
                SET status = ?, last_error = ?
                WHERE id = ?
            """, (new_status, error, operation_id))
            self._conn.commit()
        
        self._notify_listeners('failed', operation_id)
    
    def cancel(self, operation_id: str) -> bool:
        """
        Cancela una operación pendiente.
        
        Args:
            operation_id: ID de la operación
            
        Returns:
            True si se canceló, False si no existía
        """
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                UPDATE pending_operations 
                SET status = ?
                WHERE id = ? AND status IN (?, ?)
            """, (
                OperationStatus.CANCELLED.value, 
                operation_id,
                OperationStatus.PENDING.value,
                OperationStatus.FAILED.value
            ))
            self._conn.commit()
            cancelled = cursor.rowcount > 0
        
        if cancelled:
            self._notify_listeners('cancelled', operation_id)
        return cancelled
    
    def retry(self, operation_id: str) -> bool:
        """
        Reintenta una operación fallida.
        
        Args:
            operation_id: ID de la operación
            
        Returns:
            True si se puso en cola para reintento
        """
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                UPDATE pending_operations 
                SET status = ?, attempts = 0, last_error = NULL
                WHERE id = ? AND status = ?
            """, (
                OperationStatus.PENDING.value,
                operation_id,
                OperationStatus.FAILED.value
            ))
            self._conn.commit()
            return cursor.rowcount > 0
    
    def retry_all_failed(self) -> int:
        """
        Reintenta todas las operaciones fallidas.
        
        Returns:
            Número de operaciones puestas en cola para reintento
        """
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                UPDATE pending_operations 
                SET status = ?, attempts = 0, last_error = NULL
                WHERE status = ?
            """, (OperationStatus.PENDING.value, OperationStatus.FAILED.value))
            self._conn.commit()
            return cursor.rowcount
    
    # =========================================================================
    # CONSULTAS
    # =========================================================================
    
    def get_pending_count(self) -> int:
        """Retorna el número de operaciones pendientes."""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) as count FROM pending_operations WHERE status = ?",
                (OperationStatus.PENDING.value,)
            )
            return cursor.fetchone()['count']
    
    def get_all_pending(self) -> List[PendingOperation]:
        """Retorna todas las operaciones pendientes ordenadas por prioridad."""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                SELECT * FROM pending_operations 
                WHERE status = ?
                ORDER BY priority DESC, created_at ASC
            """, (OperationStatus.PENDING.value,))
            return [PendingOperation.from_dict(dict(row)) for row in cursor.fetchall()]
    
    def get_failed_operations(self) -> List[PendingOperation]:
        """Retorna todas las operaciones fallidas."""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                SELECT * FROM pending_operations 
                WHERE status = ?
                ORDER BY created_at DESC
            """, (OperationStatus.FAILED.value,))
            return [PendingOperation.from_dict(dict(row)) for row in cursor.fetchall()]
    
    def get_processing_operations(self) -> List[PendingOperation]:
        """Retorna operaciones actualmente en proceso."""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                SELECT * FROM pending_operations 
                WHERE status = ?
            """, (OperationStatus.PROCESSING.value,))
            return [PendingOperation.from_dict(dict(row)) for row in cursor.fetchall()]
    
    def get_operation(self, operation_id: str) -> Optional[PendingOperation]:
        """Obtiene una operación específica por ID."""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT * FROM pending_operations WHERE id = ?", (operation_id,))
            row = cursor.fetchone()
            if row:
                return PendingOperation.from_dict(dict(row))
            return None
    
    def get_operations_for_entity(self, local_id: str) -> List[PendingOperation]:
        """Obtiene todas las operaciones pendientes para una entidad local."""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                SELECT * FROM pending_operations 
                WHERE local_id = ? AND status IN (?, ?)
                ORDER BY created_at ASC
            """, (local_id, OperationStatus.PENDING.value, OperationStatus.PROCESSING.value))
            return [PendingOperation.from_dict(dict(row)) for row in cursor.fetchall()]
    
    def get_stats(self) -> Dict[str, Any]:
        """Retorna estadísticas de la cola."""
        with self._lock:
            cursor = self._conn.cursor()
            
            stats = {}
            
            # Contar por estado
            cursor.execute("""
                SELECT status, COUNT(*) as count 
                FROM pending_operations 
                GROUP BY status
            """)
            stats['by_status'] = {row['status']: row['count'] for row in cursor.fetchall()}
            
            # Contar por tipo de operación
            cursor.execute("""
                SELECT operation, COUNT(*) as count 
                FROM pending_operations 
                WHERE status = ?
                GROUP BY operation
            """, (OperationStatus.PENDING.value,))
            stats['pending_by_operation'] = {row['operation']: row['count'] for row in cursor.fetchall()}
            
            # Total pendiente
            stats['total_pending'] = stats['by_status'].get(OperationStatus.PENDING.value, 0)
            stats['total_failed'] = stats['by_status'].get(OperationStatus.FAILED.value, 0)
            stats['total_processing'] = stats['by_status'].get(OperationStatus.PROCESSING.value, 0)
            
            # Operación más antigua pendiente
            cursor.execute("""
                SELECT created_at FROM pending_operations 
                WHERE status = ?
                ORDER BY created_at ASC
                LIMIT 1
            """, (OperationStatus.PENDING.value,))
            row = cursor.fetchone()
            stats['oldest_pending'] = row['created_at'] if row else None
            
            return stats
    
    # =========================================================================
    # LIMPIEZA
    # =========================================================================
    
    def clear_completed(self, older_than_hours: int = 24) -> int:
        """
        Elimina operaciones completadas antiguas.
        
        Args:
            older_than_hours: Eliminar completadas hace más de X horas
            
        Returns:
            Número de operaciones eliminadas
        """
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                DELETE FROM pending_operations 
                WHERE status = ? 
                AND datetime(created_at) < datetime('now', ?)
            """, (OperationStatus.COMPLETED.value, f'-{older_than_hours} hours'))
            self._conn.commit()
            return cursor.rowcount
    
    def clear_cancelled(self) -> int:
        """Elimina todas las operaciones canceladas."""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "DELETE FROM pending_operations WHERE status = ?",
                (OperationStatus.CANCELLED.value,)
            )
            self._conn.commit()
            return cursor.rowcount
    
    def clear_all(self) -> int:
        """Elimina TODAS las operaciones (usar con cuidado)."""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("DELETE FROM pending_operations")
            self._conn.commit()
            return cursor.rowcount
    
    def reset_stuck_processing(self, older_than_minutes: int = 5) -> int:
        """
        Resetea operaciones que quedaron 'stuck' en processing.
        
        Esto puede ocurrir si la aplicación se cierra durante la sincronización.
        
        Args:
            older_than_minutes: Considerar stuck si está processing por más de X minutos
            
        Returns:
            Número de operaciones reseteadas
        """
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                UPDATE pending_operations 
                SET status = ?
                WHERE status = ?
                AND datetime(last_attempt_at) < datetime('now', ?)
            """, (
                OperationStatus.PENDING.value,
                OperationStatus.PROCESSING.value,
                f'-{older_than_minutes} minutes'
            ))
            self._conn.commit()
            return cursor.rowcount
    
    # =========================================================================
    # LISTENERS
    # =========================================================================
    
    def add_listener(self, callback: callable) -> None:
        """
        Agrega un listener para eventos de la cola.
        
        El callback recibe (event_type, operation_id) donde event_type es:
        'enqueued', 'completed', 'failed', 'cancelled'
        """
        self._listeners.append(callback)
    
    def remove_listener(self, callback: callable) -> None:
        """Remueve un listener."""
        if callback in self._listeners:
            self._listeners.remove(callback)
    
    def _notify_listeners(self, event_type: str, operation_id: str) -> None:
        """Notifica a todos los listeners de un evento."""
        for listener in self._listeners:
            try:
                listener(event_type, operation_id)
            except Exception:
                pass  # Ignorar errores en listeners
    
    # =========================================================================
    # UTILIDADES
    # =========================================================================
    
    def has_pending_operations(self) -> bool:
        """Indica si hay operaciones pendientes de sincronizar."""
        return self.get_pending_count() > 0
    
    def get_next_batch(self, batch_size: int = 10) -> List[PendingOperation]:
        """
        Obtiene un batch de operaciones para procesar en paralelo.
        
        Marca todas como 'processing' antes de retornarlas.
        
        Args:
            batch_size: Número máximo de operaciones a obtener
            
        Returns:
            Lista de operaciones para procesar
        """
        with self._lock:
            cursor = self._conn.cursor()
            
            # Obtener batch de operaciones
            cursor.execute("""
                SELECT * FROM pending_operations 
                WHERE status = ?
                ORDER BY priority DESC, created_at ASC
                LIMIT ?
            """, (OperationStatus.PENDING.value, batch_size))
            
            rows = cursor.fetchall()
            if not rows:
                return []
            
            # Marcar todas como processing
            now = datetime.now().isoformat()
            ids = [row['id'] for row in rows]
            placeholders = ','.join('?' * len(ids))
            cursor.execute(f"""
                UPDATE pending_operations 
                SET status = ?, last_attempt_at = ?, attempts = attempts + 1
                WHERE id IN ({placeholders})
            """, [OperationStatus.PROCESSING.value, now] + ids)
            self._conn.commit()
            
            return [PendingOperation.from_dict(dict(row)) for row in rows]
    
    def close(self) -> None:
        """Cierra la conexión a la base de datos."""
        with self._lock:
            self._conn.close()
