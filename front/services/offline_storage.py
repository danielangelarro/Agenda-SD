"""
Almacenamiento local SQLite para funcionalidad offline-first.

Este módulo proporciona una capa de persistencia local que permite:
- Cachear datos del servidor para acceso offline
- Almacenar sesiones para login offline
- Trackear metadatos de sincronización
- Marcar datos como "dirty" (modificados localmente)
"""

import sqlite3
import json
import os
import hashlib
import threading
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path


class OfflineStorage:
    """
    Almacenamiento local SQLite para datos offline.
    
    Cada usuario tiene su propia base de datos local para evitar
    conflictos y mantener aislamiento de datos.
    """
    
    _instances: Dict[int, 'OfflineStorage'] = {}
    _lock = threading.Lock()
    
    def __new__(cls, user_id: int = 0, db_path: str = None):
        """Singleton por user_id para evitar múltiples conexiones."""
        with cls._lock:
            if user_id not in cls._instances:
                instance = super().__new__(cls)
                instance._initialized = False
                cls._instances[user_id] = instance
            return cls._instances[user_id]
    
    def __init__(self, user_id: int = 0, db_path: str = None):
        if self._initialized:
            return
            
        self.user_id = user_id
        
        # Directorio para bases de datos locales
        if db_path:
            self.db_path = db_path
        else:
            data_dir = Path(os.getenv('OFFLINE_DATA_DIR', 'data/offline'))
            data_dir.mkdir(parents=True, exist_ok=True)
            self.db_path = str(data_dir / f"user_{user_id}_cache.db")
        
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._db_lock = threading.Lock()
        
        self._init_schema()
        self._initialized = True
    
    def _init_schema(self):
        """Inicializa el esquema de la base de datos local."""
        with self._db_lock:
            cursor = self._conn.cursor()
            
            # Tabla de eventos cacheados
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cached_events (
                    id TEXT PRIMARY KEY,
                    server_id INTEGER,
                    data TEXT NOT NULL,
                    synced_at TIMESTAMP,
                    is_dirty INTEGER DEFAULT 0,
                    is_deleted INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Tabla de grupos cacheados
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cached_groups (
                    id TEXT PRIMARY KEY,
                    server_id INTEGER,
                    data TEXT NOT NULL,
                    synced_at TIMESTAMP,
                    is_dirty INTEGER DEFAULT 0,
                    is_deleted INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Tabla de miembros de grupo cacheados
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cached_group_members (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id TEXT NOT NULL,
                    data TEXT NOT NULL,
                    synced_at TIMESTAMP,
                    UNIQUE(group_id)
                )
            """)
            
            # Tabla de usuarios cacheados
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cached_users (
                    id INTEGER PRIMARY KEY,
                    username TEXT NOT NULL,
                    data TEXT NOT NULL,
                    synced_at TIMESTAMP
                )
            """)
            
            # Tabla de invitaciones cacheadas (grupos y eventos)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cached_invitations (
                    id TEXT PRIMARY KEY,
                    invitation_type TEXT NOT NULL,
                    data TEXT NOT NULL,
                    synced_at TIMESTAMP,
                    is_dirty INTEGER DEFAULT 0
                )
            """)
            
            # Tabla de participantes de eventos
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cached_event_participants (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    data TEXT NOT NULL,
                    synced_at TIMESTAMP,
                    UNIQUE(event_id)
                )
            """)
            
            # Metadatos de sincronización
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sync_metadata (
                    entity_type TEXT PRIMARY KEY,
                    last_sync TIMESTAMP,
                    last_full_sync TIMESTAMP,
                    sync_version INTEGER DEFAULT 0
                )
            """)
            
            # Sesión local para login offline
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS local_session (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    user_id INTEGER NOT NULL,
                    username TEXT NOT NULL,
                    token TEXT NOT NULL,
                    password_hash TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_online_login TIMESTAMP
                )
            """)
            
            # Índices para búsquedas rápidas
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_server_id ON cached_events(server_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_dirty ON cached_events(is_dirty)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_groups_server_id ON cached_groups(server_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_groups_dirty ON cached_groups(is_dirty)")
            
            self._conn.commit()
    
    # =========================================================================
    # GESTIÓN DE SESIÓN
    # =========================================================================
    
    def save_session(self, user_id: int, username: str, token: str, 
                     password: str = None) -> None:
        """
        Guarda la sesión del usuario para permitir login offline.
        
        Args:
            user_id: ID del usuario
            username: Nombre de usuario
            token: Token de autenticación actual
            password: Contraseña en texto plano (se hashea para verificación offline)
        """
        password_hash = None
        if password:
            password_hash = hashlib.sha256(password.encode()).hexdigest()
        
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO local_session 
                (id, user_id, username, token, password_hash, last_online_login)
                VALUES (1, ?, ?, ?, ?, ?)
            """, (user_id, username, token, password_hash, datetime.now().isoformat()))
            self._conn.commit()
    
    def get_session(self) -> Optional[Dict[str, Any]]:
        """
        Obtiene la sesión guardada localmente.
        
        Returns:
            Dict con user_id, username, token o None si no hay sesión
        """
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT * FROM local_session WHERE id = 1")
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
    
    def verify_offline_password(self, password: str) -> bool:
        """
        Verifica la contraseña contra el hash guardado para login offline.
        
        Args:
            password: Contraseña a verificar
            
        Returns:
            True si la contraseña es correcta
        """
        session = self.get_session()
        if not session or not session.get('password_hash'):
            return False
        
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        return password_hash == session['password_hash']
    
    def clear_session(self) -> None:
        """Elimina la sesión guardada (logout)."""
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute("DELETE FROM local_session")
            self._conn.commit()
    
    # =========================================================================
    # CACHEO DE EVENTOS
    # =========================================================================
    
    def cache_events(self, events: List[Dict]) -> None:
        """
        Cachea una lista de eventos del servidor.
        
        Args:
            events: Lista de diccionarios con datos de eventos
        """
        now = datetime.now().isoformat()
        with self._db_lock:
            cursor = self._conn.cursor()
            for event in events:
                event_id = str(event.get('id', ''))
                cursor.execute("""
                    INSERT OR REPLACE INTO cached_events 
                    (id, server_id, data, synced_at, is_dirty, updated_at)
                    VALUES (?, ?, ?, ?, 0, ?)
                """, (event_id, event.get('id'), json.dumps(event), now, now))
            self._conn.commit()
        
        self.set_last_sync('events', datetime.now())
    
    def cache_event(self, event_id: str, event_data: Dict, is_dirty: bool = False) -> None:
        """
        Cachea un único evento.
        
        Args:
            event_id: ID del evento (puede ser local o del servidor)
            event_data: Datos del evento
            is_dirty: True si es una modificación local pendiente de sync
        """
        now = datetime.now().isoformat()
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO cached_events 
                (id, server_id, data, synced_at, is_dirty, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                str(event_id), 
                event_data.get('id') if not str(event_id).startswith('local_') else None,
                json.dumps(event_data), 
                now if not is_dirty else None,
                1 if is_dirty else 0,
                now
            ))
            self._conn.commit()
    
    def get_cached_events(self, include_deleted: bool = False) -> List[Dict]:
        """
        Obtiene todos los eventos cacheados.
        
        Args:
            include_deleted: Si incluir eventos marcados como eliminados
            
        Returns:
            Lista de eventos
        """
        with self._db_lock:
            cursor = self._conn.cursor()
            if include_deleted:
                cursor.execute("SELECT data, is_dirty FROM cached_events")
            else:
                cursor.execute("SELECT data, is_dirty FROM cached_events WHERE is_deleted = 0")
            
            events = []
            for row in cursor.fetchall():
                event = json.loads(row['data'])
                event['_is_dirty'] = bool(row['is_dirty'])
                events.append(event)
            return events
    
    def get_dirty_events(self) -> List[Dict]:
        """Obtiene eventos modificados localmente pendientes de sincronizar."""
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT id, data FROM cached_events WHERE is_dirty = 1")
            return [{'local_id': row['id'], **json.loads(row['data'])} for row in cursor.fetchall()]
    
    def mark_event_synced(self, local_id: str, server_id: int = None) -> None:
        """Marca un evento como sincronizado."""
        now = datetime.now().isoformat()
        with self._db_lock:
            cursor = self._conn.cursor()
            if server_id:
                cursor.execute("""
                    UPDATE cached_events 
                    SET is_dirty = 0, server_id = ?, synced_at = ?
                    WHERE id = ?
                """, (server_id, now, local_id))
            else:
                cursor.execute("""
                    UPDATE cached_events 
                    SET is_dirty = 0, synced_at = ?
                    WHERE id = ?
                """, (now, local_id))
            self._conn.commit()
    
    def update_event_id(self, local_id: str, server_id: int) -> None:
        """Actualiza el ID del servidor para un evento local."""
        with self._db_lock:
            cursor = self._conn.cursor()
            # Obtener datos actuales
            cursor.execute("SELECT data FROM cached_events WHERE id = ?", (local_id,))
            row = cursor.fetchone()
            if row:
                data = json.loads(row['data'])
                data['id'] = server_id
                # Actualizar con nuevo ID
                cursor.execute("""
                    UPDATE cached_events 
                    SET server_id = ?, data = ?, is_dirty = 0, synced_at = ?
                    WHERE id = ?
                """, (server_id, json.dumps(data), datetime.now().isoformat(), local_id))
                self._conn.commit()
    
    def mark_event_deleted(self, event_id: str) -> None:
        """Marca un evento como eliminado (soft delete para sync)."""
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                UPDATE cached_events 
                SET is_deleted = 1, is_dirty = 1, updated_at = ?
                WHERE id = ? OR server_id = ?
            """, (datetime.now().isoformat(), str(event_id), event_id))
            self._conn.commit()
    
    # =========================================================================
    # CACHEO DE GRUPOS
    # =========================================================================
    
    def cache_groups(self, groups: List[Dict]) -> None:
        """Cachea una lista de grupos del servidor."""
        now = datetime.now().isoformat()
        with self._db_lock:
            cursor = self._conn.cursor()
            for group in groups:
                group_id = str(group.get('id', ''))
                cursor.execute("""
                    INSERT OR REPLACE INTO cached_groups 
                    (id, server_id, data, synced_at, is_dirty, updated_at)
                    VALUES (?, ?, ?, ?, 0, ?)
                """, (group_id, group.get('id'), json.dumps(group), now, now))
            self._conn.commit()
        
        self.set_last_sync('groups', datetime.now())
    
    def cache_group(self, group_id: str, group_data: Dict, is_dirty: bool = False) -> None:
        """Cachea un único grupo."""
        now = datetime.now().isoformat()
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO cached_groups 
                (id, server_id, data, synced_at, is_dirty, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                str(group_id),
                group_data.get('id') if not str(group_id).startswith('local_') else None,
                json.dumps(group_data),
                now if not is_dirty else None,
                1 if is_dirty else 0,
                now
            ))
            self._conn.commit()
    
    def get_cached_groups(self, include_deleted: bool = False) -> List[Dict]:
        """Obtiene todos los grupos cacheados."""
        with self._db_lock:
            cursor = self._conn.cursor()
            if include_deleted:
                cursor.execute("SELECT data, is_dirty FROM cached_groups")
            else:
                cursor.execute("SELECT data, is_dirty FROM cached_groups WHERE is_deleted = 0")
            
            groups = []
            for row in cursor.fetchall():
                group = json.loads(row['data'])
                group['_is_dirty'] = bool(row['is_dirty'])
                groups.append(group)
            return groups
    
    def get_dirty_groups(self) -> List[Dict]:
        """Obtiene grupos modificados localmente pendientes de sincronizar."""
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT id, data FROM cached_groups WHERE is_dirty = 1")
            return [{'local_id': row['id'], **json.loads(row['data'])} for row in cursor.fetchall()]
    
    def cache_group_members(self, group_id: str, members: List[Dict]) -> None:
        """Cachea los miembros de un grupo."""
        now = datetime.now().isoformat()
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO cached_group_members (group_id, data, synced_at)
                VALUES (?, ?, ?)
            """, (str(group_id), json.dumps(members), now))
            self._conn.commit()
    
    def get_cached_group_members(self, group_id: str) -> List[Dict]:
        """Obtiene los miembros cacheados de un grupo."""
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT data FROM cached_group_members WHERE group_id = ?", (str(group_id),))
            row = cursor.fetchone()
            if row:
                return json.loads(row['data'])
            return []
    
    # =========================================================================
    # CACHEO DE USUARIOS
    # =========================================================================
    
    def cache_users(self, users: List[Dict]) -> None:
        """Cachea una lista de usuarios del servidor."""
        now = datetime.now().isoformat()
        with self._db_lock:
            cursor = self._conn.cursor()
            for user in users:
                cursor.execute("""
                    INSERT OR REPLACE INTO cached_users (id, username, data, synced_at)
                    VALUES (?, ?, ?, ?)
                """, (user.get('id'), user.get('username', ''), json.dumps(user), now))
            self._conn.commit()
        
        self.set_last_sync('users', datetime.now())
    
    def get_cached_users(self) -> List[Dict]:
        """Obtiene todos los usuarios cacheados."""
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT data FROM cached_users")
            return [json.loads(row['data']) for row in cursor.fetchall()]
    
    def get_cached_user(self, user_id: int) -> Optional[Dict]:
        """Obtiene un usuario específico del caché."""
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT data FROM cached_users WHERE id = ?", (user_id,))
            row = cursor.fetchone()
            if row:
                return json.loads(row['data'])
            return None
    
    # =========================================================================
    # CACHEO DE INVITACIONES
    # =========================================================================
    
    def cache_invitations(self, invitations: List[Dict], invitation_type: str) -> None:
        """
        Cachea invitaciones (de grupo o de evento).
        
        Args:
            invitations: Lista de invitaciones
            invitation_type: 'group' o 'event'
        """
        now = datetime.now().isoformat()
        with self._db_lock:
            cursor = self._conn.cursor()
            for inv in invitations:
                inv_id = f"{invitation_type}_{inv.get('id', '')}"
                cursor.execute("""
                    INSERT OR REPLACE INTO cached_invitations 
                    (id, invitation_type, data, synced_at, is_dirty)
                    VALUES (?, ?, ?, ?, 0)
                """, (inv_id, invitation_type, json.dumps(inv), now))
            self._conn.commit()
        
        self.set_last_sync(f'{invitation_type}_invitations', datetime.now())
    
    def get_cached_invitations(self, invitation_type: str = None) -> List[Dict]:
        """Obtiene invitaciones cacheadas."""
        with self._db_lock:
            cursor = self._conn.cursor()
            if invitation_type:
                cursor.execute(
                    "SELECT data FROM cached_invitations WHERE invitation_type = ?", 
                    (invitation_type,)
                )
            else:
                cursor.execute("SELECT data, invitation_type FROM cached_invitations")
            
            return [json.loads(row['data']) for row in cursor.fetchall()]
    
    def cache_event_participants(self, event_id: str, participants: List[Dict]) -> None:
        """Cachea los participantes de un evento."""
        now = datetime.now().isoformat()
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO cached_event_participants (event_id, data, synced_at)
                VALUES (?, ?, ?)
            """, (str(event_id), json.dumps(participants), now))
            self._conn.commit()
    
    def get_cached_event_participants(self, event_id: str) -> List[Dict]:
        """Obtiene los participantes cacheados de un evento."""
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT data FROM cached_event_participants WHERE event_id = ?", (str(event_id),))
            row = cursor.fetchone()
            if row:
                return json.loads(row['data'])
            return []
    
    # =========================================================================
    # METADATOS DE SINCRONIZACIÓN
    # =========================================================================
    
    def get_last_sync(self, entity_type: str) -> Optional[datetime]:
        """Obtiene el timestamp de la última sincronización para un tipo de entidad."""
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT last_sync FROM sync_metadata WHERE entity_type = ?", 
                (entity_type,)
            )
            row = cursor.fetchone()
            if row and row['last_sync']:
                return datetime.fromisoformat(row['last_sync'])
            return None
    
    def set_last_sync(self, entity_type: str, timestamp: datetime) -> None:
        """Establece el timestamp de la última sincronización."""
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO sync_metadata (entity_type, last_sync)
                VALUES (?, ?)
            """, (entity_type, timestamp.isoformat()))
            self._conn.commit()
    
    def get_sync_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas de sincronización."""
        with self._db_lock:
            cursor = self._conn.cursor()
            
            # Contar entidades por tipo
            stats = {}
            
            cursor.execute("SELECT COUNT(*) as count FROM cached_events WHERE is_deleted = 0")
            stats['events_count'] = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) as count FROM cached_events WHERE is_dirty = 1")
            stats['events_dirty'] = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) as count FROM cached_groups WHERE is_deleted = 0")
            stats['groups_count'] = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) as count FROM cached_groups WHERE is_dirty = 1")
            stats['groups_dirty'] = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) as count FROM cached_users")
            stats['users_count'] = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) as count FROM cached_invitations")
            stats['invitations_count'] = cursor.fetchone()['count']
            
            # Última sincronización por tipo
            cursor.execute("SELECT entity_type, last_sync FROM sync_metadata")
            stats['last_syncs'] = {row['entity_type']: row['last_sync'] for row in cursor.fetchall()}
            
            return stats
    
    def get_storage_summary(self) -> Dict[str, Any]:
        """
        Obtiene un resumen del almacenamiento local para diagnóstico.
        Útil para el endpoint /data/distribution del coordinador.
        """
        stats = self.get_sync_stats()
        session = self.get_session()
        
        return {
            'user_id': self.user_id,
            'username': session.get('username') if session else None,
            'db_path': self.db_path,
            'counts': {
                'events': stats['events_count'],
                'events_pending_sync': stats['events_dirty'],
                'groups': stats['groups_count'],
                'groups_pending_sync': stats['groups_dirty'],
                'users': stats['users_count'],
                'invitations': stats['invitations_count'],
            },
            'last_syncs': stats['last_syncs'],
            'has_pending_operations': stats['events_dirty'] > 0 or stats['groups_dirty'] > 0,
        }
    
    # =========================================================================
    # LIMPIEZA Y MANTENIMIENTO
    # =========================================================================
    
    def clear_all_cache(self) -> None:
        """Elimina todo el caché local (no afecta la sesión)."""
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute("DELETE FROM cached_events")
            cursor.execute("DELETE FROM cached_groups")
            cursor.execute("DELETE FROM cached_group_members")
            cursor.execute("DELETE FROM cached_users")
            cursor.execute("DELETE FROM cached_invitations")
            cursor.execute("DELETE FROM cached_event_participants")
            cursor.execute("DELETE FROM sync_metadata")
            self._conn.commit()
    
    def clear_synced_deleted(self) -> None:
        """Elimina permanentemente los registros marcados como eliminados y ya sincronizados."""
        with self._db_lock:
            cursor = self._conn.cursor()
            cursor.execute("DELETE FROM cached_events WHERE is_deleted = 1 AND is_dirty = 0")
            cursor.execute("DELETE FROM cached_groups WHERE is_deleted = 1 AND is_dirty = 0")
            self._conn.commit()
    
    def vacuum(self) -> None:
        """Compacta la base de datos."""
        with self._db_lock:
            self._conn.execute("VACUUM")
    
    def close(self) -> None:
        """Cierra la conexión a la base de datos."""
        with self._db_lock:
            self._conn.close()
        
        # Eliminar de instancias
        with OfflineStorage._lock:
            if self.user_id in OfflineStorage._instances:
                del OfflineStorage._instances[self.user_id]
    
    @classmethod
    def get_instance(cls, user_id: int) -> Optional['OfflineStorage']:
        """Obtiene la instancia existente para un usuario si existe."""
        with cls._lock:
            return cls._instances.get(user_id)
    
    @classmethod
    def clear_all_instances(cls) -> None:
        """Cierra y elimina todas las instancias (para cleanup)."""
        with cls._lock:
            for instance in list(cls._instances.values()):
                try:
                    instance._conn.close()
                except Exception:
                    pass
            cls._instances.clear()
