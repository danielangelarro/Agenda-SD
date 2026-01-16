"""
Resolución de conflictos para sincronización offline-first.

Este módulo implementa estrategias para resolver conflictos cuando
los datos locales y remotos diverjan durante la sincronización.
"""

import json
import uuid
import threading
from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass, field


class ConflictType(Enum):
    """Tipos de conflictos posibles."""
    CREATE_CREATE = "create_create"  # Mismo ID creado en ambos lados
    UPDATE_UPDATE = "update_update"  # Mismo registro modificado en ambos lados
    UPDATE_DELETE = "update_delete"  # Modificado localmente, eliminado en servidor
    DELETE_UPDATE = "delete_update"  # Eliminado localmente, modificado en servidor
    VERSION_MISMATCH = "version_mismatch"  # Versiones incompatibles


class ResolutionStrategy(Enum):
    """Estrategias de resolución de conflictos."""
    SERVER_WINS = "server_wins"      # El servidor siempre gana
    CLIENT_WINS = "client_wins"      # El cliente siempre gana
    LATEST_WINS = "latest_wins"      # El más reciente gana (basado en timestamp)
    MERGE = "merge"                  # Intentar combinar cambios
    MANUAL = "manual"                # Requiere intervención del usuario


class ConflictStatus(Enum):
    """Estados de un conflicto."""
    PENDING = "pending"       # Esperando resolución
    RESOLVED = "resolved"     # Resuelto
    IGNORED = "ignored"       # Ignorado por el usuario


@dataclass
class Conflict:
    """Representa un conflicto de sincronización."""
    id: str                          # UUID único
    type: ConflictType               # Tipo de conflicto
    entity_type: str                 # 'event', 'group', etc.
    entity_id: str                   # ID de la entidad (local o servidor)
    local_data: Dict[str, Any]       # Datos locales
    remote_data: Dict[str, Any]      # Datos del servidor
    detected_at: datetime            # Cuándo se detectó
    status: ConflictStatus = ConflictStatus.PENDING
    resolution: Optional[str] = None  # Estrategia usada si se resolvió
    resolved_data: Optional[Dict[str, Any]] = None  # Datos finales
    resolved_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)  # Info adicional
    
    def to_dict(self) -> Dict[str, Any]:
        """Convierte a diccionario serializable."""
        return {
            'id': self.id,
            'type': self.type.value,
            'entity_type': self.entity_type,
            'entity_id': self.entity_id,
            'local_data': self.local_data,
            'remote_data': self.remote_data,
            'detected_at': self.detected_at.isoformat() if self.detected_at else None,
            'status': self.status.value,
            'resolution': self.resolution,
            'resolved_data': self.resolved_data,
            'resolved_at': self.resolved_at.isoformat() if self.resolved_at else None,
            'metadata': self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Conflict':
        """Crea una instancia desde un diccionario."""
        return cls(
            id=data['id'],
            type=ConflictType(data['type']),
            entity_type=data['entity_type'],
            entity_id=data['entity_id'],
            local_data=data['local_data'],
            remote_data=data['remote_data'],
            detected_at=datetime.fromisoformat(data['detected_at']) if data.get('detected_at') else datetime.now(),
            status=ConflictStatus(data.get('status', 'pending')),
            resolution=data.get('resolution'),
            resolved_data=data.get('resolved_data'),
            resolved_at=datetime.fromisoformat(data['resolved_at']) if data.get('resolved_at') else None,
            metadata=data.get('metadata', {}),
        )
    
    def get_diff(self) -> Dict[str, Any]:
        """Retorna las diferencias entre local y remoto."""
        diff = {
            'only_in_local': {},
            'only_in_remote': {},
            'different_values': {},
        }
        
        local_keys = set(self.local_data.keys())
        remote_keys = set(self.remote_data.keys())
        
        # Keys solo en local
        for key in local_keys - remote_keys:
            diff['only_in_local'][key] = self.local_data[key]
        
        # Keys solo en remoto
        for key in remote_keys - local_keys:
            diff['only_in_remote'][key] = self.remote_data[key]
        
        # Keys con valores diferentes
        for key in local_keys & remote_keys:
            if self.local_data[key] != self.remote_data[key]:
                diff['different_values'][key] = {
                    'local': self.local_data[key],
                    'remote': self.remote_data[key],
                }
        
        return diff


class ConflictResolver:
    """
    Resuelve conflictos entre datos locales y remotos.
    
    Soporta múltiples estrategias configurables por tipo de entidad.
    """
    
    # Estrategias por defecto por tipo de entidad
    DEFAULT_STRATEGIES = {
        'event': ResolutionStrategy.LATEST_WINS,
        'group': ResolutionStrategy.SERVER_WINS,
        'invitation': ResolutionStrategy.SERVER_WINS,
        'user': ResolutionStrategy.SERVER_WINS,
    }
    
    # Campos de timestamp para determinar "más reciente"
    TIMESTAMP_FIELDS = ['updated_at', 'modified_at', 'created_at', 'start_time']
    
    def __init__(self, default_strategy: ResolutionStrategy = ResolutionStrategy.LATEST_WINS):
        """
        Inicializa el resolver.
        
        Args:
            default_strategy: Estrategia por defecto para entidades no configuradas
        """
        self._default_strategy = default_strategy
        self._strategies: Dict[str, ResolutionStrategy] = dict(self.DEFAULT_STRATEGIES)
        self._conflicts: Dict[str, Conflict] = {}
        self._lock = threading.Lock()
        self._listeners: List[Callable] = []
        
        # Hooks personalizados para merge
        self._merge_handlers: Dict[str, Callable] = {}
    
    # =========================================================================
    # CONFIGURACIÓN
    # =========================================================================
    
    def set_strategy(self, entity_type: str, strategy: ResolutionStrategy) -> None:
        """
        Configura la estrategia para un tipo de entidad.
        
        Args:
            entity_type: Tipo de entidad ('event', 'group', etc.)
            strategy: Estrategia a usar
        """
        self._strategies[entity_type] = strategy
    
    def get_strategy(self, entity_type: str) -> ResolutionStrategy:
        """Obtiene la estrategia configurada para un tipo de entidad."""
        return self._strategies.get(entity_type, self._default_strategy)
    
    def register_merge_handler(self, entity_type: str, 
                               handler: Callable[[Dict, Dict], Dict]) -> None:
        """
        Registra un handler personalizado para merge de un tipo de entidad.
        
        El handler recibe (local_data, remote_data) y debe retornar los datos combinados.
        """
        self._merge_handlers[entity_type] = handler
    
    # =========================================================================
    # DETECCIÓN DE CONFLICTOS
    # =========================================================================
    
    def detect_conflict(self, local_data: Dict[str, Any], 
                        remote_data: Dict[str, Any],
                        entity_type: str,
                        entity_id: str = None) -> Optional[Conflict]:
        """
        Detecta si hay un conflicto entre datos locales y remotos.
        
        Args:
            local_data: Datos locales
            remote_data: Datos del servidor
            entity_type: Tipo de entidad
            entity_id: ID de la entidad (opcional, se extrae de los datos si no se provee)
            
        Returns:
            Conflict si se detecta uno, None si no hay conflicto
        """
        if not local_data and not remote_data:
            return None
        
        # Determinar ID
        if entity_id is None:
            entity_id = str(
                local_data.get('id') or 
                remote_data.get('id') or 
                local_data.get('local_id') or 
                'unknown'
            )
        
        conflict_type = self._determine_conflict_type(local_data, remote_data)
        
        if conflict_type is None:
            return None  # No hay conflicto
        
        # Crear conflicto
        conflict = Conflict(
            id=str(uuid.uuid4()),
            type=conflict_type,
            entity_type=entity_type,
            entity_id=entity_id,
            local_data=local_data or {},
            remote_data=remote_data or {},
            detected_at=datetime.now(),
        )
        
        # Guardar conflicto
        with self._lock:
            self._conflicts[conflict.id] = conflict
        
        self._notify_listeners('detected', conflict)
        
        return conflict
    
    def _determine_conflict_type(self, local_data: Dict, 
                                  remote_data: Dict) -> Optional[ConflictType]:
        """Determina el tipo de conflicto basándose en los datos."""
        
        local_exists = bool(local_data and not local_data.get('_is_deleted'))
        remote_exists = bool(remote_data and not remote_data.get('_is_deleted'))
        local_is_dirty = local_data.get('_is_dirty', False) if local_data else False
        
        if not local_is_dirty:
            return None  # No hay cambios locales, no hay conflicto
        
        # Caso: ambos existen y tienen cambios
        if local_exists and remote_exists:
            # Verificar si realmente hay diferencias significativas
            if self._has_significant_differences(local_data, remote_data):
                return ConflictType.UPDATE_UPDATE
            return None
        
        # Caso: modificado localmente, eliminado en servidor
        if local_exists and not remote_exists:
            return ConflictType.UPDATE_DELETE
        
        # Caso: eliminado localmente, modificado en servidor
        if not local_exists and remote_exists:
            return ConflictType.DELETE_UPDATE
        
        return None
    
    def _has_significant_differences(self, local_data: Dict, 
                                      remote_data: Dict) -> bool:
        """Determina si hay diferencias significativas entre los datos."""
        # Campos a ignorar en la comparación
        ignore_fields = {
            '_is_dirty', '_is_deleted', '_local_id', '_synced', 
            'synced_at', 'updated_at', 'modified_at'
        }
        
        for key in set(local_data.keys()) | set(remote_data.keys()):
            if key in ignore_fields:
                continue
            
            local_val = local_data.get(key)
            remote_val = remote_data.get(key)
            
            if local_val != remote_val:
                return True
        
        return False
    
    # =========================================================================
    # RESOLUCIÓN DE CONFLICTOS
    # =========================================================================
    
    def resolve(self, conflict: Conflict, 
                strategy: ResolutionStrategy = None) -> Dict[str, Any]:
        """
        Resuelve un conflicto usando la estrategia especificada.
        
        Args:
            conflict: Conflicto a resolver
            strategy: Estrategia a usar (default: la configurada para el entity_type)
            
        Returns:
            Datos resueltos
        """
        if strategy is None:
            strategy = self.get_strategy(conflict.entity_type)
        
        resolved_data = None
        
        if strategy == ResolutionStrategy.SERVER_WINS:
            resolved_data = self._resolve_server_wins(conflict)
            
        elif strategy == ResolutionStrategy.CLIENT_WINS:
            resolved_data = self._resolve_client_wins(conflict)
            
        elif strategy == ResolutionStrategy.LATEST_WINS:
            resolved_data = self._resolve_latest_wins(conflict)
            
        elif strategy == ResolutionStrategy.MERGE:
            resolved_data = self._resolve_merge(conflict)
            
        elif strategy == ResolutionStrategy.MANUAL:
            # En modo manual, no resolvemos automáticamente
            return conflict.local_data
        
        # Actualizar estado del conflicto
        conflict.status = ConflictStatus.RESOLVED
        conflict.resolution = strategy.value
        conflict.resolved_data = resolved_data
        conflict.resolved_at = datetime.now()
        
        with self._lock:
            self._conflicts[conflict.id] = conflict
        
        self._notify_listeners('resolved', conflict)
        
        return resolved_data
    
    def resolve_automatically(self, conflict: Conflict) -> Optional[Dict[str, Any]]:
        """
        Intenta resolver un conflicto automáticamente.
        
        Solo resuelve si la estrategia no es MANUAL.
        
        Returns:
            Datos resueltos o None si requiere intervención manual
        """
        strategy = self.get_strategy(conflict.entity_type)
        
        if strategy == ResolutionStrategy.MANUAL:
            return None
        
        return self.resolve(conflict, strategy)
    
    def _resolve_server_wins(self, conflict: Conflict) -> Dict[str, Any]:
        """El servidor gana: usa los datos remotos."""
        return conflict.remote_data.copy() if conflict.remote_data else {}
    
    def _resolve_client_wins(self, conflict: Conflict) -> Dict[str, Any]:
        """El cliente gana: usa los datos locales."""
        result = conflict.local_data.copy() if conflict.local_data else {}
        # Limpiar flags internos
        result.pop('_is_dirty', None)
        result.pop('_is_deleted', None)
        return result
    
    def _resolve_latest_wins(self, conflict: Conflict) -> Dict[str, Any]:
        """El más reciente gana: compara timestamps."""
        local_ts = self._extract_timestamp(conflict.local_data)
        remote_ts = self._extract_timestamp(conflict.remote_data)
        
        if local_ts and remote_ts:
            if local_ts > remote_ts:
                return self._resolve_client_wins(conflict)
            else:
                return self._resolve_server_wins(conflict)
        elif local_ts:
            return self._resolve_client_wins(conflict)
        elif remote_ts:
            return self._resolve_server_wins(conflict)
        else:
            # Sin timestamps, el servidor gana por defecto
            return self._resolve_server_wins(conflict)
    
    def _resolve_merge(self, conflict: Conflict) -> Dict[str, Any]:
        """Intenta combinar los datos de ambas fuentes."""
        
        # Verificar si hay un handler personalizado
        if conflict.entity_type in self._merge_handlers:
            handler = self._merge_handlers[conflict.entity_type]
            return handler(conflict.local_data, conflict.remote_data)
        
        # Merge genérico: preferir valores remotos pero mantener
        # campos locales que no existan en remoto
        result = {}
        
        # Empezar con datos remotos como base
        if conflict.remote_data:
            result.update(conflict.remote_data)
        
        # Agregar campos locales que no están en remoto
        if conflict.local_data:
            for key, value in conflict.local_data.items():
                if key.startswith('_'):
                    continue  # Ignorar campos internos
                if key not in result:
                    result[key] = value
        
        return result
    
    def _extract_timestamp(self, data: Dict[str, Any]) -> Optional[datetime]:
        """Extrae el timestamp más reciente de los datos."""
        if not data:
            return None
        
        for field in self.TIMESTAMP_FIELDS:
            if field in data and data[field]:
                ts = data[field]
                if isinstance(ts, str):
                    try:
                        return datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    except Exception:
                        continue
                elif isinstance(ts, datetime):
                    return ts
        
        return None
    
    # =========================================================================
    # RESOLUCIÓN MANUAL
    # =========================================================================
    
    def resolve_with_local(self, conflict_id: str) -> Optional[Dict[str, Any]]:
        """Resuelve un conflicto usando los datos locales."""
        conflict = self.get_conflict(conflict_id)
        if conflict:
            return self.resolve(conflict, ResolutionStrategy.CLIENT_WINS)
        return None
    
    def resolve_with_remote(self, conflict_id: str) -> Optional[Dict[str, Any]]:
        """Resuelve un conflicto usando los datos remotos."""
        conflict = self.get_conflict(conflict_id)
        if conflict:
            return self.resolve(conflict, ResolutionStrategy.SERVER_WINS)
        return None
    
    def resolve_with_custom(self, conflict_id: str, 
                            data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Resuelve un conflicto con datos personalizados."""
        conflict = self.get_conflict(conflict_id)
        if not conflict:
            return None
        
        conflict.status = ConflictStatus.RESOLVED
        conflict.resolution = 'custom'
        conflict.resolved_data = data
        conflict.resolved_at = datetime.now()
        
        with self._lock:
            self._conflicts[conflict.id] = conflict
        
        self._notify_listeners('resolved', conflict)
        
        return data
    
    def ignore_conflict(self, conflict_id: str) -> bool:
        """Marca un conflicto como ignorado."""
        conflict = self.get_conflict(conflict_id)
        if not conflict:
            return False
        
        conflict.status = ConflictStatus.IGNORED
        conflict.resolved_at = datetime.now()
        
        with self._lock:
            self._conflicts[conflict.id] = conflict
        
        self._notify_listeners('ignored', conflict)
        
        return True
    
    # =========================================================================
    # CONSULTAS
    # =========================================================================
    
    def get_conflict(self, conflict_id: str) -> Optional[Conflict]:
        """Obtiene un conflicto por ID."""
        with self._lock:
            return self._conflicts.get(conflict_id)
    
    def get_pending_conflicts(self) -> List[Conflict]:
        """Retorna todos los conflictos pendientes de resolución."""
        with self._lock:
            return [c for c in self._conflicts.values() 
                    if c.status == ConflictStatus.PENDING]
    
    def get_all_conflicts(self) -> List[Conflict]:
        """Retorna todos los conflictos."""
        with self._lock:
            return list(self._conflicts.values())
    
    def get_conflicts_by_entity(self, entity_type: str) -> List[Conflict]:
        """Retorna conflictos para un tipo de entidad."""
        with self._lock:
            return [c for c in self._conflicts.values() 
                    if c.entity_type == entity_type]
    
    def get_pending_count(self) -> int:
        """Retorna el número de conflictos pendientes."""
        with self._lock:
            return len([c for c in self._conflicts.values() 
                       if c.status == ConflictStatus.PENDING])
    
    def has_pending_conflicts(self) -> bool:
        """Indica si hay conflictos pendientes."""
        return self.get_pending_count() > 0
    
    # =========================================================================
    # LIMPIEZA
    # =========================================================================
    
    def clear_resolved(self) -> int:
        """Elimina conflictos resueltos o ignorados."""
        with self._lock:
            to_remove = [
                cid for cid, c in self._conflicts.items()
                if c.status in (ConflictStatus.RESOLVED, ConflictStatus.IGNORED)
            ]
            for cid in to_remove:
                del self._conflicts[cid]
            return len(to_remove)
    
    def clear_all(self) -> int:
        """Elimina todos los conflictos."""
        with self._lock:
            count = len(self._conflicts)
            self._conflicts.clear()
            return count
    
    # =========================================================================
    # LISTENERS
    # =========================================================================
    
    def add_listener(self, callback: Callable) -> None:
        """
        Agrega un listener para eventos de conflictos.
        
        El callback recibe (event_type, conflict) donde event_type es:
        'detected', 'resolved', 'ignored'
        """
        self._listeners.append(callback)
    
    def remove_listener(self, callback: Callable) -> None:
        """Remueve un listener."""
        if callback in self._listeners:
            self._listeners.remove(callback)
    
    def _notify_listeners(self, event_type: str, conflict: Conflict) -> None:
        """Notifica a los listeners de un evento."""
        for listener in self._listeners:
            try:
                listener(event_type, conflict)
            except Exception:
                pass
    
    # =========================================================================
    # UTILIDADES
    # =========================================================================
    
    def get_stats(self) -> Dict[str, Any]:
        """Retorna estadísticas de conflictos."""
        with self._lock:
            conflicts = list(self._conflicts.values())
        
        return {
            'total': len(conflicts),
            'pending': len([c for c in conflicts if c.status == ConflictStatus.PENDING]),
            'resolved': len([c for c in conflicts if c.status == ConflictStatus.RESOLVED]),
            'ignored': len([c for c in conflicts if c.status == ConflictStatus.IGNORED]),
            'by_type': {
                ct.value: len([c for c in conflicts if c.type == ct])
                for ct in ConflictType
            },
            'by_entity': {},
        }
    
    def export_conflicts(self) -> List[Dict[str, Any]]:
        """Exporta todos los conflictos como lista de diccionarios."""
        with self._lock:
            return [c.to_dict() for c in self._conflicts.values()]
    
    def import_conflicts(self, conflicts_data: List[Dict[str, Any]]) -> int:
        """
        Importa conflictos desde una lista de diccionarios.
        
        Returns:
            Número de conflictos importados
        """
        count = 0
        with self._lock:
            for data in conflicts_data:
                try:
                    conflict = Conflict.from_dict(data)
                    self._conflicts[conflict.id] = conflict
                    count += 1
                except Exception:
                    pass
        return count
