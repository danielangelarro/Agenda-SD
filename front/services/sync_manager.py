"""
Gestor de sincronización automática para funcionalidad offline-first.

Este módulo implementa:
- Detección automática de conectividad
- Sincronización bidireccional (push operaciones locales, pull datos del servidor)
- Background sync thread
- Manejo de conflictos básico (delega a ConflictResolver)
"""

import asyncio
import threading
import time
import logging
from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sync_manager")


class ConnectivityState(Enum):
    """Estados de conectividad."""
    ONLINE = "online"           # Conectado al servidor
    OFFLINE = "offline"         # Sin conexión
    DEGRADED = "degraded"       # Conexión inestable o parcial


@dataclass
class SyncResult:
    """Resultado de una operación de sincronización."""
    success: bool
    synced_operations: int = 0
    failed_operations: int = 0
    conflicts: List[Dict] = None
    errors: List[str] = None
    duration_ms: float = 0
    
    def __post_init__(self):
        if self.conflicts is None:
            self.conflicts = []
        if self.errors is None:
            self.errors = []
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'success': self.success,
            'synced_operations': self.synced_operations,
            'failed_operations': self.failed_operations,
            'conflicts_count': len(self.conflicts),
            'errors': self.errors,
            'duration_ms': self.duration_ms,
        }


class SyncManager:
    """
    Gestor de sincronización bidireccional.
    
    Coordina la sincronización entre el almacenamiento local (OfflineStorage),
    la cola de operaciones pendientes (PendingOperationsQueue) y el
    cliente API (APIClient).
    """
    
    def __init__(self, api_client, storage, queue):
        """
        Inicializa el SyncManager.
        
        Args:
            api_client: Instancia de APIClient para comunicación con servidor
            storage: Instancia de OfflineStorage para caché local
            queue: Instancia de PendingOperationsQueue para operaciones pendientes
        """
        self._api = api_client
        self._storage = storage
        self._queue = queue
        
        self._state = ConnectivityState.OFFLINE
        self._last_connectivity_check = 0
        self._connectivity_check_interval = 5  # segundos
        
        self._state_listeners: List[Callable] = []
        self._sync_listeners: List[Callable] = []
        
        self._background_thread: Optional[threading.Thread] = None
        self._stop_background = threading.Event()
        self._sync_lock = threading.Lock()
        
        self._last_sync_result: Optional[SyncResult] = None
        self._is_syncing = False
        
        # Estadísticas
        self._sync_stats = {
            'total_syncs': 0,
            'successful_syncs': 0,
            'failed_syncs': 0,
            'total_operations_synced': 0,
            'last_sync_time': None,
        }
    
    # =========================================================================
    # DETECCIÓN DE CONECTIVIDAD
    # =========================================================================
    
    def check_connectivity(self, force: bool = False) -> ConnectivityState:
        """
        Verifica el estado de conectividad con el servidor.
        
        Args:
            force: Si True, ignora el caché y fuerza verificación
            
        Returns:
            Estado de conectividad actual
        """
        now = time.time()
        
        # Usar caché si no ha pasado el intervalo
        if not force and (now - self._last_connectivity_check) < self._connectivity_check_interval:
            return self._state
        
        old_state = self._state
        
        try:
            # Intentar obtener el coordinador actual
            base_url = self._api.get_current_base_url()
            
            if base_url:
                # Verificar cuántos coordinadores están vivos
                alive = self._api._alive_bases()
                if len(alive) >= len(self._api.base_urls):
                    new_state = ConnectivityState.ONLINE
                elif len(alive) > 0:
                    new_state = ConnectivityState.DEGRADED
                else:
                    new_state = ConnectivityState.OFFLINE
            else:
                new_state = ConnectivityState.OFFLINE
                
        except Exception as e:
            logger.debug(f"Connectivity check failed: {e}")
            new_state = ConnectivityState.OFFLINE
        
        self._last_connectivity_check = now
        self._state = new_state
        
        # Notificar si cambió el estado
        if old_state != new_state:
            logger.info(f"Connectivity state changed: {old_state.value} -> {new_state.value}")
            self._notify_state_change(old_state, new_state)
        
        return new_state
    
    def get_state(self) -> ConnectivityState:
        """Retorna el estado de conectividad actual."""
        return self._state
    
    def is_online(self) -> bool:
        """Indica si hay conexión con el servidor."""
        state = self.check_connectivity()
        return state in (ConnectivityState.ONLINE, ConnectivityState.DEGRADED)
    
    def is_fully_online(self) -> bool:
        """Indica si todos los coordinadores están disponibles."""
        return self.check_connectivity() == ConnectivityState.ONLINE
    
    # =========================================================================
    # SINCRONIZACIÓN DE OPERACIONES PENDIENTES (PUSH)
    # =========================================================================
    
    def sync_pending_operations(self, token: str) -> SyncResult:
        """
        Sincroniza todas las operaciones pendientes con el servidor.
        
        Args:
            token: Token de autenticación del usuario
            
        Returns:
            Resultado de la sincronización
        """
        start_time = time.time()
        result = SyncResult(success=True)
        
        if not self.is_online():
            return SyncResult(
                success=False, 
                errors=["No hay conexión con el servidor"]
            )
        
        with self._sync_lock:
            self._is_syncing = True
            
            try:
                # Resetear operaciones stuck en processing
                self._queue.reset_stuck_processing()
                
                # Procesar operaciones una por una
                while True:
                    op = self._queue.dequeue()
                    if not op:
                        break
                    
                    try:
                        server_id = self._execute_operation(op, token)
                        self._queue.mark_completed(op.id, server_id)
                        
                        # Actualizar el caché local si es necesario
                        if op.local_id and server_id:
                            self._update_local_after_sync(op, server_id)
                        
                        result.synced_operations += 1
                        
                    except Exception as e:
                        error_msg = str(e)
                        logger.warning(f"Failed to sync operation {op.id}: {error_msg}")
                        
                        # Determinar si es un error permanente
                        is_permanent = self._is_permanent_error(e)
                        self._queue.mark_failed(op.id, error_msg, permanent=is_permanent)
                        
                        result.failed_operations += 1
                        result.errors.append(f"{op.operation}: {error_msg}")
                        
                        if is_permanent:
                            result.success = False
                
                # Actualizar estadísticas
                self._sync_stats['total_syncs'] += 1
                self._sync_stats['total_operations_synced'] += result.synced_operations
                self._sync_stats['last_sync_time'] = datetime.now().isoformat()
                
                if result.failed_operations == 0:
                    self._sync_stats['successful_syncs'] += 1
                else:
                    self._sync_stats['failed_syncs'] += 1
                    
            finally:
                self._is_syncing = False
        
        result.duration_ms = (time.time() - start_time) * 1000
        self._last_sync_result = result
        self._notify_sync_complete(result)
        
        return result
    
    def _execute_operation(self, op, token: str) -> Optional[int]:
        """
        Ejecuta una operación pendiente contra el servidor.
        
        Args:
            op: PendingOperation a ejecutar
            token: Token de autenticación
            
        Returns:
            ID del servidor si es una creación, None en otros casos
        """
        payload = op.payload
        operation = op.operation
        
        # Mapeo de operaciones a métodos del API
        if operation == 'create_event':
            result = self._api.create_event(
                title=payload.get('title', ''),
                description=payload.get('description', ''),
                start_time=payload.get('start_time', ''),
                end_time=payload.get('end_time', ''),
                token=token,
                group_id=payload.get('group_id'),
                is_group_event=payload.get('is_group_event', False),
                participants_ids=payload.get('participants_ids'),
                is_hierarchical=payload.get('is_hierarchical', False),
            )
            return result.get('id') if isinstance(result, dict) else None
            
        elif operation == 'update_event':
            self._api.update_event(
                event_id=payload.get('event_id'),
                token=token,
                title=payload.get('title'),
                description=payload.get('description'),
                start_time=payload.get('start_time'),
                end_time=payload.get('end_time'),
                participants_ids=payload.get('participants_ids'),
            )
            return None
            
        elif operation == 'delete_event':
            self._api.cancel_event(
                event_id=payload.get('event_id'),
                token=token,
            )
            return None
            
        elif operation == 'leave_event':
            self._api.leave_event(
                event_id=payload.get('event_id'),
                token=token,
            )
            return None
            
        elif operation == 'respond_event_invitation':
            self._api.respond_to_event_invitation(
                event_id=payload.get('event_id'),
                accepted=payload.get('accepted', False),
                token=token,
            )
            return None
            
        elif operation == 'create_group':
            result = self._api.create_group(
                name=payload.get('name', ''),
                description=payload.get('description', ''),
                is_hierarchical=payload.get('is_hierarchical', False),
                token=token,
                members=payload.get('members'),
            )
            return result.get('id') if isinstance(result, dict) else None
            
        elif operation == 'update_group':
            self._api.update_group(
                group_id=payload.get('group_id'),
                name=payload.get('name'),
                description=payload.get('description'),
                token=token,
            )
            return None
            
        elif operation == 'delete_group':
            self._api.delete_group(
                group_id=payload.get('group_id'),
                token=token,
            )
            return None
            
        elif operation == 'invite_to_group':
            self._api.invite_user_to_group(
                group_id=payload.get('group_id'),
                invited_user_id=payload.get('invited_user_id'),
                token=token,
            )
            return None
            
        elif operation == 'respond_group_invitation':
            self._api.respond_to_group_invitation(
                invitation_id=payload.get('invitation_id'),
                response=payload.get('response', 'reject'),
                token=token,
            )
            return None
            
        elif operation == 'remove_group_member':
            self._api.remove_member(
                group_id=payload.get('group_id'),
                member_id=payload.get('member_id'),
                token=token,
            )
            return None
        
        else:
            raise ValueError(f"Operación desconocida: {operation}")
    
    def _update_local_after_sync(self, op, server_id: int):
        """Actualiza el caché local después de una sincronización exitosa."""
        try:
            if op.entity_type == 'event' and op.local_id:
                self._storage.update_event_id(op.local_id, server_id)
                self._storage.mark_event_synced(op.local_id, server_id)
            elif op.entity_type == 'group' and op.local_id:
                # Similar para grupos si es necesario
                pass
        except Exception as e:
            logger.warning(f"Failed to update local cache after sync: {e}")
    
    def _is_permanent_error(self, error: Exception) -> bool:
        """Determina si un error es permanente (no debe reintentarse)."""
        error_str = str(error).lower()
        permanent_patterns = [
            'not found',
            'unauthorized',
            'forbidden',
            'invalid',
            'no existe',
            'usuario o contraseña incorrectos',
        ]
        return any(pattern in error_str for pattern in permanent_patterns)
    
    # =========================================================================
    # SINCRONIZACIÓN DESDE SERVIDOR (PULL)
    # =========================================================================
    
    def sync_from_server(self, token: str, 
                         entity_types: List[str] = None) -> SyncResult:
        """
        Descarga datos actualizados del servidor al caché local.
        
        Args:
            token: Token de autenticación
            entity_types: Lista de tipos a sincronizar (default: todos)
            
        Returns:
            Resultado de la sincronización
        """
        start_time = time.time()
        result = SyncResult(success=True)
        
        if not self.is_online():
            return SyncResult(
                success=False,
                errors=["No hay conexión con el servidor"]
            )
        
        if entity_types is None:
            entity_types = ['events', 'groups', 'users', 'invitations']
        
        with self._sync_lock:
            try:
                for entity_type in entity_types:
                    try:
                        if entity_type == 'events':
                            events = self._api.get_user_events_detailed(token)
                            if events:
                                self._storage.cache_events(events)
                                result.synced_operations += len(events)
                                
                        elif entity_type == 'groups':
                            groups = self._api.list_user_groups(token)
                            if groups:
                                # Convertir tuplas a dicts si es necesario
                                group_dicts = []
                                for g in groups:
                                    if isinstance(g, tuple):
                                        group_dicts.append({
                                            'id': g[0], 
                                            'name': g[1], 
                                            'is_hierarchical': g[2] if len(g) > 2 else False
                                        })
                                    else:
                                        group_dicts.append(g)
                                self._storage.cache_groups(group_dicts)
                                result.synced_operations += len(group_dicts)
                                
                        elif entity_type == 'users':
                            users = self._api.list_users(token)
                            if users:
                                self._storage.cache_users(users)
                                result.synced_operations += len(users)
                                
                        elif entity_type == 'invitations':
                            # Invitaciones de grupo
                            group_invs = self._api.get_pending_invitations(token)
                            if group_invs:
                                self._storage.cache_invitations(group_invs, 'group')
                                result.synced_operations += len(group_invs)
                            
                            # Invitaciones de evento
                            event_invs = self._api.get_pending_event_invitations(token)
                            if event_invs:
                                self._storage.cache_invitations(event_invs, 'event')
                                result.synced_operations += len(event_invs)
                                
                    except Exception as e:
                        error_msg = f"Error syncing {entity_type}: {str(e)}"
                        logger.warning(error_msg)
                        result.errors.append(error_msg)
                        result.failed_operations += 1
                
                if result.errors:
                    result.success = len(result.errors) < len(entity_types)
                    
            except Exception as e:
                result.success = False
                result.errors.append(str(e))
        
        result.duration_ms = (time.time() - start_time) * 1000
        return result
    
    def full_sync(self, token: str) -> SyncResult:
        """
        Realiza una sincronización completa bidireccional.
        
        Primero push (operaciones pendientes) y luego pull (datos del servidor).
        
        Args:
            token: Token de autenticación
            
        Returns:
            Resultado combinado de la sincronización
        """
        logger.info("Starting full sync...")
        
        # 1. Push: sincronizar operaciones pendientes
        push_result = self.sync_pending_operations(token)
        
        # 2. Pull: obtener datos actualizados del servidor
        pull_result = self.sync_from_server(token)
        
        # Combinar resultados
        combined = SyncResult(
            success=push_result.success and pull_result.success,
            synced_operations=push_result.synced_operations + pull_result.synced_operations,
            failed_operations=push_result.failed_operations + pull_result.failed_operations,
            conflicts=push_result.conflicts + pull_result.conflicts,
            errors=push_result.errors + pull_result.errors,
            duration_ms=push_result.duration_ms + pull_result.duration_ms,
        )
        
        logger.info(f"Full sync completed: {combined.synced_operations} synced, "
                   f"{combined.failed_operations} failed")
        
        return combined
    
    # =========================================================================
    # BACKGROUND SYNC
    # =========================================================================
    
    def start_background_sync(self, token: str, interval_seconds: int = 30):
        """
        Inicia sincronización en segundo plano.
        
        Args:
            token: Token de autenticación
            interval_seconds: Intervalo entre sincronizaciones
        """
        if self._background_thread and self._background_thread.is_alive():
            logger.warning("Background sync already running")
            return
        
        self._stop_background.clear()
        
        def _sync_loop():
            logger.info(f"Background sync started (interval: {interval_seconds}s)")
            while not self._stop_background.is_set():
                try:
                    # Verificar conectividad
                    if self.is_online():
                        # Solo sincronizar si hay operaciones pendientes
                        if self._queue.has_pending_operations():
                            logger.debug("Background sync: syncing pending operations...")
                            self.sync_pending_operations(token)
                        
                        # Sincronizar datos del servidor periódicamente
                        self.sync_from_server(token, ['events', 'invitations'])
                        
                except Exception as e:
                    logger.error(f"Background sync error: {e}")
                
                # Esperar hasta el próximo ciclo
                self._stop_background.wait(timeout=interval_seconds)
            
            logger.info("Background sync stopped")
        
        self._background_thread = threading.Thread(target=_sync_loop, daemon=True)
        self._background_thread.start()
    
    def stop_background_sync(self):
        """Detiene la sincronización en segundo plano."""
        self._stop_background.set()
        if self._background_thread:
            self._background_thread.join(timeout=5)
            self._background_thread = None
    
    def is_background_sync_running(self) -> bool:
        """Indica si el background sync está activo."""
        return self._background_thread is not None and self._background_thread.is_alive()
    
    # =========================================================================
    # LISTENERS
    # =========================================================================
    
    def on_state_change(self, callback: Callable) -> None:
        """
        Registra un callback para cambios de estado de conectividad.
        
        El callback recibe (old_state: ConnectivityState, new_state: ConnectivityState)
        """
        self._state_listeners.append(callback)
    
    def on_sync_complete(self, callback: Callable) -> None:
        """
        Registra un callback para cuando una sincronización termina.
        
        El callback recibe (result: SyncResult)
        """
        self._sync_listeners.append(callback)
    
    def _notify_state_change(self, old_state: ConnectivityState, 
                             new_state: ConnectivityState) -> None:
        """Notifica a los listeners del cambio de estado."""
        for listener in self._state_listeners:
            try:
                listener(old_state, new_state)
            except Exception as e:
                logger.error(f"State listener error: {e}")
    
    def _notify_sync_complete(self, result: SyncResult) -> None:
        """Notifica a los listeners que una sincronización terminó."""
        for listener in self._sync_listeners:
            try:
                listener(result)
            except Exception as e:
                logger.error(f"Sync listener error: {e}")
    
    # =========================================================================
    # UTILIDADES
    # =========================================================================
    
    def get_sync_status(self) -> Dict[str, Any]:
        """Retorna el estado actual de sincronización."""
        return {
            'connectivity': self._state.value,
            'is_syncing': self._is_syncing,
            'background_sync_running': self.is_background_sync_running(),
            'pending_operations': self._queue.get_pending_count(),
            'failed_operations': len(self._queue.get_failed_operations()),
            'last_sync': self._sync_stats.get('last_sync_time'),
            'stats': self._sync_stats.copy(),
            'last_result': self._last_sync_result.to_dict() if self._last_sync_result else None,
        }
    
    def get_pending_summary(self) -> Dict[str, Any]:
        """Retorna un resumen de las operaciones pendientes."""
        stats = self._queue.get_stats()
        return {
            'total_pending': stats.get('total_pending', 0),
            'total_failed': stats.get('total_failed', 0),
            'by_operation': stats.get('pending_by_operation', {}),
            'oldest_pending': stats.get('oldest_pending'),
        }
    
    def force_sync_now(self, token: str) -> SyncResult:
        """
        Fuerza una sincronización inmediata.
        
        Args:
            token: Token de autenticación
            
        Returns:
            Resultado de la sincronización
        """
        # Forzar verificación de conectividad
        self.check_connectivity(force=True)
        return self.full_sync(token)
    
    def retry_failed_operations(self) -> int:
        """
        Reintenta todas las operaciones fallidas.
        
        Returns:
            Número de operaciones puestas en cola para reintento
        """
        return self._queue.retry_all_failed()
    
    def cancel_pending_operation(self, operation_id: str) -> bool:
        """Cancela una operación pendiente específica."""
        return self._queue.cancel(operation_id)
    
    def clear_completed_operations(self) -> int:
        """Limpia operaciones completadas antiguas."""
        return self._queue.clear_completed()
