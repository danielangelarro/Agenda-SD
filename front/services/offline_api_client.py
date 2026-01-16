"""
Cliente API Offline-First que integra todos los componentes de sincronización.

Este cliente actúa como un wrapper transparente que:
1. Intenta operaciones online primero
2. Si falla, usa datos del caché local
3. Encola operaciones de escritura para sincronización posterior
4. Sincroniza automáticamente cuando hay conexión
"""

import uuid
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from .api_client import APIClient
from .offline_storage import OfflineStorage
from .pending_operations import PendingOperationsQueue, OperationType
from .sync_manager import SyncManager, ConnectivityState
from .conflict_resolver import ConflictResolver

logger = logging.getLogger("offline_api_client")


class OfflineAPIClient:
    """
    Cliente API que opera en modo offline-first.
    
    Todas las operaciones intentan:
    1. Ejecutar contra el servidor si hay conexión
    2. Usar/actualizar caché local
    3. Encolar operaciones para sync posterior si está offline
    """
    
    def __init__(self, user_id: int = 0):
        """
        Inicializa el cliente offline-first.
        
        Args:
            user_id: ID del usuario (0 si no está autenticado aún)
        """
        self.user_id = user_id
        self._api = APIClient()
        self._storage: Optional[OfflineStorage] = None
        self._queue: Optional[PendingOperationsQueue] = None
        self._sync: Optional[SyncManager] = None
        self._conflict_resolver: Optional[ConflictResolver] = None
        self._token: Optional[str] = None
        
        if user_id > 0:
            self._init_offline_components(user_id)
    
    def _init_offline_components(self, user_id: int):
        """Inicializa los componentes offline para un usuario."""
        self._storage = OfflineStorage(user_id=user_id)
        self._queue = PendingOperationsQueue(self._storage.db_path)
        self._sync = SyncManager(self._api, self._storage, self._queue)
        self._conflict_resolver = ConflictResolver()
    
    def set_user(self, user_id: int, token: str):
        """
        Configura el usuario después de login.
        
        Args:
            user_id: ID del usuario
            token: Token de autenticación
        """
        self.user_id = user_id
        self._token = token
        self._init_offline_components(user_id)
    
    @property
    def storage(self) -> Optional[OfflineStorage]:
        return self._storage
    
    @property
    def queue(self) -> Optional[PendingOperationsQueue]:
        return self._queue
    
    @property
    def sync_manager(self) -> Optional[SyncManager]:
        return self._sync
    
    @property
    def conflict_resolver(self) -> Optional[ConflictResolver]:
        return self._conflict_resolver
    
    def is_online(self) -> bool:
        """Verifica si hay conexión con el servidor."""
        if self._sync:
            return self._sync.is_online()
        return self._api.get_current_base_url() is not None
    
    # =========================================================================
    # AUTENTICACIÓN
    # =========================================================================
    
    def login(self, username: str, password: str) -> Dict[str, Any]:
        """
        Intenta login online; si falla, usa credenciales cacheadas.
        
        Args:
            username: Nombre de usuario
            password: Contraseña
            
        Returns:
            Dict con user_id, token, username
        """
        # Intentar login online primero
        try:
            result = self._api.login(username, password)
            
            if result and 'user_id' in result:
                user_id = result['user_id']
                token = result.get('token', '')
                
                # Inicializar componentes offline
                self.set_user(user_id, token)
                
                # Guardar sesión para login offline futuro
                self._storage.save_session(user_id, username, token, password)
                
                # Sincronizar datos del servidor al caché
                try:
                    self._sync.sync_from_server(token)
                except Exception as e:
                    logger.warning(f"Failed to sync after login: {e}")
                
                return {
                    'user_id': user_id,
                    'token': token,
                    'username': username,
                    'offline': False
                }
                
        except Exception as e:
            logger.info(f"Online login failed: {e}, trying offline...")
        
        # Intentar login offline
        return self._try_offline_login(username, password)
    
    def _try_offline_login(self, username: str, password: str) -> Dict[str, Any]:
        """Intenta login con credenciales cacheadas localmente."""
        # Buscar sesión guardada para este usuario
        # Primero necesitamos encontrar el user_id del username
        temp_storage = OfflineStorage(user_id=0)
        session = temp_storage.get_session()
        
        if not session:
            raise Exception("No hay sesión guardada. Necesitas conexión para el primer login.")
        
        if session.get('username') != username:
            raise Exception("Usuario no coincide con la sesión guardada.")
        
        # Verificar contraseña
        if not temp_storage.verify_offline_password(password):
            raise Exception("Contraseña incorrecta.")
        
        user_id = session['user_id']
        token = session['token']
        
        # Inicializar con datos de la sesión guardada
        self.user_id = user_id
        self._token = token
        self._storage = OfflineStorage(user_id=user_id)
        self._queue = PendingOperationsQueue(self._storage.db_path)
        self._sync = SyncManager(self._api, self._storage, self._queue)
        self._conflict_resolver = ConflictResolver()
        
        return {
            'user_id': user_id,
            'token': token,
            'username': username,
            'offline': True
        }
    
    def register(self, username: str, password: str) -> Dict[str, Any]:
        """
        Registra un nuevo usuario (requiere conexión).
        
        Args:
            username: Nombre de usuario
            password: Contraseña
            
        Returns:
            Resultado del registro
        """
        # El registro siempre requiere conexión
        if not self.is_online():
            raise Exception("Se requiere conexión para registrar un nuevo usuario.")
        
        return self._api.register(username, password)
    
    def logout(self):
        """Cierra la sesión y limpia el estado."""
        if self._storage:
            self._storage.clear_session()
        
        self.user_id = 0
        self._token = None
        self._storage = None
        self._queue = None
        self._sync = None
    
    # =========================================================================
    # EVENTOS
    # =========================================================================
    
    def create_event(self, title: str, description: str, start_time: str, 
                     end_time: str, token: str, group_id: Optional[int] = None,
                     is_group_event: bool = False, 
                     participants_ids: Optional[List[int]] = None,
                     is_hierarchical: bool = False) -> Dict[str, Any]:
        """
        Crea un evento (offline-first).
        
        Si está online, crea directamente en el servidor.
        Si está offline, guarda localmente y encola para sync.
        """
        event_data = {
            'title': title,
            'description': description,
            'start_time': start_time,
            'end_time': end_time,
            'group_id': group_id,
            'is_group_event': is_group_event,
            'participants_ids': participants_ids or [],
            'is_hierarchical': is_hierarchical,
        }
        
        # Generar ID local temporal
        local_id = f"local_{uuid.uuid4().hex[:12]}"
        
        # Intentar crear online
        if self.is_online():
            try:
                result = self._api.create_event(
                    title=title, description=description,
                    start_time=start_time, end_time=end_time,
                    token=token, group_id=group_id,
                    is_group_event=is_group_event,
                    participants_ids=participants_ids,
                    is_hierarchical=is_hierarchical
                )
                
                # Cachear el evento creado
                if self._storage and result:
                    self._storage.cache_event(str(result.get('id', local_id)), result)
                
                return result
                
            except Exception as e:
                logger.warning(f"Failed to create event online: {e}")
        
        # Modo offline: guardar localmente y encolar
        event_data['id'] = local_id
        event_data['_local_id'] = local_id
        event_data['_is_dirty'] = True
        event_data['_synced'] = False
        event_data['creator_id'] = self.user_id
        event_data['created_at'] = datetime.now().isoformat()
        
        if self._storage:
            self._storage.cache_event(local_id, event_data, is_dirty=True)
        
        if self._queue:
            self._queue.enqueue(
                operation=OperationType.CREATE_EVENT.value,
                entity_type='event',
                payload={**event_data, 'token': token},
                local_id=local_id
            )
        
        return {**event_data, '_pending': True}
    
    def get_user_events(self, token: str) -> List[Dict]:
        """
        Obtiene eventos del usuario (offline-first).
        
        Intenta obtener del servidor y actualiza caché.
        Si falla, retorna datos cacheados.
        """
        # Intentar obtener del servidor
        if self.is_online():
            try:
                events = self._api.get_user_events(token)
                
                # Actualizar caché
                if self._storage and events:
                    self._storage.cache_events(events)
                
                # Agregar eventos locales pendientes de sync
                if self._storage:
                    local_events = self._storage.get_dirty_events()
                    for local_event in local_events:
                        # Evitar duplicados
                        if not any(e.get('id') == local_event.get('_local_id') for e in events):
                            local_event['_pending'] = True
                            events.append(local_event)
                
                return events
                
            except Exception as e:
                logger.warning(f"Failed to get events online: {e}")
        
        # Retornar desde caché
        if self._storage:
            events = self._storage.get_cached_events()
            # Marcar eventos locales como pendientes
            for event in events:
                if event.get('_is_dirty'):
                    event['_pending'] = True
            return events
        
        return []
    
    def get_user_events_detailed(self, token: str, filter_type: str = "all") -> List[Dict]:
        """Obtiene eventos detallados (offline-first)."""
        if self.is_online():
            try:
                events = self._api.get_user_events_detailed(token, filter_type)
                
                if self._storage and events:
                    self._storage.cache_events(events)
                
                return events
                
            except Exception as e:
                logger.warning(f"Failed to get detailed events: {e}")
        
        # Retornar desde caché
        if self._storage:
            return self._storage.get_cached_events()
        
        return []
    
    def update_event(self, event_id: int, token: str, 
                     title: Optional[str] = None,
                     description: Optional[str] = None,
                     start_time: Optional[str] = None,
                     end_time: Optional[str] = None,
                     participants_ids: Optional[List[int]] = None) -> Dict:
        """Actualiza un evento (offline-first)."""
        update_data = {}
        if title is not None:
            update_data['title'] = title
        if description is not None:
            update_data['description'] = description
        if start_time is not None:
            update_data['start_time'] = start_time
        if end_time is not None:
            update_data['end_time'] = end_time
        if participants_ids is not None:
            update_data['participants_ids'] = participants_ids
        
        # Intentar actualizar online
        if self.is_online():
            try:
                result = self._api.update_event(event_id, token, **update_data)
                return result
            except Exception as e:
                logger.warning(f"Failed to update event online: {e}")
        
        # Modo offline: encolar operación
        if self._queue:
            self._queue.enqueue(
                operation=OperationType.UPDATE_EVENT.value,
                entity_type='event',
                payload={'event_id': event_id, 'token': token, **update_data},
                local_id=str(event_id)
            )
        
        return {'id': event_id, '_pending': True, **update_data}
    
    def cancel_event(self, event_id: int, token: str) -> Dict:
        """Cancela un evento (offline-first)."""
        if self.is_online():
            try:
                result = self._api.cancel_event(event_id, token)
                
                if self._storage:
                    self._storage.mark_event_deleted(str(event_id))
                
                return result
            except Exception as e:
                logger.warning(f"Failed to cancel event online: {e}")
        
        # Modo offline
        if self._storage:
            self._storage.mark_event_deleted(str(event_id))
        
        if self._queue:
            self._queue.enqueue(
                operation=OperationType.DELETE_EVENT.value,
                entity_type='event',
                payload={'event_id': event_id, 'token': token},
                local_id=str(event_id)
            )
        
        return {'deleted': True, '_pending': True}
    
    def leave_event(self, event_id: int, token: str) -> Dict:
        """Abandona un evento (offline-first)."""
        if self.is_online():
            try:
                return self._api.leave_event(event_id, token)
            except Exception as e:
                logger.warning(f"Failed to leave event online: {e}")
        
        if self._queue:
            self._queue.enqueue(
                operation=OperationType.LEAVE_EVENT.value,
                entity_type='event',
                payload={'event_id': event_id, 'token': token},
                local_id=str(event_id)
            )
        
        return {'left': True, '_pending': True}
    
    def respond_to_event_invitation(self, event_id: int, accepted: bool, 
                                     token: str) -> Dict:
        """Responde a una invitación de evento (offline-first)."""
        if self.is_online():
            try:
                return self._api.respond_to_event_invitation(event_id, accepted, token)
            except Exception as e:
                logger.warning(f"Failed to respond to event invitation: {e}")
        
        if self._queue:
            self._queue.enqueue(
                operation=OperationType.RESPOND_EVENT_INVITATION.value,
                entity_type='invitation',
                payload={'event_id': event_id, 'accepted': accepted, 'token': token}
            )
        
        return {'responded': True, '_pending': True}
    
    def get_pending_event_invitations(self, token: str) -> List[Dict]:
        """Obtiene invitaciones de evento pendientes (offline-first)."""
        if self.is_online():
            try:
                invitations = self._api.get_pending_event_invitations(token)
                
                if self._storage and invitations:
                    self._storage.cache_invitations(invitations, 'event')
                
                return invitations
            except Exception as e:
                logger.warning(f"Failed to get event invitations: {e}")
        
        if self._storage:
            return self._storage.get_cached_invitations('event')
        
        return []
    
    def get_pending_event_invitations_count(self, token: str) -> Dict:
        """Obtiene conteo de invitaciones de evento pendientes."""
        if self.is_online():
            try:
                return self._api.get_pending_event_invitations_count(token)
            except Exception:
                pass
        
        if self._storage:
            invitations = self._storage.get_cached_invitations('event')
            return {'count': len(invitations)}
        
        return {'count': 0}
    
    def get_event_details(self, event_id: int, token: str) -> Dict:
        """Obtiene detalles de un evento."""
        if self.is_online():
            try:
                return self._api.get_event_details(event_id, token)
            except Exception as e:
                logger.warning(f"Failed to get event details: {e}")
        
        # Buscar en caché
        if self._storage:
            events = self._storage.get_cached_events()
            for event in events:
                if event.get('id') == event_id:
                    return event
        
        return {}
    
    # =========================================================================
    # GRUPOS
    # =========================================================================
    
    def create_group(self, name: str, description: str, is_hierarchical: bool,
                     token: str, members: Optional[List[int]] = None) -> Dict:
        """Crea un grupo (offline-first)."""
        group_data = {
            'name': name,
            'description': description,
            'is_hierarchical': is_hierarchical,
            'members': members or [],
        }
        
        local_id = f"local_{uuid.uuid4().hex[:12]}"
        
        if self.is_online():
            try:
                result = self._api.create_group(name, description, is_hierarchical, token, members)
                
                if self._storage and result:
                    self._storage.cache_group(str(result.get('id', local_id)), result)
                
                return result
            except Exception as e:
                logger.warning(f"Failed to create group online: {e}")
        
        # Modo offline
        group_data['id'] = local_id
        group_data['_local_id'] = local_id
        group_data['_is_dirty'] = True
        group_data['creator_id'] = self.user_id
        
        if self._storage:
            self._storage.cache_group(local_id, group_data, is_dirty=True)
        
        if self._queue:
            self._queue.enqueue(
                operation=OperationType.CREATE_GROUP.value,
                entity_type='group',
                payload={**group_data, 'token': token},
                local_id=local_id
            )
        
        return {**group_data, '_pending': True}
    
    def list_user_groups(self, token: str) -> List[tuple]:
        """Lista grupos del usuario (offline-first)."""
        if self.is_online():
            try:
                groups = self._api.list_user_groups(token)
                
                # Convertir a dicts para cachear
                if self._storage and groups:
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
                
                return groups
            except Exception as e:
                logger.warning(f"Failed to list groups online: {e}")
        
        # Retornar desde caché
        if self._storage:
            cached = self._storage.get_cached_groups()
            return [(g.get('id'), g.get('name'), g.get('is_hierarchical', False)) 
                    for g in cached]
        
        return []
    
    def list_group_members(self, group_id: int, token: str) -> List[Dict]:
        """Lista miembros de un grupo (offline-first)."""
        if self.is_online():
            try:
                members = self._api.list_group_members(group_id, token)
                
                if self._storage and members:
                    self._storage.cache_group_members(str(group_id), members)
                
                return members
            except Exception as e:
                logger.warning(f"Failed to list group members: {e}")
        
        if self._storage:
            return self._storage.get_cached_group_members(str(group_id))
        
        return []
    
    def invite_user_to_group(self, group_id: int, invited_user_id: int, 
                             token: str) -> Dict:
        """Invita usuario a grupo (offline-first)."""
        if self.is_online():
            try:
                return self._api.invite_user_to_group(group_id, invited_user_id, token)
            except Exception as e:
                logger.warning(f"Failed to invite user online: {e}")
        
        if self._queue:
            self._queue.enqueue(
                operation=OperationType.INVITE_TO_GROUP.value,
                entity_type='invitation',
                payload={'group_id': group_id, 'invited_user_id': invited_user_id, 'token': token}
            )
        
        return {'invited': True, '_pending': True}
    
    def get_pending_invitations(self, token: str) -> List[Dict]:
        """Obtiene invitaciones de grupo pendientes (offline-first)."""
        if self.is_online():
            try:
                invitations = self._api.get_pending_invitations(token)
                
                if self._storage and invitations:
                    self._storage.cache_invitations(invitations, 'group')
                
                return invitations
            except Exception as e:
                logger.warning(f"Failed to get group invitations: {e}")
        
        if self._storage:
            return self._storage.get_cached_invitations('group')
        
        return []
    
    def respond_to_group_invitation(self, invitation_id: int, response: str, 
                                    token: str) -> Dict:
        """Responde a invitación de grupo (offline-first)."""
        if self.is_online():
            try:
                return self._api.respond_to_group_invitation(invitation_id, response, token)
            except Exception as e:
                logger.warning(f"Failed to respond to group invitation: {e}")
        
        if self._queue:
            self._queue.enqueue(
                operation=OperationType.RESPOND_GROUP_INVITATION.value,
                entity_type='invitation',
                payload={'invitation_id': invitation_id, 'response': response, 'token': token}
            )
        
        return {'responded': True, '_pending': True}
    
    def get_pending_invitations_count(self, token: str) -> Dict:
        """Obtiene conteo de invitaciones de grupo pendientes."""
        if self.is_online():
            try:
                return self._api.get_pending_invitations_count(token)
            except Exception:
                pass
        
        if self._storage:
            invitations = self._storage.get_cached_invitations('group')
            return {'count': len(invitations)}
        
        return {'count': 0}
    
    def update_group(self, group_id: int, name: Optional[str] = None,
                     description: Optional[str] = None, token: str = "") -> Dict:
        """Actualiza un grupo (offline-first)."""
        if self.is_online():
            try:
                return self._api.update_group(group_id, name, description, token)
            except Exception as e:
                logger.warning(f"Failed to update group online: {e}")
        
        if self._queue:
            self._queue.enqueue(
                operation=OperationType.UPDATE_GROUP.value,
                entity_type='group',
                payload={'group_id': group_id, 'name': name, 'description': description, 'token': token},
                local_id=str(group_id)
            )
        
        return {'id': group_id, '_pending': True}
    
    def delete_group(self, group_id: int, token: str) -> Dict:
        """Elimina un grupo (offline-first)."""
        if self.is_online():
            try:
                return self._api.delete_group(group_id, token)
            except Exception as e:
                logger.warning(f"Failed to delete group online: {e}")
        
        if self._queue:
            self._queue.enqueue(
                operation=OperationType.DELETE_GROUP.value,
                entity_type='group',
                payload={'group_id': group_id, 'token': token},
                local_id=str(group_id)
            )
        
        return {'deleted': True, '_pending': True}
    
    def remove_member(self, group_id: int, member_id: int, token: str) -> Dict:
        """Elimina miembro de grupo (offline-first)."""
        if self.is_online():
            try:
                return self._api.remove_member(group_id, member_id, token)
            except Exception as e:
                logger.warning(f"Failed to remove member online: {e}")
        
        if self._queue:
            self._queue.enqueue(
                operation=OperationType.REMOVE_GROUP_MEMBER.value,
                entity_type='group',
                payload={'group_id': group_id, 'member_id': member_id, 'token': token}
            )
        
        return {'removed': True, '_pending': True}
    
    def get_group_info(self, group_id: int, token: str) -> Dict:
        """Obtiene información de un grupo."""
        if self.is_online():
            try:
                return self._api.get_group_info(group_id, token)
            except Exception as e:
                logger.warning(f"Failed to get group info: {e}")
        
        if self._storage:
            groups = self._storage.get_cached_groups()
            for g in groups:
                if g.get('id') == group_id:
                    return g
        
        return {}
    
    # =========================================================================
    # USUARIOS
    # =========================================================================
    
    def list_users(self, token: str) -> List:
        """Lista usuarios (offline-first)."""
        if self.is_online():
            try:
                users = self._api.list_users(token)
                
                # Convertir a dicts si son tuplas
                if self._storage and users:
                    user_dicts = []
                    for u in users:
                        if isinstance(u, tuple):
                            user_dicts.append({'id': u[0], 'username': u[1]})
                        else:
                            user_dicts.append(u)
                    self._storage.cache_users(user_dicts)
                
                return users
            except Exception as e:
                logger.warning(f"Failed to list users online: {e}")
        
        if self._storage:
            cached = self._storage.get_cached_users()
            return [(u.get('id'), u.get('username')) for u in cached]
        
        return []
    
    # =========================================================================
    # VISUALIZACIÓN Y DISPONIBILIDAD
    # =========================================================================
    
    def get_group_agendas(self, group_id: int, start_date: str, end_date: str, 
                          token: str) -> Dict:
        """Obtiene agendas del grupo."""
        if self.is_online():
            try:
                return self._api.get_group_agendas(group_id, start_date, end_date, token)
            except Exception as e:
                logger.warning(f"Failed to get group agendas: {e}")
        
        return {'agendas': []}
    
    def get_common_availability(self, group_id: int, start_date: str, end_date: str,
                                duration_hours: float, token: str) -> Dict:
        """Obtiene disponibilidad común del grupo."""
        if self.is_online():
            try:
                return self._api.get_common_availability(
                    group_id, start_date, end_date, duration_hours, token
                )
            except Exception as e:
                logger.warning(f"Failed to get common availability: {e}")
        
        return {'slots': []}
    
    def get_event_conflicts(self, token: str, limit: int = 50) -> List:
        """Obtiene conflictos de eventos."""
        if self.is_online():
            try:
                return self._api.get_event_conflicts(token, limit)
            except Exception:
                pass
        
        return []
    
    # =========================================================================
    # SINCRONIZACIÓN
    # =========================================================================
    
    def sync_now(self, token: str = None) -> Dict:
        """
        Ejecuta sincronización inmediata.
        
        Returns:
            Resultado de la sincronización
        """
        if not self._sync:
            return {'success': False, 'error': 'Sync manager not initialized'}
        
        token = token or self._token
        if not token:
            return {'success': False, 'error': 'No token available'}
        
        result = self._sync.full_sync(token)
        return result.to_dict()
    
    def start_background_sync(self, token: str = None, interval: int = 30):
        """Inicia sincronización en segundo plano."""
        if self._sync:
            token = token or self._token
            if token:
                self._sync.start_background_sync(token, interval)
    
    def stop_background_sync(self):
        """Detiene sincronización en segundo plano."""
        if self._sync:
            self._sync.stop_background_sync()
    
    def get_sync_status(self) -> Dict:
        """Obtiene estado de sincronización."""
        if self._sync:
            return self._sync.get_sync_status()
        return {'connectivity': 'unknown', 'pending_operations': 0}
    
    def get_pending_operations_count(self) -> int:
        """Obtiene número de operaciones pendientes."""
        if self._queue:
            return self._queue.get_pending_count()
        return 0
    
    # =========================================================================
    # COMPATIBILIDAD CON APIClient ORIGINAL
    # =========================================================================
    
    def get_current_base_url(self):
        """Compatibilidad con APIClient."""
        return self._api.get_current_base_url()
    
    def get_current_ws_target(self):
        """Compatibilidad con APIClient."""
        return self._api.get_current_ws_target()
