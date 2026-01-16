"""
Componentes de UI para estado offline y sincronización.

Este módulo proporciona widgets de Streamlit para mostrar:
- Estado de conectividad
- Operaciones pendientes de sincronización  
- Conflictos por resolver
- Progreso de sincronización
"""

import streamlit as st
from datetime import datetime
from typing import Optional, List, Dict, Any


def render_connectivity_badge(is_online: bool, state: str = None) -> None:
    """
    Renderiza un badge de estado de conectividad en el sidebar.
    
    Args:
        is_online: True si hay conexión
        state: Estado específico ('online', 'offline', 'degraded')
    """
    if state == 'degraded':
        st.sidebar.warning("⚠️ Conexión inestable")
    elif is_online:
        st.sidebar.success("🟢 Conectado")
    else:
        st.sidebar.error("🔴 Sin conexión (modo offline)")


def render_sync_status(pending_count: int, 
                       last_sync: Optional[datetime] = None,
                       is_syncing: bool = False) -> None:
    """
    Renderiza el estado de sincronización en el sidebar.
    
    Args:
        pending_count: Número de operaciones pendientes
        last_sync: Última sincronización exitosa
        is_syncing: Si hay una sincronización en progreso
    """
    with st.sidebar.expander("🔄 Sincronización", expanded=pending_count > 0):
        if is_syncing:
            st.info("⏳ Sincronizando...")
        
        if pending_count > 0:
            st.warning(f"📤 {pending_count} operación(es) pendiente(s)")
        else:
            st.success("✅ Todo sincronizado")
        
        if last_sync:
            if isinstance(last_sync, str):
                last_sync = datetime.fromisoformat(last_sync)
            time_ago = _format_time_ago(last_sync)
            st.caption(f"Última sync: {time_ago}")


def render_pending_banner(pending_count: int, on_sync_click=None) -> None:
    """
    Renderiza un banner de operaciones pendientes en la parte superior.
    
    Args:
        pending_count: Número de operaciones pendientes
        on_sync_click: Callback cuando se hace click en sincronizar
    """
    if pending_count == 0:
        return
    
    col1, col2 = st.columns([4, 1])
    with col1:
        st.warning(f"📤 Tienes {pending_count} cambio(s) pendiente(s) de sincronizar")
    with col2:
        if st.button("🔄 Sincronizar ahora", key="sync_now_btn"):
            if on_sync_click:
                on_sync_click()


def render_offline_banner() -> None:
    """Renderiza un banner cuando está en modo offline."""
    st.info("📡 Estás trabajando en modo offline. Los cambios se sincronizarán cuando vuelvas a estar conectado.")


def render_conflict_list(conflicts: List[Dict], 
                         on_resolve_local=None,
                         on_resolve_remote=None) -> None:
    """
    Renderiza la lista de conflictos pendientes.
    
    Args:
        conflicts: Lista de conflictos
        on_resolve_local: Callback para resolver con datos locales
        on_resolve_remote: Callback para resolver con datos del servidor
    """
    if not conflicts:
        return
    
    st.warning(f"⚠️ {len(conflicts)} conflicto(s) detectado(s)")
    
    for i, conflict in enumerate(conflicts):
        with st.expander(f"Conflicto: {conflict.get('entity_type', 'Unknown')} - {conflict.get('entity_id', '')}"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📱 Tu versión")
                local_data = conflict.get('local_data', {})
                _render_data_preview(local_data)
            
            with col2:
                st.subheader("☁️ Versión del servidor")
                remote_data = conflict.get('remote_data', {})
                _render_data_preview(remote_data)
            
            st.divider()
            
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                if st.button("Usar mi versión", key=f"use_local_{i}"):
                    if on_resolve_local:
                        on_resolve_local(conflict.get('id'))
            with col_b:
                if st.button("Usar versión del servidor", key=f"use_remote_{i}"):
                    if on_resolve_remote:
                        on_resolve_remote(conflict.get('id'))
            with col_c:
                if st.button("Ignorar", key=f"ignore_{i}"):
                    pass  # Se ignora el conflicto


def render_pending_operations_modal(operations: List[Dict]) -> None:
    """
    Renderiza un modal/expander con las operaciones pendientes.
    
    Args:
        operations: Lista de operaciones pendientes
    """
    if not operations:
        return
    
    with st.expander(f"📋 Ver operaciones pendientes ({len(operations)})"):
        for op in operations:
            op_type = op.get('operation', 'unknown')
            entity = op.get('entity_type', '')
            created_at = op.get('created_at', '')
            status = op.get('status', 'pending')
            
            icon = _get_operation_icon(op_type)
            status_icon = "🔄" if status == 'pending' else "❌" if status == 'failed' else "✅"
            
            st.text(f"{status_icon} {icon} {op_type} ({entity}) - {_format_datetime(created_at)}")
            
            if status == 'failed' and op.get('last_error'):
                st.caption(f"   Error: {op.get('last_error')}")


def render_sync_progress(current: int, total: int, message: str = "") -> None:
    """
    Renderiza una barra de progreso de sincronización.
    
    Args:
        current: Operaciones completadas
        total: Total de operaciones
        message: Mensaje adicional
    """
    if total == 0:
        return
    
    progress = current / total
    st.progress(progress, text=message or f"Sincronizando... {current}/{total}")


def render_data_status_indicator(is_dirty: bool = False,
                                  is_synced: bool = True,
                                  is_local_only: bool = False) -> str:
    """
    Retorna un indicador visual del estado de los datos.
    
    Args:
        is_dirty: Tiene cambios locales sin sincronizar
        is_synced: Está sincronizado con el servidor
        is_local_only: Solo existe localmente
        
    Returns:
        String con emoji indicador
    """
    if is_local_only:
        return "📝"  # Solo local
    if is_dirty:
        return "🔄"  # Pendiente de sync
    if is_synced:
        return ""    # Normal, no mostrar nada
    return "⚠️"      # Estado desconocido


def show_offline_login_option(on_offline_login=None) -> None:
    """
    Muestra la opción de login offline cuando no hay conexión.
    
    Args:
        on_offline_login: Callback para intentar login offline
    """
    st.info("💡 ¿Ya iniciaste sesión antes? Puedes acceder a tus datos offline.")
    if st.button("🔓 Acceder modo offline"):
        if on_offline_login:
            on_offline_login()


def render_last_update_time(entity_type: str, last_sync: Optional[datetime]) -> None:
    """
    Muestra cuándo se actualizaron los datos por última vez.
    
    Args:
        entity_type: Tipo de entidad ("eventos", "grupos", etc.)
        last_sync: Última sincronización
    """
    if last_sync:
        if isinstance(last_sync, str):
            last_sync = datetime.fromisoformat(last_sync)
        time_ago = _format_time_ago(last_sync)
        st.caption(f"📅 {entity_type.capitalize()} actualizado(s): {time_ago}")
    else:
        st.caption(f"📅 {entity_type.capitalize()}: datos locales")


# =============================================================================
# HELPERS
# =============================================================================

def _format_time_ago(dt: datetime) -> str:
    """Formatea un datetime como tiempo relativo."""
    now = datetime.now()
    diff = now - dt
    
    seconds = diff.total_seconds()
    
    if seconds < 60:
        return "hace un momento"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        return f"hace {minutes} min"
    elif seconds < 86400:
        hours = int(seconds / 3600)
        return f"hace {hours} hora(s)"
    else:
        days = int(seconds / 86400)
        return f"hace {days} día(s)"


def _format_datetime(dt_str: str) -> str:
    """Formatea un string de datetime."""
    try:
        dt = datetime.fromisoformat(dt_str)
        return dt.strftime("%d/%m %H:%M")
    except Exception:
        return dt_str


def _get_operation_icon(op_type: str) -> str:
    """Retorna un icono para el tipo de operación."""
    icons = {
        'create_event': '📅',
        'update_event': '✏️',
        'delete_event': '🗑️',
        'create_group': '👥',
        'update_group': '✏️',
        'delete_group': '🗑️',
        'invite_to_group': '📨',
        'respond_group_invitation': '📩',
        'respond_event_invitation': '📩',
        'leave_event': '🚪',
    }
    return icons.get(op_type, '📋')


def _render_data_preview(data: Dict) -> None:
    """Renderiza una preview de datos."""
    if not data:
        st.text("(sin datos)")
        return
    
    # Mostrar campos principales
    display_fields = ['title', 'name', 'description', 'start_time', 'end_time']
    for field in display_fields:
        if field in data and data[field]:
            value = data[field]
            if len(str(value)) > 50:
                value = str(value)[:50] + "..."
            st.text(f"{field}: {value}")


# =============================================================================
# SESSION STATE HELPERS  
# =============================================================================

def init_offline_state():
    """Inicializa el estado offline en session_state."""
    defaults = {
        'offline_mode': False,
        'pending_sync_count': 0,
        'last_sync_time': None,
        'conflicts': [],
        'sync_in_progress': False,
        'connectivity_state': 'unknown',
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def update_offline_state(sync_manager=None, conflict_resolver=None, queue=None):
    """
    Actualiza el estado offline desde los managers.
    
    Args:
        sync_manager: Instancia de SyncManager
        conflict_resolver: Instancia de ConflictResolver
        queue: Instancia de PendingOperationsQueue
    """
    if sync_manager:
        status = sync_manager.get_sync_status()
        st.session_state.connectivity_state = status.get('connectivity', 'unknown')
        st.session_state.offline_mode = status['connectivity'] == 'offline'
        st.session_state.pending_sync_count = status.get('pending_operations', 0)
        st.session_state.last_sync_time = status.get('last_sync')
        st.session_state.sync_in_progress = status.get('is_syncing', False)
    
    if conflict_resolver:
        pending_conflicts = conflict_resolver.get_pending_conflicts()
        st.session_state.conflicts = [c.to_dict() for c in pending_conflicts]
    
    if queue:
        st.session_state.pending_sync_count = queue.get_pending_count()


def get_offline_state() -> Dict[str, Any]:
    """Retorna el estado offline actual."""
    return {
        'offline_mode': st.session_state.get('offline_mode', False),
        'pending_sync_count': st.session_state.get('pending_sync_count', 0),
        'last_sync_time': st.session_state.get('last_sync_time'),
        'conflicts': st.session_state.get('conflicts', []),
        'sync_in_progress': st.session_state.get('sync_in_progress', False),
        'connectivity_state': st.session_state.get('connectivity_state', 'unknown'),
    }
