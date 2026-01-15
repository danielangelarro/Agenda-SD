import streamlit as st
import asyncio
import threading
import os
import time
import warnings
import logging
from ui.login_view import show_login_page
from ui.calendar_view import show_calendar_view
from ui.event_view import show_events_view
from ui.group_view import show_groups_view
from ui.invitations_view import show_invitations_view
from ui.notifications_view import show_notifications_view, ensure_notification_state
from services.api_client import APIClient
from services.websocket_client import WebSocketClient

# Suppress asyncio warnings about unclosed resources
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*coroutine.*was never awaited.*')
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*Event loop is closed.*')
warnings.filterwarnings('ignore', category=ResourceWarning)

# Configure logging to suppress certain messages
logging.getLogger('asyncio').setLevel(logging.ERROR)

# Configuración de la página
st.set_page_config(
    page_title="Agenda Distribuida",
    page_icon="📅",
    layout="wide"
)

# Initialize API client
api_client = APIClient()

def get_ws_client() -> WebSocketClient:
    """WebSocketClient por sesión (evita que pestañas/usuarios se pisen)."""
    if "ws_client" not in st.session_state:
        st.session_state.ws_client = WebSocketClient()
    return st.session_state.ws_client

def _notification_handler(data: dict):
    if "notifications" not in st.session_state:
        st.session_state.notifications = []
    text = f"📢 {data.get('type', 'Notificación')}: {data.get('message', 'Nueva notificación')}"
    st.session_state.notifications.append(text)

def register_ws_handlers(ws_client: WebSocketClient, user_id: int):
    """Registrar handlers una sola vez por sesión."""
    key = f"ws_handlers_registered_{user_id}"
    if st.session_state.get(key):
        return
    for t in [
        "event_invitation",
        "group_event_invitation",
        "event_reminder",
        "event_accepted",
        "event_declined",
        "event_rescheduled",
        "event_updated",
        "event_removed",
        "event_cancelled",
        "participant_left",
        "new_group_member",
        "removed_from_group",
        "group_deleted",
        "hierarchical_event_added",
        "hierarchical_event_updated",
        "member_conflict",
        "group_invitation",
    ]:
        ws_client.register_handler(t, _notification_handler)
    st.session_state[key] = True

def restore_session():
    """Restaurar sesión desde query params o localStorage"""
    # Intentar obtener token de los query params
    if 'session_token' in st.query_params:
        token = st.query_params['session_token']
        try:
            # Validate token by making a simple API call
            users = api_client.list_users(token)
            
            # Get user_id from query params if available
            if 'user_id' in st.query_params:
                try:
                    user_id = int(st.query_params['user_id'])
                    # Find username for this user_id
                    username = None
                    for user in users:
                        if user[0] == user_id:  # user[0] is user_id
                            username = user[1]  # user[1] is username
                            break
                    
                    if username:
                        # Restore complete session state
                        st.session_state.logged_in = True
                        st.session_state.session_token = token
                        st.session_state.user_id = user_id
                        st.session_state.username = username
                        st.session_state.websocket_connected = False
                        return True
                except (ValueError, IndexError):
                    pass
            
            # If we couldn't restore user_id from query params, token is still valid
            # but we need to get user info from somewhere else
            st.session_state.logged_in = True
            st.session_state.session_token = token
            st.session_state.websocket_connected = False
            return True
        except:
            # Token is invalid, remove it
            if 'session_token' in st.query_params:
                del st.query_params['session_token']
            if 'user_id' in st.query_params:
                del st.query_params['user_id']
            return False

    return False

def main():
    # Inicializar estado de sesión
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False

    ensure_notification_state()

    # Intentar restaurar sesión si no está logueado
    if not st.session_state.logged_in:
        restore_session()

    if not st.session_state.logged_in:
        show_login_page(api_client)
    else:
        ws_client = get_ws_client()
        # Make sure username is in session state
        if 'username' not in st.session_state:
            # Try to get username from API
            try:
                users = api_client.list_users(st.session_state.session_token)
                # Find current user ID
                for user in users:
                    if user[0] == st.session_state.user_id:  # user[0] is user_id
                        st.session_state.username = user[1]  # user[1] is username
                        break
            except Exception as e:
                st.error(f"Error getting user info: {e}")
                st.session_state.logged_in = False
                st.rerun()

        # Sidebar con información de conexión
        st.sidebar.title(f"👋 Hola, {st.session_state.username}")

        # Mostrar estado de conexión WebSocket
        # Ajustar WS al coordinador activo según API
        try:
            base_url = api_client.get_current_base_url()
            if base_url:
                ws_host, ws_port = api_client.get_current_ws_target()
                ws_client.configure_from_base(base_url, ws_port)
        except Exception:
            pass

        st.sidebar.info(f"🌐 Conectado a: {ws_client.url}")

        # Get user ID from token if not in session state
        if 'user_id' not in st.session_state:
            try:
                users = api_client.list_users(st.session_state.session_token)
                # Find current user ID
                for user in users:
                    if user[1] == st.session_state.username:  # user[1] is username
                        st.session_state.user_id = user[0]  # user[0] is user_id
                        break
            except Exception as e:
                st.error(f"Error getting user info: {e}")
                st.session_state.logged_in = False
                st.rerun()

        # WebSocket por sesión: inicia background y procesa mensajes pendientes en cada rerun.
        register_ws_handlers(ws_client, int(st.session_state.user_id))
        ws_client.start_background(int(st.session_state.user_id), st.session_state.session_token)
        ws_client.dispatch_pending(max_items=200)
        st.session_state.websocket_connected = bool(ws_client.connected)

        # Obtener conteos de invitaciones pendientes
        try:
            groups_count_data = api_client.get_pending_invitations_count(st.session_state.session_token)
            events_count_data = api_client.get_pending_event_invitations_count(st.session_state.session_token)
            groups_count = groups_count_data.get("count", 0)
            events_count = events_count_data.get("count", 0)
            total_invitations = groups_count + events_count
        except Exception as e:
            st.error(f"Error getting invitation counts: {e}")
            groups_count = 0
            events_count = 0
            total_invitations = 0

        # Guardar conteo anterior para detectar cambios
        if 'previous_invitations_count' not in st.session_state:
            st.session_state.previous_invitations_count = total_invitations

        # Auto-refresh: Si hay cambios en invitaciones, actualizar
        if st.session_state.previous_invitations_count != total_invitations:
            st.session_state.previous_invitations_count = total_invitations

        # Construir etiquetas con badges
        invitations_label = f"📧 Invitaciones ({total_invitations})" if total_invitations > 0 else "📧 Invitaciones"

        # Verificar si hay una vista específica solicitada
        if 'current_view' in st.session_state:
            requested_view = st.session_state.pop('current_view')
            if requested_view == 'events':
                default_page = "📋 Eventos"
            else:
                default_page = "📅 Calendario"
        else:
            default_page = "📅 Calendario"

        # Navegación
        page = st.sidebar.radio(
            "Navegación",
            ["📅 Calendario", "📋 Eventos", "👥 Grupos", invitations_label, "🔔 Notificaciones"],
            index=["📅 Calendario", "📋 Eventos", "👥 Grupos", invitations_label, "🔔 Notificaciones"].index(default_page) if default_page in ["📅 Calendario", "📋 Eventos", "👥 Grupos", invitations_label, "🔔 Notificaciones"] else 0
        )
        
        if st.sidebar.button("🚪 Cerrar sesión"):
            # Detener WS background (por sesión)
            try:
                ws_client.stop_background()
            except Exception:
                pass
            
            # Limpiar query params
            if 'session_token' in st.query_params:
                del st.query_params['session_token']
            if 'user_id' in st.query_params:
                del st.query_params['user_id']

            # Limpiar session state
            for key in list(st.session_state.keys()):
                del st.session_state[key]

            st.rerun()
        
        # Mostrar página seleccionada
        if page == "📅 Calendario":
            show_calendar_view(st.session_state.user_id, api_client, st.session_state.session_token)
        elif page == "📋 Eventos":
            show_events_view(st.session_state.user_id, api_client, st.session_state.session_token)
        elif page == "👥 Grupos":
            show_groups_view(st.session_state.user_id, api_client, st.session_state.session_token)
        elif page.startswith("📧 Invitaciones"):  # Maneja tanto con badge como sin badge
            show_invitations_view(st.session_state.user_id, api_client, st.session_state.session_token)
        elif page == "🔔 Notificaciones":
            show_notifications_view(st.session_state.user_id, api_client, st.session_state.session_token, ws_client)

# Iniciar WebSocket solo si no está ya corriendo
if __name__ == "__main__":
    # Usar un archivo de bandera global en lugar de session_state
    # porque session_state se reinicia en cada recarga de Streamlit
    import socket

    def is_port_in_use(port):
        """Verificar si un puerto está en uso"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('0.0.0.0', port))
                return False
            except OSError:
                return True

    # Solo intentar iniciar el WebSocket si el puerto 8767 está libre
    # y no estamos en Docker
    if os.getenv('DOCKER_ENV') != 'true':
        websocket_port = int(os.getenv('WEBSOCKET_PORT', '8767'))
        if not is_port_in_use(websocket_port):
            # Note: We're keeping the WebSocket server in the client for now
            # In a production environment, this would be in the server
            pass

    main()
