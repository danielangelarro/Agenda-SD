import streamlit as st
from datetime import datetime
import asyncio

def show_events_view(user_id, api_client, token):
    """Vista principal de eventos con pestañas para crear y listar eventos"""
    st.header("📋 Mis Eventos")
    st.session_state.setdefault("removed_event_ids", [])
    
    # Tabs para crear evento y ver lista de eventos
    tab1, tab2 = st.tabs(["➕ Crear Evento", "📋 Lista de Eventos"])
    
    with tab1:
        show_create_event_view(user_id, api_client, token)
    
    with tab2:
        show_events_list_view(user_id, api_client, token)


def show_events_list_view(user_id, api_client, token):
    """Vista de lista de eventos con filtros y detalles"""
    # Filtros
    st.subheader("🔍 Filtrar eventos")
    col1, col2 = st.columns(2)

    with col1:
        filter_type = st.selectbox(
            "Mostrar:",
            ["Todos", "Próximos", "Pasados", "Pendientes de aceptar", "Creados por mí"],
            key="event_filter"
        )

    with col2:
        search_text = st.text_input("🔎 Buscar por título", key="search_text")

    # Mapear filtro seleccionado
    filter_map = {
        "Todos": "all",
        "Próximos": "upcoming",
        "Pasados": "past",
        "Pendientes de aceptar": "pending",
        "Creados por mí": "created"
    }

    # Obtener eventos filtrados
    try:
        events = api_client.get_user_events_detailed(token, filter_map[filter_type])
        if st.session_state["removed_event_ids"]:
            events = [e for e in events if str(e.get("id")) not in st.session_state["removed_event_ids"]]

        # Aplicar búsqueda por texto
        if search_text:
            events = [e for e in events if search_text.lower() in e['title'].lower()]

        # Mostrar estadísticas
        st.markdown("---")
        col_stat1, col_stat2, col_stat3 = st.columns(3)
        with col_stat1:
            st.metric("Total eventos", len(events))
        with col_stat2:
            pending_count = len([e for e in events if e['is_accepted'] == 0 and not e['is_creator']])
            st.metric("Pendientes", pending_count)
        with col_stat3:
            created_count = len([e for e in events if e['is_creator']])
            st.metric("Creados por ti", created_count)

        st.markdown("---")

        # Mostrar lista de eventos
        if not events:
            st.info("No hay eventos para mostrar con los filtros seleccionados")
        else:
            for event in events:
                show_event_card(event, user_id, api_client, token)
    except Exception as e:
        st.error(f"Error al cargar eventos: {str(e)}")


def show_event_card(event, user_id, api_client, token):
    """Mostrar tarjeta de evento con detalles y acciones"""
    # Determinar el estado del evento
    try:
        event_date = datetime.strptime(event['start_time'], '%Y-%m-%d %H:%M:%S')
        is_past = event_date < datetime.now()
    except:
        is_past = False

    # Determinar el color/estado del evento
    if event['is_creator']:
        status_badge = "🔵 Creado por ti"
        status_color = "blue"
    elif event['is_accepted'] == 0:
        status_badge = "🟡 Pendiente de aceptar"
        status_color = "orange"
    elif is_past:
        status_badge = "🔴 Evento pasado"
        status_color = "red"
    else:
        status_badge = "🟢 Confirmado"
        status_color = "green"

    # Contenedor del evento
    with st.container():
        # Header del evento
        col_title, col_status = st.columns([3, 1])
        with col_title:
            st.markdown(f"### {event['title']}")
        with col_status:
            st.markdown(f"**{status_badge}**")

        # Información básica
        col_info1, col_info2 = st.columns(2)
        with col_info1:
            st.write(f"📅 **Inicio:** {event['start_time']}")
            st.write(f"⏰ **Fin:** {event['end_time']}")
        with col_info2:
            st.write(f"👤 **Creador:** {event['creator_name']}")
            if event['is_group_event'] and event['group_name']:
                st.write(f"👥 **Grupo:** {event['group_name']}")

        # Descripción
        if event['description']:
            with st.expander("📝 Ver descripción"):
                st.write(event['description'])

        # Botón para ver detalles completos
        col_actions1, col_actions2, col_actions3 = st.columns(3)

        with col_actions1:
            if st.button(f"ℹ️ Ver detalles", key=f"details_{event['id']}"):
                st.session_state[f'show_details_{event["id"]}'] = True

        # Acciones según el rol del usuario
        if event['is_creator']:
            with col_actions2:
                if st.button(f"❌ Cancelar evento", key=f"cancel_{event['id']}"):
                    st.session_state[f'confirm_cancel_{event["id"]}'] = True

            with col_actions3:
                if st.button("✏️ Replanificar", key=f"edit_{event['id']}"):
                    st.session_state[f'editing_event_{event["id"]}'] = True

            # Confirmación de cancelación
            if st.session_state.get(f'confirm_cancel_{event["id"]}', False):
                st.warning("⚠️ ¿Estás seguro de que quieres cancelar este evento?")
                col_yes, col_no = st.columns(2)
                with col_yes:
                    if st.button("Sí, cancelar", key=f"yes_cancel_{event['id']}"):
                        try:
                            response = api_client.cancel_event(event['id'], token)
                            st.success(response.get('message', 'Evento cancelado exitosamente'))
                            st.session_state[f'confirm_cancel_{event["id"]}'] = False
                            if str(event["id"]) not in st.session_state["removed_event_ids"]:
                                st.session_state["removed_event_ids"].append(str(event["id"]))
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al cancelar el evento: {str(e)}")
                            st.session_state[f'confirm_cancel_{event["id"]}'] = False
                with col_no:
                    if st.button("No, mantener", key=f"no_cancel_{event['id']}"):
                        st.session_state[f'confirm_cancel_{event["id"]}'] = False
                        st.rerun()

        else:
            # Participante puede salir del evento
            with col_actions2:
                if st.button(f"🚪 Salir del evento", key=f"leave_{event['id']}"):
                    st.session_state[f'confirm_leave_{event["id"]}'] = True

            # Confirmación de salida
            if st.session_state.get(f'confirm_leave_{event["id"]}', False):
                st.warning("⚠️ ¿Estás seguro de que quieres salir de este evento?")
                col_yes, col_no = st.columns(2)
                with col_yes:
                    if st.button("Sí, salir", key=f"yes_leave_{event['id']}"):
                        try:
                            response = api_client.leave_event(event['id'], token)
                            st.success(response.get('message', 'Has salido del evento'))
                            st.session_state[f'confirm_leave_{event["id"]}'] = False
                            if str(event["id"]) not in st.session_state["removed_event_ids"]:
                                st.session_state["removed_event_ids"].append(str(event["id"]))
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al salir del evento: {str(e)}")
                            st.session_state[f'confirm_leave_{event["id"]}'] = False
                with col_no:
                    if st.button("No, quedarme", key=f"no_leave_{event['id']}"):
                        st.session_state[f'confirm_leave_{event["id"]}'] = False
                        st.rerun()

        # Mostrar detalles completos si se solicita
        if st.session_state.get(f'show_details_{event["id"]}', False):
            show_event_details(event['id'], user_id, api_client, token)

        # Mostrar formulario de edición si se solicita (solo creador)
        if event['is_creator'] and st.session_state.get(f'editing_event_{event["id"]}', False):
            show_event_edit_form(event, api_client, token)

        st.markdown("---")


def show_event_details(event_id, user_id, api_client, token):
    """Mostrar detalles completos del evento incluyendo participantes"""
    try:
        event_details = api_client.get_event_details(event_id, token)
        
        st.subheader(f"Detalles del evento: {event_details['title']}")
        
        # Información básica del evento
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"📅 **Inicio:** {event_details['start_time']}")
            st.write(f"⏰ **Fin:** {event_details['end_time']}")
            st.write(f"👤 **Creador:** {event_details['creator_name']}")
            
        with col2:
            if event_details['is_group_event'] and event_details['group_name']:
                st.write(f"👥 **Grupo:** {event_details['group_name']}")
            if event_details['description']:
                st.write(f"📝 **Descripción:** {event_details['description']}")
        
        # Mostrar participantes
        st.markdown("---")
        st.subheader("👥 Participantes")
        
        if event_details['participants']:
            # Separar participantes aceptados y pendientes
            accepted_participants = [p for p in event_details['participants'] if p['is_accepted']]
            pending_participants = [p for p in event_details['participants'] if not p['is_accepted']]
            
            # Mostrar participantes aceptados
            if accepted_participants:
                st.markdown("**✅ Aceptados:**")
                for participant in accepted_participants:
                    badge = "👑" if participant['user_id'] == event_details['creator_id'] else "👤"
                    st.write(f"- {badge} {participant['username']}")
            
            # Mostrar participantes pendientes
            if pending_participants:
                st.markdown("**⏳ Pendientes:**")
                for participant in pending_participants:
                    st.write(f"- ⏳ {participant['username']}")
        else:
            st.info("No hay participantes en este evento")
        
        # Botón para cerrar detalles
        if st.button("Cerrar detalles", key=f"close_details_{event_id}"):
            st.session_state[f'show_details_{event_id}'] = False
            st.rerun()
            
    except Exception as e:
        st.error(f"Error al cargar detalles del evento: {str(e)}")
        # Botón para cerrar detalles
        if st.button("Cerrar detalles", key=f"close_details_{event_id}_error"):
            st.session_state[f'show_details_{event_id}'] = False
            st.rerun()


def show_event_edit_form(event, api_client, token):
    """Formulario de edición/replanificación (solo creador)."""
    event_id = int(event["id"])
    st.subheader("✏️ Replanificar / editar evento")
    details = None
    try:
        details = api_client.get_event_details(event_id, token)
    except Exception:
        # Fallback: permitir editar usando la info de la lista si el endpoint de detalles falla.
        details = {
            "title": event.get("title"),
            "description": event.get("description"),
            "start_time": event.get("start_time"),
            "end_time": event.get("end_time"),
            "is_hierarchical_event": False,
        }
        st.warning("No se pudieron cargar los detalles completos; se usará la información disponible.")

    try:
        start_dt = datetime.strptime(details["start_time"], "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(details["end_time"], "%Y-%m-%d %H:%M:%S")
    except Exception:
        now = datetime.now()
        start_dt = now.replace(minute=0, second=0, microsecond=0)
        end_dt = start_dt

    title = st.text_input("Título", value=details.get("title") or "", key=f"edit_title_{event_id}")
    description = st.text_area("Descripción", value=details.get("description") or "", key=f"edit_desc_{event_id}")

    if details.get("is_hierarchical_event"):
        st.info("Este es un evento jerárquico: al reprogramarlo se aplica a todo el grupo; si hay conflictos, se registran.")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Fecha inicio",
            value=start_dt.date() if start_dt else None,
            key=f"edit_start_date_{event_id}",
        )
        start_time = st.time_input(
            "Hora inicio",
            value=start_dt.time() if start_dt else None,
            key=f"edit_start_time_{event_id}",
        )
    with col2:
        end_date = st.date_input(
            "Fecha fin",
            value=end_dt.date() if end_dt else None,
            key=f"edit_end_date_{event_id}",
        )
        end_time = st.time_input(
            "Hora fin",
            value=end_dt.time() if end_dt else None,
            key=f"edit_end_time_{event_id}",
        )

    start_str = f"{start_date} {start_time}"
    end_str = f"{end_date} {end_time}"

    col_save, col_close = st.columns(2)
    with col_save:
        if st.button("💾 Guardar cambios", key=f"save_edit_{event_id}"):
            try:
                start_norm = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                end_norm = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                st.error("Formato de fecha/hora inválido")
                return

            try:
                resp = api_client.update_event(
                    event_id,
                    token=token,
                    title=title,
                    description=description,
                    start_time=start_norm,
                    end_time=end_norm,
                )
                st.success(resp.get("message", "Evento actualizado"))
                st.session_state[f'editing_event_{event_id}'] = False
                st.rerun()
            except Exception as e:
                st.error(f"Error al actualizar el evento: {e}")

    with col_close:
        if st.button("Cerrar edición", key=f"close_edit_{event_id}"):
            st.session_state[f'editing_event_{event_id}'] = False
            st.rerun()


def show_create_event_view(user_id, api_client, token):
    st.header("➕ Crear evento")

    # Verificar si hay datos pre-llenados desde horarios disponibles
    prefill_start = st.session_state.pop('prefill_start', None)
    prefill_end = st.session_state.pop('prefill_end', None)
    prefill_group_id = st.session_state.pop('prefill_group_id', None)

    # Valores por defecto
    default_start_date = None
    default_start_time = None
    default_end_date = None
    default_end_time = None

    if prefill_start and prefill_end:
        try:
            start_dt = datetime.strptime(prefill_start, '%Y-%m-%d %H:%M:%S')
            end_dt = datetime.strptime(prefill_end, '%Y-%m-%d %H:%M:%S')
            default_start_date = start_dt.date()
            default_start_time = start_dt.time()
            default_end_date = end_dt.date()
            default_end_time = end_dt.time()
            st.info(f"📅 Horario seleccionado: {prefill_start} ➡️ {prefill_end}")
        except ValueError:
            pass

    title = st.text_input("Título")
    description = st.text_area("Descripción")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Fecha inicio", value=default_start_date)
        start_time = st.time_input("Hora inicio", value=default_start_time)
    with col2:
        end_date = st.date_input("Fecha fin", value=default_end_date)
        end_time = st.time_input("Hora fin", value=default_end_time)

    start_str = f"{start_date} {start_time}"
    end_str = f"{end_date} {end_time}"

    is_group_event = st.checkbox("Evento grupal", value=prefill_group_id is not None)

    # NUEVO: Opción para evento jerárquico
    is_hierarchical = False
    participants_ids = []
    group_id = None

    if is_group_event:
        try:
            groups = api_client.list_user_groups(token)
            if groups:
                group_names = [g[1] for g in groups]

                # Pre-seleccionar grupo si viene de horarios disponibles
                default_index = 0
                if prefill_group_id:
                    try:
                        default_index = [g[0] for g in groups].index(prefill_group_id)
                    except ValueError:
                        pass

                selected_group_name = st.selectbox("Selecciona grupo", group_names, index=default_index)
                group_id = [g[0] for g in groups if g[1] == selected_group_name][0]

                # Opción jerárquica (solo si el grupo es jerárquico y el usuario es el creador/líder)
                group_info = api_client.get_group_info(group_id, token)
                can_hierarchical = bool(group_info.get("is_hierarchical")) and int(group_info.get("creator_id")) == int(user_id)
                if can_hierarchical:
                    is_hierarchical = st.checkbox(
                        "Evento jerárquico (obligatorio para todos)",
                        value=False,
                        help="Si lo activas, el evento se agrega automáticamente a todos los miembros del grupo. Si hay conflictos, se registran.",
                    )

                members = api_client.list_group_members(group_id, token)
                participants_ids = [m[0] for m in members]

                st.info(f"Participantes: {', '.join([m[1] for m in members])}")
            else:
                st.warning("No tienes grupos")
        except Exception as e:
            st.error(f"Error al cargar grupos: {str(e)}")
    else:
        try:
            users = api_client.list_users(token)
            options = {u[1]: u[0] for u in users if u[0] != user_id}
            selected = st.multiselect("Invitar usuarios", list(options.keys()))
            participants_ids = [options[s] for s in selected]
        except Exception as e:
            st.error(f"Error al cargar usuarios: {str(e)}")

    if st.button("Crear evento"):
        try:
            datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
            datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            st.error("Formato de fecha/hora inválido")
            return

        try:
            # Create event using API
            result = api_client.create_event(
                title, description, start_str, end_str,
                token, group_id, is_group_event, 
                participants_ids, is_hierarchical
            )
            
            st.success("✅ Evento creado")
            if is_hierarchical:
                st.success("🔔 Notificaciones enviadas a todos los miembros del grupo")
            st.balloons()
            st.rerun()
        except Exception as e:
            st.error(f"❌ Error al crear evento: {str(e)}")
