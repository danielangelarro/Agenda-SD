import streamlit as st
import asyncio
from datetime import datetime, timedelta

def show_groups_view(user_id, api_client, token):
    st.header("👥 Gestión de Grupos")

    # Crear tabs para separar creación y listado
    tab1, tab2 = st.tabs(["📋 Mis Grupos", "➕ Crear Grupo"])
    
    # --- TAB 1: Listar grupos ---
    with tab1:
        show_groups_list(user_id, api_client, token)
    
    # --- TAB 2: Crear nuevo grupo ---
    with tab2:
        show_create_group_form(user_id, api_client, token)

def show_create_group_form(user_id, api_client, token):
    """Formulario para crear un nuevo grupo"""
    st.subheader("➕ Crear nuevo grupo")
    
    # Limpiar estado si se acaba de crear un grupo
    if st.session_state.get('group_created', False):
        st.session_state.group_created = False
    
    name = st.text_input("Nombre del grupo", key="new_group_name")
    description = st.text_area("Descripción", key="new_group_desc")
    is_hierarchical = st.checkbox("Grupo jerárquico", key="new_group_hier")
    
    st.markdown("---")
    st.subheader("👥 Invitar miembros (opcional)")
    
    try:
        users = api_client.list_users(token)
        options = {u[1]: u[0] for u in users if u[0] != user_id}
        
        if options:
            selected = st.multiselect(
                "Selecciona usuarios para invitar al grupo",
                list(options.keys()),
                key="new_group_members"
            )
        else:
            st.info("No hay otros usuarios disponibles para invitar")
            selected = []

        st.markdown("---")
        
        col1, col2 = st.columns([1, 3])
        with col1:
            if st.button("✅ Crear Grupo", type="primary", use_container_width=True):
                if not name or not name.strip():
                    st.error("❌ El nombre del grupo no puede estar vacío")
                elif len(name.strip()) < 3:
                    st.error("❌ El nombre debe tener al menos 3 caracteres")
                else:
                    try:
                        members = [options[s] for s in selected]
                        result = api_client.create_group(name.strip(), description.strip(), is_hierarchical, token, members)
                        st.success(f"✅ {result['message']}")
                        # Marcar para rerun sin intentar modificar widgets existentes
                        st.session_state.group_created = True
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ {str(e)}")
        with col2:
            st.caption("💡 Los miembros invitados recibirán una notificación y deberán aceptar la invitación")
            
    except Exception as e:
        st.error(f"❌ Error al cargar usuarios: {str(e)}")

def show_groups_list(user_id, api_client, token):
    """Listar todos los grupos del usuario"""
    # --- Listar grupos con nuevas funcionalidades ---
    try:
        groups = api_client.list_user_groups(token)
        if groups:
            for g in groups:
                gid, gname, hier = g
                # Check if user is leader/creator
                is_leader = False
                creator_id = None
                try:
                    group_info = api_client.get_group_info(gid, token)
                    if group_info:
                        creator_id = group_info.get('creator_id')
                        is_leader = creator_id == user_id
                    
                except Exception as e:
                    st.error(f"⚠️ ERROR al obtener info del grupo '{gname}': {str(e)}")

                # Header con indicador de líder
                if is_leader:
                    leader_badge = "👑 CREADOR - "
                elif creator_id is None:
                    # Grupo sin creador asignado - todos pueden administrarlo
                    leader_badge = "⚠️ SIN CREADOR - "
                    is_leader = True  # Permitir gestión si no hay creador
                else:
                    leader_badge = ""
                    
                st.subheader(f"{leader_badge}🏢 {gname} {'👑 (Jerárquico)' if hier else '👥 (No jerárquico)'}")

                # Opciones de visualización
                if is_leader:
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        if st.button(f"📊 Ver agendas", key=f"view_{gid}"):
                            st.session_state.current_group_view = gid
                    with col2:
                        if st.button(f"🕐 Disponibilidad", key=f"availability_{gid}"):
                            st.session_state.common_availability_group = gid
                    with col3:
                        if st.button(f"✏️ Editar", key=f"edit_{gid}"):
                            st.session_state[f'editing_group_{gid}'] = True
                    with col4:
                        if st.button(f"🗑️ Eliminar", key=f"delete_btn_{gid}", type="secondary"):
                            st.session_state[f'deleting_group_{gid}'] = True
                else:
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button(f"📊 Ver agendas", key=f"view_{gid}"):
                            st.session_state.current_group_view = gid
                    with col2:
                        if st.button(f"🕐 Disponibilidad", key=f"availability_{gid}"):
                            st.session_state.common_availability_group = gid

                # Panel de edición para líderes
                if is_leader and st.session_state.get(f'editing_group_{gid}', False):
                    show_group_edit_panel(user_id, gid, gname, api_client, token)

                # Panel de confirmación de eliminación
                if is_leader and st.session_state.get(f'deleting_group_{gid}', False):
                    show_delete_group_confirmation(user_id, gid, gname, api_client, token)

                # Mostrar miembros con sus roles y opciones de gestión
                try:
                    members = api_client.list_group_members(gid, token)

                    # Obtener el nombre del creador
                    creator_name = None
                    if creator_id is not None:
                        for member_info in members:
                            member_id = member_info[0]
                            member_name = member_info[1]
                            if member_id == creator_id:
                                creator_name = member_name
                                break

                    # Mostrar creador si existe
                    if creator_name:
                        st.write(f"**👑 Creador:** {creator_name}")
                        # Separar creador de otros miembros
                        other_members = [member_info[1] for member_info in members if member_info[0] != creator_id]
                        if other_members:
                            st.write("**👥 Miembros:** " + ", ".join(other_members))
                    else:
                        # Si no se identificó creador, mostrar todos como miembros
                        st.write("**👥 Miembros:** " + ", ".join([member_info[1] for member_info in members]))

                    # NUEVO: Gestión de miembros para líderes
                    if is_leader:
                        with st.expander("👥 Gestionar miembros"):
                            show_member_management(user_id, gid, members, api_client, token)

                    # Mostrar visualización de agendas si está activa
                    if 'current_group_view' in st.session_state and st.session_state.current_group_view == gid:
                        show_group_agendas(user_id, gid, api_client, token)

                    # Mostrar disponibilidad común si está activa
                    if 'common_availability_group' in st.session_state and st.session_state.common_availability_group == gid:
                        show_common_availability(gid, api_client, token)

                    st.markdown("---")
                except Exception as e:
                    st.error(f"Error al cargar miembros del grupo: {str(e)}")
        else:
            st.info("No perteneces a ningún grupo")
    except Exception as e:
        st.error(f"Error al cargar grupos: {str(e)}")

def show_group_edit_panel(user_id, group_id, current_name, api_client, token):
    """Panel de edición para líderes del grupo"""
    st.markdown("---")
    st.subheader("✏️ Editar grupo")

    # Get current group info
    try:
        group_info = api_client.get_group_info(group_id, token)
        current_desc = group_info.get('description', '')
    except:
        current_desc = ""

    col1, col2 = st.columns(2)
    with col1:
        new_name = st.text_input("Nombre del grupo", value=current_name, key=f"edit_name_{group_id}")
    with col2:
        new_desc = st.text_area("Descripción", value=current_desc or "", key=f"edit_desc_{group_id}")

    col_save, col_cancel = st.columns(2)
    with col_save:
        if st.button("💾 Guardar cambios", key=f"save_{group_id}"):
            try:
                # Only send fields that have changed
                update_data = {}
                if new_name != current_name:
                    update_data['name'] = new_name
                if new_desc != current_desc:
                    update_data['description'] = new_desc
                
                if update_data:
                    result = api_client.update_group(group_id, token=token, **update_data)
                    st.success(result['message'])
                    st.session_state[f'editing_group_{group_id}'] = False
                    st.rerun()
                else:
                    st.info("No hay cambios para guardar")
            except Exception as e:
                st.error(f"Error al actualizar grupo: {str(e)}")

    with col_cancel:
        if st.button("❌ Cancelar", key=f"cancel_edit_{group_id}"):
            st.session_state[f'editing_group_{group_id}'] = False
            st.rerun()

def show_delete_group_confirmation(user_id, group_id, group_name, api_client, token):
    """Panel de confirmación de eliminación de grupo"""
    st.markdown("---")
    st.error("### ⚠️ Eliminar grupo")
    st.warning(f"Estás a punto de eliminar el grupo **{group_name}**")

    st.write("**Esta acción es irreversible y eliminará:**")
    st.write("- ❌ El grupo completamente")
    st.write("- ❌ Todos los miembros del grupo")
    st.write("- ❌ Todas las invitaciones pendientes")
    st.write("- ❌ Todos los eventos del grupo")
    st.write("")
    st.info("ℹ️ Solo el creador del grupo puede eliminarlo")

    confirm_text = st.text_input(
        "Para confirmar, escribe exactamente: **ELIMINAR**",
        key=f"confirm_delete_{group_id}",
        placeholder="Escribe ELIMINAR aquí"
    )

    col_delete, col_cancel = st.columns(2)
    with col_cancel:
        if st.button("❌ Cancelar", key=f"cancel_delete_{group_id}"):
            st.session_state[f'deleting_group_{group_id}'] = False
            st.rerun()
    
    with col_delete:
        if st.button("🗑️ Eliminar permanentemente", key=f"confirm_delete_btn_{group_id}", type="primary"):
            if confirm_text.strip() == "ELIMINAR":
                try:
                    result = api_client.delete_group(group_id, token)
                    st.success("✅ Grupo eliminado exitosamente")
                    # Limpiar estado
                    if f'deleting_group_{group_id}' in st.session_state:
                        del st.session_state[f'deleting_group_{group_id}']
                    if 'current_group_view' in st.session_state and st.session_state.current_group_view == group_id:
                        del st.session_state.current_group_view
                    if 'common_availability_group' in st.session_state and st.session_state.common_availability_group == group_id:
                        del st.session_state.common_availability_group
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ {str(e)}")
            else:
                st.error("❌ Debes escribir exactamente 'ELIMINAR' para confirmar (en mayúsculas)")

def show_member_management(leader_id, group_id, member_details, api_client, token):
    """Panel de gestión de miembros para líderes"""
    st.markdown("**Invitar nuevos miembros**")

    # Obtener usuarios que no están en el grupo
    try:
        all_users = api_client.list_users(token)
        current_member_ids = [m[0] for m in member_details]
        available_users = {u[1]: u[0] for u in all_users if u[0] not in current_member_ids and u[0] != leader_id}

        if available_users:
            selected_user = st.selectbox(
                "Seleccionar usuario",
                options=list(available_users.keys()),
                key=f"invite_user_{group_id}"
            )

            if st.button("📧 Enviar invitación", key=f"send_invite_{group_id}"):
                try:
                    result = api_client.invite_user_to_group(
                        group_id,
                        available_users[selected_user],
                        token
                    )
                    st.success(f"✅ {result['message']}")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error al invitar usuario: {str(e)}")
        else:
            st.info("No hay usuarios disponibles para invitar")
    except Exception as e:
        st.error(f"Error al cargar usuarios: {str(e)}")

    st.markdown("---")
    st.markdown("**Eliminar miembros**")

    # Mostrar miembros que pueden ser eliminados (no líderes)
    # Note: We'll assume all members can be removed for now, except the leader
    removable_members = [(m[0], m[1]) for m in member_details if m[0] != leader_id]

    if removable_members:
        for member_info in removable_members:
            member_id = member_info[0]
            username = member_info[1]
            col1, col2 = st.columns([3, 1])
            with col1:
                st.write(f"👤 {username}")
            with col2:
                if st.button("🗑️ Eliminar", key=f"remove_{group_id}_{member_id}"):
                    try:
                        result = api_client.remove_member(group_id, member_id, token)
                        st.success(result['message'])
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error al eliminar miembro: {str(e)}")
    else:
        st.info("No hay miembros regulares para eliminar")

def show_group_agendas(viewer_id, group_id, api_client, token):
    """Mostrar agendas del grupo con control de acceso"""
    col_title, col_close = st.columns([3, 1])
    with col_title:
        st.subheader("📊 Agendas del grupo")
    with col_close:
        if st.button("Cerrar agendas", key=f"close_agendas_{group_id}"):
            st.session_state.pop("current_group_view", None)
            st.rerun()
    
    try:
        st.markdown("Selecciona un rango de fechas para visualizar las agendas del grupo.")
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Desde", key=f"agendas_start_{group_id}")
        with col2:
            end_date = st.date_input("Hasta", key=f"agendas_end_{group_id}")

        start_dt = datetime.combine(start_date, datetime.min.time())
        end_dt = datetime.combine(end_date, datetime.max.time()).replace(microsecond=0)
        start_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')

        agendas = api_client.get_group_agendas(group_id, start_str, end_str, token)

        if not agendas:
            st.info("No hay eventos en el período seleccionado o no tienes permisos para verlos.")
        else:
            # Mostrar por usuario
            for username, payload in agendas.items():
                events = payload.get("events", [])
                with st.expander(f"👤 {username} ({len(events)})", expanded=False):
                    if not events:
                        st.caption("Sin eventos en el rango.")
                        continue
                    for e in events:
                        title = e.get("title", "Evento")
                        is_private = e.get("is_private", False)
                        private_badge = "🔒" if is_private else "👥"
                        st.write(f"{private_badge} **{title}**")
                        st.caption(f"{e.get('start_time')} → {e.get('end_time')}")
                        if e.get("description"):
                            st.write(e.get("description"))

    except Exception as e:
        st.error(f"Error al cargar agendas del grupo: {str(e)}")

def show_common_availability(group_id, api_client, token):
    """Mostrar horarios comunes disponibles"""
    col_title, col_close = st.columns([3, 1])
    with col_title:
        st.subheader("🕐 Horarios comunes disponibles")
    with col_close:
        if st.button("Cerrar disponibilidad", key=f"close_availability_{group_id}"):
            st.session_state.pop("common_availability_group", None)
            st.rerun()
    
    try:
        st.markdown("Selecciona un rango de fechas y duración para buscar espacios libres para todo el grupo.")
        col1, col2, col3 = st.columns(3)
        with col1:
            start_date = st.date_input("Desde", key=f"avail_start_{group_id}")
        with col2:
            end_date = st.date_input("Hasta", key=f"avail_end_{group_id}")
        with col3:
            duration_hours = st.number_input("Duración (horas)", min_value=0.5, max_value=8.0, value=1.0, step=0.5, key=f"avail_dur_{group_id}")

        start_dt = datetime.combine(start_date, datetime.min.time())
        end_dt = datetime.combine(end_date, datetime.max.time()).replace(microsecond=0)
        start_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')

        slots = api_client.get_common_availability(group_id, start_str, end_str, float(duration_hours), token)

        if not slots:
            st.info("No se encontraron horarios comunes disponibles en el rango.")
        else:
            st.success(f"Encontrados {len(slots)} horarios comunes.")
            for i, slot in enumerate(slots[:50]):  # evitar listas enormes
                col_slot, col_btn = st.columns([3, 1])
                with col_slot:
                    st.write(f"🕒 {slot['start_time']} → {slot['end_time']}")
                with col_btn:
                    if st.button("Usar", key=f"use_slot_{group_id}_{i}"):
                        st.session_state.prefill_start = slot['start_time']
                        st.session_state.prefill_end = slot['end_time']
                        st.session_state.prefill_group_id = group_id
                        st.session_state.current_view = 'events'
                        st.rerun()
        
    except Exception as e:
        st.error(f"Error al calcular disponibilidad común: {str(e)}")
