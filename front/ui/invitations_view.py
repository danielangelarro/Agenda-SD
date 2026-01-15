import streamlit as st
import asyncio

def show_invitations_view(user_id, api_client, token):
    st.header("📧 Invitaciones pendientes")
    # Obtener conteos para los badges
    try:
        groups_count_data = api_client.get_pending_invitations_count(token)
        events_count_data = api_client.get_pending_event_invitations_count(token)
        groups_count = groups_count_data.get("count", 0)
        events_count = events_count_data.get("count", 0)

        # Crear etiquetas con badges para las pestañas
        groups_label = f"👥 Grupos ({groups_count})" if groups_count > 0 else "👥 Grupos"
        events_label = f"📅 Eventos ({events_count})" if events_count > 0 else "📅 Eventos"

        # Tabs para separar invitaciones a grupos y eventos
        tab1, tab2 = st.tabs([groups_label, events_label])

        with tab1:
            show_group_invitations(user_id, api_client, token)

        with tab2:
            show_event_invitations(user_id, api_client, token)
    except Exception as e:
        st.error(f"Error al cargar invitaciones: {str(e)}")


def show_group_invitations(user_id, api_client, token):
    """Mostrar invitaciones a grupos"""
    st.subheader("Invitaciones a grupos")
    try:
        invitations = api_client.get_pending_invitations(token)

        if not invitations:
            st.info("No tienes invitaciones a grupos pendientes")
            return

        for inv in invitations:
            # Backend devuelve dicts; mantenemos compatibilidad si llega una tupla
            if isinstance(inv, dict):
                inv_id = inv.get("id")
                group_id = inv.get("group_id")
                inviter_id = inv.get("inviter_id")
                invited_username = inv.get("invited_username", "")
                created_at = inv.get("created_at", "")
                group_name = f"Grupo {group_id}" if group_id is not None else "Grupo"
                inviter_name = f"Usuario {inviter_id}" if inviter_id is not None else "Desconocido"
            else:
                try:
                    inv_id, group_name, inviter_name, created_at, group_id = inv
                except Exception:
                    st.warning(f"Formato de invitación no reconocido: {inv}")
                    continue
            with st.container():
                st.markdown(f"### 🏢 {group_name}")
                st.markdown(f"**Invitado por:** {inviter_name}")
                st.markdown(f"**Fecha:** {created_at}")

                col1, col2 = st.columns(2)
                with col1:
                    if st.button(f"✅ Aceptar", key=f"acc_grp_{inv_id}"):
                        try:
                            result = api_client.respond_to_group_invitation(inv_id, "accepted", token)
                            st.success("Te uniste al grupo")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al aceptar invitación: {str(e)}")
                with col2:
                    if st.button(f"❌ Rechazar", key=f"rej_grp_{inv_id}"):
                        try:
                            result = api_client.respond_to_group_invitation(inv_id, "declined", token)
                            st.warning("Invitación rechazada")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error al rechazar invitación: {str(e)}")
                st.markdown("---")
    except Exception as e:
        st.error(f"Error al cargar invitaciones a grupos: {str(e)}")


def show_event_invitations(user_id, api_client, token):
    """Mostrar invitaciones a eventos"""
    st.subheader("Invitaciones a eventos")
    try:
        invitations = api_client.get_pending_event_invitations(token)

        if not invitations:
            st.info("No tienes invitaciones a eventos pendientes")
            return

        for inv in invitations:
            event_id, title, description, start_time, end_time, creator_name, group_name, is_group_event, group_id = inv

            with st.container():
                st.markdown(f"### 📅 {title}")
                st.markdown(f"**Descripción:** {description or 'Sin descripción'}")
                st.markdown(f"**Creador:** {creator_name}")

                if is_group_event and group_name:
                    st.markdown(f"**Grupo:** 👥 {group_name}")

                st.markdown(f"**⏰ Inicio:** {start_time}")
                st.markdown(f"**⏰ Fin:** {end_time}")

                col1, col2 = st.columns(2)
                with col1:
                    if st.button(f"✅ Aceptar", key=f"acc_evt_{event_id}"):
                        try:
                            result = api_client.respond_to_event_invitation(event_id, True, token)
                            st.success(f"✅ {result['message']}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error al aceptar invitación: {str(e)}")

                with col2:
                    if st.button(f"❌ Rechazar", key=f"rej_evt_{event_id}"):
                        try:
                            result = api_client.respond_to_event_invitation(event_id, False, token)
                            st.warning(f"⚠️ {result['message']}")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error al rechazar invitación: {str(e)}")

                st.markdown("---")
    except Exception as e:
        st.error(f"Error al cargar invitaciones a eventos: {str(e)}")
