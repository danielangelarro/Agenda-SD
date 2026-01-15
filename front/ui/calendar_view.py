import streamlit as st
from streamlit_calendar import calendar
import asyncio
from datetime import datetime
from streamlit_autorefresh import st_autorefresh

def show_calendar_view(user_id, api_client, token):
    st.header("📅 Mi calendario")
    # Auto refresh cada 15s para reflejar cambios sin recargar toda la app
    st_autorefresh(interval=15000, key="calendar_autorefresh")

    # Solo mostrar la vista de calendario
    try:
        events = api_client.get_user_events(token)
        removed_ids = set(st.session_state.get("removed_event_ids", []))
        calendar_events = []
        for e in events:
            # Soporta respuestas como dict (distribuido) o tupla (backend monolito)
            if isinstance(e, dict):
                if removed_ids and str(e.get("id")) in removed_ids:
                    continue
                title = e.get("title")
                start = e.get("start_time")
                end = e.get("end_time")
            else:
                try:
                    eid, title, start, end, *_ = e
                    if removed_ids and str(eid) in removed_ids:
                        continue
                except Exception:
                    # salto si el formato no es reconocido
                    continue
            if title and start and end:
                calendar_events.append({
                    "title": title,
                    "start": start,
                    "end": end,
                })

        calendar_options = {
            "headerToolbar": {
                "left": "today prev,next",
                "center": "title",
                "right": "dayGridMonth,timeGridWeek,timeGridDay",
            },
            "initialView": "dayGridMonth"
        }

        calendar(events=calendar_events, options=calendar_options, key="calendar1")
    except Exception as e:
        st.error(f"Error al cargar eventos: {str(e)}")
