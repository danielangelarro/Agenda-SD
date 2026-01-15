from database.repository import Database
from services.hierarchy_service import HierarchyService
from datetime import datetime, timedelta

class VisualizationService:
    def __init__(self):
        self.db = Database()
        self.hierarchy = HierarchyService()

    def get_group_agendas(self, viewer_id, group_id, start_date, end_date):
        """
        Obtener agendas de los miembros de un grupo en un período de tiempo.
        Respeta jerarquías y privacidad.
        """
        # Verificar que el usuario pertenece al grupo
        self.db.cursor.execute('''
            SELECT 1 FROM user_groups WHERE user_id = ? AND group_id = ?
        ''', (viewer_id, group_id))

        if not self.db.cursor.fetchone():
            return None, "No perteneces a este grupo"

        # Obtener miembros accesibles según jerarquía
        accessible_members = self.hierarchy.get_accessible_members(viewer_id, group_id)

        if not accessible_members:
            return None, "No tienes permisos para ver agendas de este grupo"

        # Construir resultado
        group_agendas = {}

        for member_id in accessible_members:
            username = self.db.get_username(member_id)

            # Obtener eventos del usuario en el rango de fechas
            self.db.cursor.execute('''
                SELECT e.id, e.title, e.description, e.start_time, e.end_time,
                       e.is_group_event, e.group_id,
                       g.name as group_name, e.creator_id
                FROM events e
                LEFT JOIN event_participants ep ON e.id = ep.event_id
                LEFT JOIN groups g ON e.group_id = g.id
                WHERE (ep.user_id = ? OR e.creator_id = ?)
                  AND ep.is_accepted = 1
                  AND e.start_time >= ?
                  AND e.end_time <= ?
                ORDER BY e.start_time
            ''', (member_id, member_id, start_date, end_date))

            events = []
            for row in self.db.cursor.fetchall():
                event_id, title, description, start_time, end_time, is_group_event, event_group_id, group_name, creator_id = row

                # Privacidad: si el evento no corresponde al grupo consultado, ocultar detalles.
                show_details = bool(is_group_event) and event_group_id == group_id
                safe_title = title if show_details else "Ocupado"
                safe_description = description if show_details else None

                events.append({
                    'id': event_id,
                    'title': safe_title,
                    'description': safe_description,
                    'start_time': start_time,
                    'end_time': end_time,
                    'is_group_event': is_group_event,
                    'group_name': group_name,
                    'creator_id': creator_id,
                    'is_private': not show_details,
                })

            group_agendas[username] = {
                'user_id': member_id,
                'events': events
            }

        return group_agendas, None

    def get_common_availability(self, group_id, start_date, end_date, duration_hours):
        """
        Encontrar horarios comunes disponibles para todos los miembros de un grupo.
        """
        # Obtener todos los miembros del grupo
        members = self.db.get_group_members(group_id)
        member_ids = [m[0] for m in members]

        if not member_ids:
            return []

        # Convertir fechas a datetime
        start_dt = datetime.strptime(f"{start_date} 00:00:00", '%Y-%m-%d %H:%M:%S')
        end_dt = datetime.strptime(f"{end_date} 23:59:59", '%Y-%m-%d %H:%M:%S')

        # Definir horario laboral (9:00 - 18:00)
        work_start_hour = 9
        work_end_hour = 18

        # Generar slots candidatos de 30 minutos
        slots = []
        current = start_dt.replace(hour=work_start_hour, minute=0, second=0)
        slot_duration = timedelta(hours=duration_hours)

        while current <= end_dt:
            # Solo considerar horarios dentro del horario laboral
            if work_start_hour <= current.hour < work_end_hour:
                slot_end = current + slot_duration

                # Verificar que el slot completo está dentro del horario laboral
                if slot_end.hour <= work_end_hour:
                    slots.append({
                        'start': current,
                        'end': slot_end
                    })

            current += timedelta(minutes=30)

            # Saltar al siguiente día si pasamos las 18:00
            if current.hour >= work_end_hour:
                current = current.replace(hour=work_start_hour, minute=0) + timedelta(days=1)

        # Filtrar slots que están libres para TODOS los miembros
        available_slots = []

        for slot in slots:
            is_available_for_all = True

            for member_id in member_ids:
                # Verificar si el miembro tiene conflictos en este slot
                if self.db.check_conflict(
                    member_id,
                    slot['start'].strftime('%Y-%m-%d %H:%M:%S'),
                    slot['end'].strftime('%Y-%m-%d %H:%M:%S')
                ):
                    is_available_for_all = False
                    break

            if is_available_for_all:
                available_slots.append({
                    'start_time': slot['start'].strftime('%Y-%m-%d %H:%M:%S'),
                    'end_time': slot['end'].strftime('%Y-%m-%d %H:%M:%S')
                })

        return available_slots

    def get_user_availability(self, user_id, start_date, end_date):
        """
        Obtener horarios disponibles de un usuario específico.
        """
        # Obtener todos los eventos del usuario en el período
        self.db.cursor.execute('''
            SELECT e.start_time, e.end_time
            FROM events e
            LEFT JOIN event_participants ep ON e.id = ep.event_id
            WHERE (ep.user_id = ? OR e.creator_id = ?)
              AND ep.is_accepted = 1
              AND e.start_time >= ?
              AND e.end_time <= ?
            ORDER BY e.start_time
        ''', (user_id, user_id, start_date, end_date))

        busy_slots = []
        for row in self.db.cursor.fetchall():
            start_time, end_time = row
            busy_slots.append({
                'start': datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S'),
                'end': datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
            })

        return busy_slots
