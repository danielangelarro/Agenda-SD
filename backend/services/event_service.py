from database.repository import Database
from services.websocket_manager import websocket_manager
from services.notification_service import NotificationService
from services.hierarchy_service import HierarchyService

class EventService:
    def __init__(self):
        self.db = Database()
        self.notifications = NotificationService()
        self.hierarchy = HierarchyService()

    async def create_event(self, title, description, start_time, end_time,
                         creator_id, group_id=None, is_group_event=False,
                         participants_ids=None, is_hierarchical=False):
        """Crea un evento validando conflictos de horario con soporte para jerarquías."""
        
        # Validar campos requeridos
        if not title:
            return None, "El título es requerido"
        
        if not start_time:
            return None, "La fecha y hora de inicio son requeridas"
            
        if not end_time:
            return None, "La fecha y hora de fin son requeridas"
            
        if not creator_id:
            return None, "El creador del evento es requerido"

        # Validar que la fecha de inicio sea anterior a la fecha de fin
        from datetime import datetime
        try:
            start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
            end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
            if start_dt >= end_dt:
                return None, "La fecha y hora de inicio debe ser anterior a la fecha y hora de finalización"
        except ValueError as e:
            return None, f"Formato de fecha inválido: {str(e)}"

        # Lógica para eventos jerárquicos
        if is_hierarchical and group_id:
            return await self.hierarchy.create_hierarchical_event(
                title, description, start_time, end_time, creator_id, group_id
            )

        # Lógica original para eventos normales
        try:
            if self.db.check_conflict(creator_id, start_time, end_time):
                return None, "Conflicto en agenda del creador"
        except Exception as e:
            return None, f"Error al verificar conflictos del creador: {str(e)}"

        # Si es evento grupal y no se pasan participantes, usar todos los miembros del grupo
        if is_group_event and group_id and not participants_ids:
            try:
                members = self.db.get_group_members(group_id)
                participants_ids = [m[0] for m in members]
            except Exception as e:
                return None, f"Error al obtener miembros del grupo: {str(e)}"

        if participants_ids:
            try:
                conflict_users = []
                for p in participants_ids:
                    if self.db.check_conflict(p, start_time, end_time):
                        username = self.db.get_username(p)
                        conflict_users.append(username if username else f"Usuario {p}")
                
                if conflict_users:
                    users_str = ", ".join(conflict_users)
                    return None, f"Los siguientes usuarios tienen conflictos de horario: {users_str}"
            except Exception as e:
                return None, f"Error al verificar conflictos de participantes: {str(e)}"

        try:
            event_id = self.db.add_event(
                title,
                description,
                start_time,
                end_time,
                creator_id,
                group_id,
                is_group_event,
                is_hierarchical_event=False,
            )
        except Exception as e:
            return None, f"Error al crear el evento en la base de datos: {str(e)}"

        # Añadir participantes
        # Creador siempre aceptado automáticamente
        try:
            self.db.add_participant_to_event(event_id, creator_id, is_accepted=True)
        except Exception as e:
            return None, f"Error al añadir al creador como participante: {str(e)}"

        # Verificar si el creador es líder del grupo
        # Para eventos NO jerárquicos, siempre requieren aceptación de los demás participantes.
        if participants_ids:
            try:
                for p in participants_ids:
                    if p != creator_id:
                        # Siempre requieren aceptación (salvo creador)
                        self.db.add_participant_to_event(event_id, p, is_accepted=False)
            except Exception as e:
                return None, f"Error al añadir participantes: {str(e)}"

        # Notificaciones en tiempo real
        try:
            if group_id and is_group_event:
                await self.notifications.notify_group_event(event_id, group_id, creator_id)
            elif participants_ids:
                for user_id in participants_ids:
                    if user_id != creator_id:
                        await websocket_manager.send_to_user(user_id, {
                            "type": "event_invitation",
                            "event_id": event_id,
                            "title": title,
                            "start_time": start_time,
                            "end_time": end_time
                        })
        except Exception as e:
            # Log the error but don't fail the event creation
            print(f"Error al enviar notificaciones: {str(e)}")

        return event_id, None

    # Mantener métodos existentes para compatibilidad
    def get_user_events(self, user_id):
        """Eventos de un usuario."""
        return self.db.get_events_by_user(user_id)

    def has_conflict(self, user_id, start_time, end_time):
        """Chequear conflictos en agenda de un usuario."""
        return self.db.check_conflict(user_id, start_time, end_time)
    
    async def update_event(self, event_id, user_id, **updates):
        """Editar/replanificar un evento con verificación de disponibilidad y notificaciones.

        Reglas:
        - Solo el creador puede editar.
        - Si cambia horario:
          - Eventos normales: todos los participantes (excepto creador) vuelven a estado pendiente.
          - Eventos jerárquicos: se impone, se registran conflictos pero no se bloquea.
        - Si se actualiza la lista de participantes (participants_ids):
          - Se agrega/quita participantes y se notifica.
          - Para eventos grupales, se valida pertenencia al grupo.
        """
        from datetime import datetime

        title = updates.get("title")
        description = updates.get("description")
        start_time = updates.get("start_time")
        end_time = updates.get("end_time")
        participants_ids = updates.get("participants_ids")  # opcional (lista de ints) o None

        ev = self.db.get_event(int(event_id))
        if not ev:
            return False, "Evento no encontrado"

        (
            _id,
            old_title,
            old_description,
            old_start,
            old_end,
            creator_id,
            group_id,
            is_group_event,
            is_hierarchical_event,
        ) = ev

        if int(creator_id) != int(user_id):
            return False, "Solo el creador puede modificar este evento"

        new_title = old_title if title is None else title
        new_description = old_description if description is None else description
        new_start = old_start if start_time is None else start_time
        new_end = old_end if end_time is None else end_time

        try:
            start_dt = datetime.strptime(new_start, "%Y-%m-%d %H:%M:%S")
            end_dt = datetime.strptime(new_end, "%Y-%m-%d %H:%M:%S")
            if start_dt >= end_dt:
                return False, "La fecha y hora de inicio debe ser anterior a la fecha y hora de finalización"
        except ValueError as e:
            return False, f"Formato de fecha inválido: {str(e)}"

        time_changed = (new_start != old_start) or (new_end != old_end)

        # Participantes actuales
        current_participants = self.db.get_event_participants(int(event_id))
        current_ids = {int(uid) for uid, _ in current_participants}
        # Asegurar que el creador está en la tabla de participantes
        if int(creator_id) not in current_ids:
            self.db.upsert_event_participant(int(event_id), int(creator_id), True)
            current_ids.add(int(creator_id))

        desired_ids = set(current_ids)
        if participants_ids is not None:
            try:
                desired_ids = {int(x) for x in participants_ids} | {int(creator_id)}
            except Exception:
                return False, "participants_ids inválido"

        # Validación adicional para eventos grupales
        if group_id and is_group_event:
            group_member_ids = set(self.db.get_group_member_ids(int(group_id)))
            if participants_ids is not None:
                invalid = sorted([uid for uid in desired_ids if uid not in group_member_ids])
                if invalid:
                    return False, "Hay participantes que no pertenecen al grupo"

        # Verificar conflicto del creador (siempre bloquea, incluso jerárquico)
        if self.db.check_conflict_excluding_event(int(creator_id), new_start, new_end, int(event_id)):
            return False, "Conflicto en agenda del creador"

        # Eventos jerárquicos: se aplican a todos los miembros, se registran conflictos
        if is_hierarchical_event:
            if not group_id:
                return False, "Evento jerárquico inválido: no tiene grupo asociado"
            if self.hierarchy.get_user_role_in_group(int(creator_id), int(group_id)) != "leader":
                return False, "Solo los líderes pueden modificar eventos jerárquicos"

            member_ids = set(self.db.get_group_member_ids(int(group_id)))
            member_ids.add(int(creator_id))

            # Actualizar evento
            if not self.db.update_event(int(event_id), title=new_title, description=new_description, start_time=new_start, end_time=new_end):
                return False, "No se pudo actualizar el evento"

            # Sincronizar participantes: todos aceptados
            to_remove = current_ids - member_ids
            for uid in to_remove:
                self.db.remove_event_participant(int(event_id), int(uid))

            for uid in member_ids:
                self.db.upsert_event_participant(int(event_id), int(uid), True)

            # Recalcular conflictos del evento
            self.db.clear_event_conflicts(int(event_id))
            conflict_members = []
            for uid in member_ids:
                if int(uid) == int(creator_id):
                    continue
                if self.db.check_conflict_excluding_event(int(uid), new_start, new_end, int(event_id)):
                    self.db.add_event_conflict(
                        int(event_id),
                        int(uid),
                        reason="Conflicto detectado con otro evento existente (evento jerárquico reprogramado)",
                    )
                    conflict_members.append(int(uid))

            # Notificar a miembros
            for uid in member_ids:
                if int(uid) == int(creator_id):
                    continue
                await websocket_manager.send_to_user(int(uid), {
                    "type": "hierarchical_event_updated",
                    "event_id": int(event_id),
                    "title": new_title,
                    "start_time": new_start,
                    "end_time": new_end,
                    "group_id": int(group_id),
                    "has_conflict": int(uid) in conflict_members,
                    "message": f"Evento jerárquico reprogramado: '{new_title}' ({new_start} → {new_end})",
                })

            if conflict_members:
                return True, f"Evento jerárquico reprogramado ({len(conflict_members)} con conflicto)"
            return True, "Evento jerárquico reprogramado"

        # Eventos normales: verificar conflictos de participantes y actualizar (si cambia horario, requiere re-aceptación)
        conflict_users = []
        for uid in sorted(desired_ids):
            if int(uid) == int(creator_id):
                continue
            if self.db.check_conflict_excluding_event(int(uid), new_start, new_end, int(event_id)):
                uname = self.db.get_username(int(uid))
                conflict_users.append(uname or f"Usuario {uid}")

        if conflict_users:
            return False, f"Los siguientes usuarios tienen conflictos de horario: {', '.join(conflict_users)}"

        # Actualizar evento
        if not self.db.update_event(int(event_id), title=new_title, description=new_description, start_time=new_start, end_time=new_end):
            return False, "No se pudo actualizar el evento"

        # Cambios de participantes (solo si se envía explícitamente)
        added = set()
        removed = set()
        if participants_ids is not None:
            added = desired_ids - current_ids
            removed = current_ids - desired_ids

            for uid in removed:
                if int(uid) == int(creator_id):
                    continue
                self.db.remove_event_participant(int(event_id), int(uid))
                await websocket_manager.send_to_user(int(uid), {
                    "type": "event_removed",
                    "event_id": int(event_id),
                    "event_title": new_title,
                    "message": f"Fuiste removido del evento '{new_title}'",
                })

            for uid in added:
                if int(uid) == int(creator_id):
                    continue
                self.db.upsert_event_participant(int(event_id), int(uid), False)
                await websocket_manager.send_to_user(int(uid), {
                    "type": "event_invitation",
                    "event_id": int(event_id),
                    "title": new_title,
                    "start_time": new_start,
                    "end_time": new_end,
                    "message": f"Fuiste invitado al evento '{new_title}' ({new_start} → {new_end})",
                })

        # Si cambió el horario, reiniciar aceptación de todos menos creador
        if time_changed:
            to_reset = [uid for uid in desired_ids if int(uid) != int(creator_id)]
            self.db.set_event_participants_acceptance(int(event_id), to_reset, False)
            # Mantener creador aceptado
            self.db.upsert_event_participant(int(event_id), int(creator_id), True)

            # Notificar reprogramación a participantes existentes (excluye los recién invitados, que ya recibieron invitación)
            for uid in desired_ids:
                if int(uid) == int(creator_id) or int(uid) in added:
                    continue
                await websocket_manager.send_to_user(int(uid), {
                    "type": "event_rescheduled",
                    "event_id": int(event_id),
                    "title": new_title,
                    "start_time": new_start,
                    "end_time": new_end,
                    "message": f"El evento '{new_title}' fue reprogramado ({new_start} → {new_end}). Debes aceptar nuevamente.",
                })

            return True, "Evento reprogramado; se solicitaron nuevas aceptaciones"

        # Si no cambió horario, solo notificar actualización “suave”
        if title is not None or description is not None:
            for uid in desired_ids:
                if int(uid) == int(creator_id) or int(uid) in added:
                    continue
                await websocket_manager.send_to_user(int(uid), {
                    "type": "event_updated",
                    "event_id": int(event_id),
                    "title": new_title,
                    "message": f"El evento '{new_title}' fue actualizado",
                })

        return True, "Evento actualizado"

    def get_pending_event_invitations(self, user_id):
        """Obtener invitaciones pendientes a eventos para un usuario."""
        self.db.cursor.execute('''
            SELECT e.id, e.title, e.description, e.start_time, e.end_time,
                   u.username as creator_name, g.name as group_name,
                   e.is_group_event, e.group_id
            FROM events e
            JOIN event_participants ep ON e.id = ep.event_id
            JOIN users u ON e.creator_id = u.id
            LEFT JOIN groups g ON e.group_id = g.id
            WHERE ep.user_id = ? AND ep.is_accepted = 0
            ORDER BY e.start_time
        ''', (user_id,))
        return self.db.cursor.fetchall()

    async def respond_to_event_invitation(self, event_id, user_id, accepted):
        """Aceptar o rechazar una invitación a un evento."""
        if accepted:
            # Verificar conflictos antes de aceptar
            self.db.cursor.execute(
                'SELECT start_time, end_time FROM events WHERE id = ?',
                (event_id,)
            )
            event = self.db.cursor.fetchone()

            if event and self.db.check_conflict(user_id, event[0], event[1]):
                return False, "Conflicto con otro evento en tu agenda"

            # Aceptar la invitación
            self.db.cursor.execute('''
                UPDATE event_participants
                SET is_accepted = 1
                WHERE event_id = ? AND user_id = ?
            ''', (event_id, user_id))
            self.db.conn.commit()

            # Notificar al creador
            self.db.cursor.execute('SELECT creator_id, title FROM events WHERE id = ?', (event_id,))
            event_data = self.db.cursor.fetchone()
            if event_data:
                creator_id, title = event_data
                username = self.db.get_username(user_id)
                await websocket_manager.send_to_user(creator_id, {
                    "type": "event_accepted",
                    "event_id": event_id,
                    "event_title": title,
                    "user_name": username
                })

            return True, "Invitación aceptada"
        else:
            # Rechazar la invitación (eliminar participante)
            self.db.cursor.execute('''
                DELETE FROM event_participants
                WHERE event_id = ? AND user_id = ?
            ''', (event_id, user_id))
            self.db.conn.commit()

            # Notificar al creador
            self.db.cursor.execute('SELECT creator_id, title FROM events WHERE id = ?', (event_id,))
            event_data = self.db.cursor.fetchone()
            if event_data:
                creator_id, title = event_data
                username = self.db.get_username(user_id)
                await websocket_manager.send_to_user(creator_id, {
                    "type": "event_declined",
                    "event_id": event_id,
                    "event_title": title,
                    "user_name": username
                })

            return True, "Invitación rechazada"

    def get_event_details(self, event_id, user_id):
        """Obtener detalles completos de un evento con participantes."""
        # Verificar que el usuario tenga acceso al evento
        self.db.cursor.execute('''
            SELECT 1 FROM event_participants
            WHERE event_id = ? AND user_id = ?
            UNION
            SELECT 1 FROM events
            WHERE id = ? AND creator_id = ?
        ''', (event_id, user_id, event_id, user_id))

        if not self.db.cursor.fetchone():
            return None, "No tienes acceso a este evento"

        # Obtener información del evento
        self.db.cursor.execute('''
            SELECT e.id, e.title, e.description, e.start_time, e.end_time,
                   e.creator_id, u.username as creator_name,
                   e.group_id, g.name as group_name, g.is_hierarchical,
                   e.is_group_event, e.is_hierarchical_event
            FROM events e
            JOIN users u ON e.creator_id = u.id
            LEFT JOIN groups g ON e.group_id = g.id
            WHERE e.id = ?
        ''', (event_id,))

        event_data = self.db.cursor.fetchone()
        if not event_data:
            return None, "Evento no encontrado"

        # Obtener participantes
        self.db.cursor.execute('''
            SELECT u.id, u.username, ep.is_accepted
            FROM event_participants ep
            JOIN users u ON ep.user_id = u.id
            WHERE ep.event_id = ?
            ORDER BY ep.is_accepted DESC, u.username
        ''', (event_id,))

        participants = []
        for p in self.db.cursor.fetchall():
            participants.append({
                'user_id': p[0],
                'username': p[1],
                'is_accepted': p[2]
            })

        event_details = {
            'id': event_data[0],
            'title': event_data[1],
            'description': event_data[2],
            'start_time': event_data[3],
            'end_time': event_data[4],
            'creator_id': event_data[5],
            'creator_name': event_data[6],
            'group_id': event_data[7],
            'group_name': event_data[8],
            'is_hierarchical': event_data[9],
            'is_group_event': event_data[10],
            'is_hierarchical_event': bool(event_data[11]),
            'participants': participants
        }

        return event_details, None

    def get_user_events_detailed(self, user_id, filter_type='all'):
        """
        Obtener eventos del usuario con información detallada.
        filter_type: 'all', 'upcoming', 'past', 'pending', 'created'
        """
        from datetime import datetime

        base_query = '''
            SELECT e.id, e.title, e.description, e.start_time, e.end_time,
                   e.creator_id, u.username as creator_name,
                   g.name as group_name, e.is_group_event,
                   ep.is_accepted,
                   CASE WHEN e.creator_id = ? THEN 1 ELSE 0 END as is_creator
            FROM events e
            LEFT JOIN event_participants ep ON e.id = ep.event_id AND ep.user_id = ?
            LEFT JOIN users u ON e.creator_id = u.id
            LEFT JOIN groups g ON e.group_id = g.id
            WHERE (ep.user_id = ? OR e.creator_id = ?)
        '''

        params = [user_id, user_id, user_id, user_id]

        # Aplicar filtros
        if filter_type == 'upcoming':
            base_query += " AND datetime(e.start_time) >= datetime('now')"
        elif filter_type == 'past':
            base_query += " AND datetime(e.start_time) < datetime('now')"
        elif filter_type == 'pending':
            base_query += " AND ep.is_accepted = 0 AND e.creator_id != ?"
            params.append(user_id)
        elif filter_type == 'created':
            base_query += " AND e.creator_id = ?"
            params.append(user_id)

        base_query += " ORDER BY e.start_time DESC"

        self.db.cursor.execute(base_query, params)

        events = []
        for row in self.db.cursor.fetchall():
            events.append({
                'id': row[0],
                'title': row[1],
                'description': row[2],
                'start_time': row[3],
                'end_time': row[4],
                'creator_id': row[5],
                'creator_name': row[6],
                'group_name': row[7],
                'is_group_event': row[8],
                'is_accepted': row[9],
                'is_creator': row[10]
            })

        return events

    async def cancel_event(self, event_id, user_id):
        """Cancelar un evento (solo el creador puede hacerlo)."""
        # Verificar que el usuario es el creador
        self.db.cursor.execute(
            'SELECT creator_id, title FROM events WHERE id = ?',
            (event_id,)
        )
        event = self.db.cursor.fetchone()

        if not event:
            return False, "Evento no encontrado"

        if event[0] != user_id:
            return False, "Solo el creador puede cancelar el evento"

        # Obtener participantes antes de eliminar
        self.db.cursor.execute(
            'SELECT user_id FROM event_participants WHERE event_id = ?',
            (event_id,)
        )
        participants = [p[0] for p in self.db.cursor.fetchall()]

        # Eliminar participantes
        self.db.cursor.execute(
            'DELETE FROM event_participants WHERE event_id = ?',
            (event_id,)
        )

        # Eliminar evento
        self.db.cursor.execute('DELETE FROM events WHERE id = ?', (event_id,))
        self.db.conn.commit()

        # Notificar a participantes
        for participant_id in participants:
            if participant_id != user_id:
                await websocket_manager.send_to_user(participant_id, {
                    "type": "event_cancelled",
                    "event_id": event_id,
                    "event_title": event[1]
                })

        return True, "Evento cancelado exitosamente"

    async def leave_event(self, event_id, user_id):
        """Salir de un evento (solo para participantes, no creadores)."""
        # Verificar que el usuario no es el creador
        self.db.cursor.execute(
            'SELECT creator_id, title FROM events WHERE id = ?',
            (event_id,)
        )
        event = self.db.cursor.fetchone()

        if not event:
            return False, "Evento no encontrado"

        if event[0] == user_id:
            return False, "El creador no puede salir del evento. Usa 'Cancelar evento' en su lugar."

        # Eliminar participante
        self.db.cursor.execute(
            'DELETE FROM event_participants WHERE event_id = ? AND user_id = ?',
            (event_id, user_id)
        )
        self.db.conn.commit()

        # Notificar al creador
        username = self.db.get_username(user_id)
        await websocket_manager.send_to_user(event[0], {
            "type": "participant_left",
            "event_id": event_id,
            "event_title": event[1],
            "user_name": username
        })

        return True, "Has salido del evento"

    def get_pending_invitations_count(self, user_id):
        """Obtener conteo de invitaciones a eventos pendientes."""
        invitations = self.get_pending_event_invitations(user_id)
        return len(invitations) if invitations else 0
