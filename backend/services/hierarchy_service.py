from database.repository import Database
from services.websocket_manager import websocket_manager
import logging

class HierarchyService:
    def __init__(self):
        self.db = Database()
    
    def get_user_role_in_group(self, user_id: int, group_id: int) -> str:
        """Obtener el rol de un usuario en un grupo"""
        self.db.cursor.execute('''
            SELECT is_leader FROM user_groups 
            WHERE user_id = ? AND group_id = ?
        ''', (user_id, group_id))
        result = self.db.cursor.fetchone()
        return "leader" if result and result[0] else "member"
    
    def can_view_user_agenda(self, viewer_id: int, target_user_id: int, group_id: int = None) -> bool:
        """Determinar si un usuario puede ver la agenda de otro"""
        if viewer_id == target_user_id:
            return True
        
        if not group_id:
            # Sin grupo específico, solo puede ver su propia agenda
            return False
        
        viewer_role = self.get_user_role_in_group(viewer_id, group_id)
        target_role = self.get_user_role_in_group(target_user_id, group_id)
        
        # Líderes pueden ver agendas de miembros, pero no viceversa
        if viewer_role == "leader" and target_role == "member":
            return True
        
        # En grupos no jerárquicos, todos pueden verse entre sí
        self.db.cursor.execute('SELECT is_hierarchical FROM groups WHERE id = ?', (group_id,))
        group = self.db.cursor.fetchone()
        
        if not group or not group[0]:  # Grupo no jerárquico
            return True
        
        return False
    
    def get_accessible_members(self, user_id: int, group_id: int) -> list:
        """Obtener lista de miembros cuyas agendas puede ver el usuario"""
        members = self.db.get_group_members(group_id)
        accessible_members = []
        
        for member_id, username in members:
            if self.can_view_user_agenda(user_id, member_id, group_id):
                accessible_members.append(member_id)
        
        return accessible_members
    
    async def create_hierarchical_event(self, title: str, description: str, start_time: str, 
                                      end_time: str, creator_id: int, group_id: int):
        """Crear evento jerárquico que se aplica automáticamente a todos los miembros"""
        # Verificar que el creador es líder del grupo
        creator_role = self.get_user_role_in_group(creator_id, group_id)
        if creator_role != "leader":
            return None, "Solo los líderes pueden crear eventos jerárquicos"
        
        # Verificar conflictos del líder
        if self.db.check_conflict(creator_id, start_time, end_time):
            return None, "Conflicto en agenda del líder"
        
        # Crear evento
        event_id = self.db.add_event(
            title,
            description,
            start_time,
            end_time,
            creator_id,
            group_id,
            True,
            is_hierarchical_event=True,
        )
        
        if not event_id:
            return None, "Error al crear evento"
        
        # Añadir automáticamente a todos los miembros del grupo
        members = self.db.get_group_members(group_id)
        added_count = 0
        conflict_count = 0
        
        for member_id, username in members:
            if member_id != creator_id:
                has_conflict = False
                try:
                    has_conflict = self.db.check_conflict(member_id, start_time, end_time)
                except Exception:
                    has_conflict = False

                # Se impone igualmente: aparece en la agenda, pero se registra el conflicto.
                self.db.add_participant_to_event(event_id, member_id, True)
                added_count += 1

                if has_conflict:
                    conflict_count += 1
                    self.db.add_event_conflict(
                        event_id,
                        member_id,
                        reason="Conflicto detectado con otro evento existente (evento jerárquico)",
                    )
                    logging.warning(f"Conflicto de horario para usuario {member_id} en evento jerárquico {event_id}")

                # Notificar al miembro
                await websocket_manager.send_to_user(member_id, {
                    "type": "hierarchical_event_added",
                    "event_id": event_id,
                    "title": title,
                    "start_time": start_time,
                    "end_time": end_time,
                    "group_id": group_id,
                    "has_conflict": has_conflict,
                })

                # Notificar al líder si hubo conflicto
                if has_conflict:
                    await websocket_manager.send_to_user(creator_id, {
                        "type": "member_conflict",
                        "event_id": event_id,
                        "member_id": member_id,
                        "member_name": username,
                        "start_time": start_time,
                        "end_time": end_time,
                        "message": f"Conflicto de horario detectado para {username} en el evento jerárquico '{title}'",
                    })
        
        # Añadir creador al evento
        self.db.add_participant_to_event(event_id, creator_id, True)
        
        total = added_count + 1
        if conflict_count:
            return event_id, f"Evento creado y aplicado a {total} miembros ({conflict_count} con conflicto)"
        return event_id, f"Evento creado y aplicado a {total} miembros"
