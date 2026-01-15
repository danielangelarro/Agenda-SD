from database.repository import Database
from services.websocket_manager import websocket_manager
import asyncio

class GroupService:
    def __init__(self):
        self.db = Database()

    async def create_group(self, name, description, is_hierarchical, creator_id, members=None):
        """Crear grupo y enviar invitaciones con notificaciones."""
        group_id = self.db.add_group(name, description, is_hierarchical, creator_id)
        if not group_id:
            return None, "El grupo ya existe"

        invited = 0
        skipped = 0
        if members:
            for m in members:
                if self.db.invite_user_to_group(group_id, m, creator_id):
                    invited += 1
                    # Notificar al usuario invitado
                    await websocket_manager.send_to_user(m, {
                        "type": "group_invitation",
                        "group_id": group_id,
                        "group_name": name,
                        "inviter_id": creator_id
                    })
                else:
                    skipped += 1

        message = f"Invitaciones enviadas: {invited}"
        if skipped > 0:
            message += f" (omitidos: {skipped} - ya son miembros o tienen invitación pendiente)"
        return group_id, message

    # Mantener métodos existentes...
    def list_user_groups(self, user_id):
        return self.db.get_groups_by_user(user_id)

    def list_group_members(self, group_id):
        return self.db.get_group_members(group_id)

    async def invite_user(self, group_id, invited_user_id, inviter_id):
        success = self.db.invite_user_to_group(group_id, invited_user_id, inviter_id)
        if success:
            group_name = self.db.cursor.execute(
                'SELECT name FROM groups WHERE id = ?', (group_id,)
            ).fetchone()[0]

            await websocket_manager.send_to_user(invited_user_id, {
                "type": "group_invitation",
                "group_id": group_id,
                "group_name": group_name,
                "inviter_id": inviter_id
            })
            return True, "Invitación enviada exitosamente"
        else:
            return False, "El usuario ya es miembro o tiene una invitación pendiente"

    def pending_invitations(self, user_id):
        return self.db.get_pending_invitations(user_id)

    async def respond_invitation(self, invitation_id, response, user_id):
        success = self.db.respond_to_invitation(invitation_id, response, user_id)
        if success and response == 'accepted':
            # Notificar al grupo sobre nuevo miembro
            invitation = self.db.cursor.execute(
                'SELECT group_id FROM group_invitations WHERE id = ?', (invitation_id,)
            ).fetchone()
            if invitation:
                group_members = self.db.get_group_members(invitation[0])
                for member_id, _ in group_members:
                    if member_id != user_id:
                        await websocket_manager.send_to_user(member_id, {
                            "type": "new_group_member",
                            "group_id": invitation[0],
                            "new_member_id": user_id
                        })
        return success

    def update_group(self, group_id, user_id, name=None, description=None):
        """Actualizar información del grupo (solo líderes)"""
        # Verificar que el usuario es líder
        if not self.db.is_group_leader(user_id, group_id):
            return False, "Solo los líderes pueden editar el grupo"

        success = self.db.update_group(group_id, name, description)
        if success:
            return True, "Grupo actualizado exitosamente"
        else:
            return False, "Error al actualizar el grupo (el nombre podría estar en uso)"

    async def remove_member(self, group_id, leader_id, member_id):
        """Eliminar un miembro del grupo (solo líderes)"""
        # Verificar que el usuario es líder
        if not self.db.is_group_leader(leader_id, group_id):
            return False, "Solo los líderes pueden eliminar miembros"

        # No permitir que el líder se elimine a sí mismo
        if leader_id == member_id:
            return False, "No puedes eliminarte a ti mismo del grupo"

        success = self.db.remove_user_from_group(member_id, group_id)
        if success:
            # Notificar al usuario eliminado
            await websocket_manager.send_to_user(member_id, {
                "type": "removed_from_group",
                "group_id": group_id
            })
            return True, "Miembro eliminado del grupo"
        else:
            return False, "Error al eliminar miembro"

    def is_leader(self, user_id, group_id):
        """Verificar si un usuario es líder de un grupo"""
        return self.db.is_group_leader(user_id, group_id)

    def get_group_info(self, group_id):
        """Obtener información del grupo"""
        return self.db.get_group_info(group_id)

    def get_pending_invitations_count(self, user_id):
        """Obtener conteo de invitaciones a grupos pendientes."""
        invitations = self.pending_invitations(user_id)
        return len(invitations) if invitations else 0

    async def delete_group(self, group_id, user_id):
        """Eliminar un grupo completamente (solo creadores)"""
        # Obtener información del grupo
        group_info = self.db.get_group_info(group_id)
        if not group_info:
            return False, "Grupo no encontrado"

        # Verificar que el usuario es el creador del grupo
        creator_id = group_info[4]
        if creator_id != user_id:
            return False, "Solo el creador del grupo puede eliminarlo"

        group_name = group_info[1]

        # Obtener todos los miembros para notificarles
        members = self.db.get_group_members(group_id)

        # Eliminar el grupo
        success = self.db.delete_group(group_id)

        if success:
            # Notificar a todos los miembros (excepto quien eliminó)
            for member_id, _ in members:
                if member_id != user_id:
                    await websocket_manager.send_to_user(member_id, {
                        "type": "group_deleted",
                        "group_id": group_id,
                        "group_name": group_name,
                        "deleted_by": user_id
                    })

            return True, f"Grupo '{group_name}' eliminado exitosamente"
        else:
            return False, "Error al eliminar el grupo"