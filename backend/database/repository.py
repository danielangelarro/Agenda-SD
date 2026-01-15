import sqlite3
import bcrypt
from datetime import datetime
from .schema import setup_database
import os

class Database:
    def __init__(self, db_name=None):
        # Use environment variable for DB path, with fallback to default
        if db_name is None:
            db_name = os.getenv('DB_PATH', 'agenda.db')
        setup_database(db_name)
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.cursor = self.conn.cursor()

    def close(self):
        self.conn.close()

    # ---------- Usuarios ----------
    def get_user(self, username):
        self.cursor.execute('SELECT * FROM users WHERE username = ?', (username,))
        return self.cursor.fetchone()

    def add_user(self, username, password):
        try:
            password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            self.cursor.execute('INSERT INTO users (username, password_hash) VALUES (?, ?)', (username, password_hash))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def check_password(self, username, password):
        user = self.get_user(username)
        if user:
            try:
                stored_hash = user[2]
                # Asegurarse de que el hash esté en bytes
                if isinstance(stored_hash, str):
                    stored_hash = stored_hash.encode('utf-8')

                # Asegurarse de que la contraseña esté en bytes
                if isinstance(password, str):
                    password = password.encode('utf-8')

                return bcrypt.checkpw(password, stored_hash)
            except Exception as e:
                print(f"Error al verificar contraseña: {e}")
                return False
        return False

    def get_user_id(self, username):
        self.cursor.execute('SELECT id FROM users WHERE username = ?', (username,))
        result = self.cursor.fetchone()
        return result[0] if result else None

    def get_username(self, user_id):
        self.cursor.execute('SELECT username FROM users WHERE id = ?', (user_id,))
        result = self.cursor.fetchone()
        return result[0] if result else None

    def get_all_users(self):
        self.cursor.execute('SELECT id, username FROM users')
        return self.cursor.fetchall()

    # ---------- Eventos ----------
    def add_event(
        self,
        title,
        description,
        start_time,
        end_time,
        creator_id,
        group_id=None,
        is_group_event=False,
        is_hierarchical_event=False,
    ):
        try:
            # Validate required fields
            if not title:
                raise ValueError("Title is required")
            if not start_time:
                raise ValueError("Start time is required")
            if not end_time:
                raise ValueError("End time is required")
            if not creator_id:
                raise ValueError("Creator ID is required")
                
            self.cursor.execute('''
                INSERT INTO events (
                    title, description, start_time, end_time, creator_id,
                    group_id, is_group_event, is_hierarchical_event
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (title, description, start_time, end_time, creator_id, group_id, is_group_event, is_hierarchical_event))
            self.conn.commit()
            return self.cursor.lastrowid
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Failed to add event to database: {str(e)}")

    def get_event(self, event_id: int):
        self.cursor.execute(
            '''
            SELECT id, title, description, start_time, end_time, creator_id,
                   group_id, is_group_event, is_hierarchical_event
            FROM events
            WHERE id = ?
            ''',
            (event_id,),
        )
        return self.cursor.fetchone()

    def update_event(self, event_id: int, title=None, description=None, start_time=None, end_time=None):
        updates = []
        params = []

        if title is not None:
            updates.append("title = ?")
            params.append(title)
        if description is not None:
            updates.append("description = ?")
            params.append(description)
        if start_time is not None:
            updates.append("start_time = ?")
            params.append(start_time)
        if end_time is not None:
            updates.append("end_time = ?")
            params.append(end_time)

        if not updates:
            return True

        params.append(event_id)
        query = f"UPDATE events SET {', '.join(updates)} WHERE id = ?"
        try:
            self.cursor.execute(query, params)
            self.conn.commit()
            return True
        except Exception:
            self.conn.rollback()
            return False

    def add_participant_to_event(self, event_id, user_id, is_accepted=False):
        try:
            if not event_id:
                raise ValueError("Event ID is required")
            if not user_id:
                raise ValueError("User ID is required")
                
            self.cursor.execute('INSERT INTO event_participants (event_id, user_id, is_accepted) VALUES (?, ?, ?)',
                                (event_id, user_id, is_accepted))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Failed to add participant to event: {str(e)}")

    def upsert_event_participant(self, event_id: int, user_id: int, is_accepted: bool):
        try:
            self.cursor.execute(
                'INSERT INTO event_participants (event_id, user_id, is_accepted) VALUES (?, ?, ?)',
                (event_id, user_id, bool(is_accepted)),
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            try:
                self.cursor.execute(
                    'UPDATE event_participants SET is_accepted = ? WHERE event_id = ? AND user_id = ?',
                    (bool(is_accepted), event_id, user_id),
                )
                self.conn.commit()
                return True
            except Exception:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            return False

    def remove_event_participant(self, event_id: int, user_id: int):
        try:
            self.cursor.execute(
                'DELETE FROM event_participants WHERE event_id = ? AND user_id = ?',
                (event_id, user_id),
            )
            self.conn.commit()
            return True
        except Exception:
            self.conn.rollback()
            return False

    def get_event_participants(self, event_id: int):
        self.cursor.execute(
            'SELECT user_id, is_accepted FROM event_participants WHERE event_id = ?',
            (event_id,),
        )
        return self.cursor.fetchall()

    def set_event_participants_acceptance(self, event_id: int, user_ids: list[int], is_accepted: bool):
        if not user_ids:
            return True
        try:
            self.cursor.executemany(
                'UPDATE event_participants SET is_accepted = ? WHERE event_id = ? AND user_id = ?',
                [(bool(is_accepted), event_id, uid) for uid in user_ids],
            )
            self.conn.commit()
            return True
        except Exception:
            self.conn.rollback()
            return False

    def get_events_by_user(self, user_id):
        self.cursor.execute('''
            SELECT e.title, e.description, e.start_time, e.end_time, u.username, g.name
            FROM events e
            LEFT JOIN event_participants ep ON e.id = ep.event_id
            LEFT JOIN users u ON e.creator_id = u.id
            LEFT JOIN groups g ON e.group_id = g.id
            WHERE (ep.user_id = ? OR e.creator_id = ?)
              AND (ep.is_accepted = 1 OR e.creator_id = ?)
            ORDER BY e.start_time
        ''', (user_id, user_id, user_id))
        return self.cursor.fetchall()

    def check_conflict(self, user_id, start_time, end_time):
        start = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')

        self.cursor.execute('''
            SELECT start_time, end_time FROM events e
            LEFT JOIN event_participants ep ON e.id = ep.event_id
            WHERE (ep.user_id = ? OR e.creator_id = ?)
              AND (ep.is_accepted = 1 OR e.creator_id = ?)
        ''', (user_id, user_id, user_id))

        for s, e in self.cursor.fetchall():
            if start < datetime.strptime(e, '%Y-%m-%d %H:%M:%S') and end > datetime.strptime(s, '%Y-%m-%d %H:%M:%S'):
                return True
        return False

    def check_conflict_excluding_event(self, user_id, start_time, end_time, exclude_event_id: int):
        start = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')

        self.cursor.execute(
            '''
            SELECT e.id, e.start_time, e.end_time
            FROM events e
            LEFT JOIN event_participants ep ON e.id = ep.event_id
            WHERE e.id != ?
              AND (ep.user_id = ? OR e.creator_id = ?)
              AND (ep.is_accepted = 1 OR e.creator_id = ?)
            ''',
            (exclude_event_id, user_id, user_id, user_id),
        )

        for _, s, e in self.cursor.fetchall():
            if start < datetime.strptime(e, '%Y-%m-%d %H:%M:%S') and end > datetime.strptime(s, '%Y-%m-%d %H:%M:%S'):
                return True
        return False

    # ---------- Conflictos ----------
    def clear_event_conflicts(self, event_id: int):
        try:
            self.cursor.execute('DELETE FROM event_conflicts WHERE event_id = ?', (event_id,))
            self.conn.commit()
            return True
        except Exception:
            self.conn.rollback()
            return False

    # ---------- Grupos ----------
    def add_group(self, name, description="", is_hierarchical=False, creator_id=None):
        try:
            self.cursor.execute('''
                INSERT INTO groups (name, description, is_hierarchical, creator_id)
                VALUES (?, ?, ?, ?)
            ''', (name, description, is_hierarchical, creator_id))
            self.conn.commit()
            group_id = self.cursor.lastrowid
            if creator_id:
                self.add_user_to_group(creator_id, group_id, True)
            return group_id
        except sqlite3.IntegrityError:
            return None

    def add_user_to_group(self, user_id, group_id, is_leader=False):
        try:
            self.cursor.execute('INSERT INTO user_groups (user_id, group_id, is_leader) VALUES (?, ?, ?)',
                                (user_id, group_id, is_leader))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def invite_user_to_group(self, group_id, invited_user_id, inviter_user_id):
        # Verificar si el usuario ya es miembro del grupo
        self.cursor.execute('''
            SELECT 1 FROM user_groups
            WHERE user_id = ? AND group_id = ?
        ''', (invited_user_id, group_id))
        if self.cursor.fetchone():
            return False  # Ya es miembro del grupo

        # Verificar si ya existe una invitación pendiente
        self.cursor.execute('''
            SELECT 1 FROM group_invitations
            WHERE group_id = ? AND invited_user_id = ? AND status = 'pending'
        ''', (group_id, invited_user_id))
        if self.cursor.fetchone():
            return False  # Ya tiene una invitación pendiente

        # Crear la invitación
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            self.cursor.execute('''
                INSERT INTO group_invitations (group_id, invited_user_id, inviter_user_id, created_at)
                VALUES (?, ?, ?, ?)
            ''', (group_id, invited_user_id, inviter_user_id, created_at))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def get_groups_by_user(self, user_id):
        self.cursor.execute('''
            SELECT g.id, g.name, g.is_hierarchical
            FROM groups g
            JOIN user_groups ug ON g.id = ug.group_id
            WHERE ug.user_id = ?
        ''', (user_id,))
        return self.cursor.fetchall()

    def get_group_members(self, group_id):
        self.cursor.execute('''
            SELECT u.id, u.username
            FROM users u
            JOIN user_groups ug ON u.id = ug.user_id
            WHERE ug.group_id = ?
        ''', (group_id,))
        return self.cursor.fetchall()

    def get_group_member_ids(self, group_id: int) -> list[int]:
        self.cursor.execute(
            'SELECT user_id FROM user_groups WHERE group_id = ?',
            (group_id,),
        )
        return [r[0] for r in self.cursor.fetchall()]

    def get_pending_invitations(self, user_id):
        self.cursor.execute('''
            SELECT gi.id, g.name, u.username, gi.created_at, gi.group_id
            FROM group_invitations gi
            JOIN groups g ON gi.group_id = g.id
            JOIN users u ON gi.inviter_user_id = u.id
            WHERE gi.invited_user_id = ? AND gi.status = 'pending'
            ORDER BY gi.created_at DESC
        ''', (user_id,))
        return self.cursor.fetchall()

    def respond_to_invitation(self, invitation_id, response, user_id):
        self.cursor.execute('''
            UPDATE group_invitations
            SET status = ?
            WHERE id = ? AND invited_user_id = ?
        ''', (response, invitation_id, user_id))
        if response == 'accepted':
            self.cursor.execute('SELECT group_id FROM group_invitations WHERE id = ?', (invitation_id,))
            group_id = self.cursor.fetchone()[0]
            self.cursor.execute('INSERT INTO user_groups (user_id, group_id, is_leader) VALUES (?, ?, ?)',
                                (user_id, group_id, False))
        self.conn.commit()
        return True

    def update_group(self, group_id, name=None, description=None):
        """Actualizar nombre y/o descripción de un grupo"""
        updates = []
        params = []

        if name is not None:
            updates.append("name = ?")
            params.append(name)

        if description is not None:
            updates.append("description = ?")
            params.append(description)

        if not updates:
            return True

        params.append(group_id)
        query = f"UPDATE groups SET {', '.join(updates)} WHERE id = ?"

        try:
            self.cursor.execute(query, params)
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def remove_user_from_group(self, user_id, group_id):
        """Eliminar un usuario de un grupo"""
        try:
            self.cursor.execute('DELETE FROM user_groups WHERE user_id = ? AND group_id = ?',
                              (user_id, group_id))
            self.conn.commit()
            return True
        except Exception:
            return False

    def is_group_leader(self, user_id, group_id):
        """Verificar si un usuario es líder de un grupo"""
        self.cursor.execute('''
            SELECT is_leader FROM user_groups
            WHERE user_id = ? AND group_id = ?
        ''', (user_id, group_id))
        result = self.cursor.fetchone()
        return result and result[0]

    def get_group_info(self, group_id):
        """Obtener información completa de un grupo"""
        self.cursor.execute('''
            SELECT id, name, description, is_hierarchical, creator_id
            FROM groups WHERE id = ?
        ''', (group_id,))
        return self.cursor.fetchone()

    def delete_group(self, group_id):
        """Eliminar un grupo y todas sus relaciones (invitaciones, miembros, eventos)"""
        try:
            # Eliminar invitaciones pendientes del grupo
            self.cursor.execute('DELETE FROM group_invitations WHERE group_id = ?', (group_id,))

            # Eliminar miembros del grupo
            self.cursor.execute('DELETE FROM user_groups WHERE group_id = ?', (group_id,))

            # Eliminar participantes de eventos del grupo
            self.cursor.execute('''
                DELETE FROM event_participants
                WHERE event_id IN (SELECT id FROM events WHERE group_id = ?)
            ''', (group_id,))

            # Eliminar eventos del grupo
            self.cursor.execute('DELETE FROM events WHERE group_id = ?', (group_id,))

            # Eliminar el grupo
            self.cursor.execute('DELETE FROM groups WHERE id = ?', (group_id,))

            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error al eliminar grupo: {e}")
            self.conn.rollback()
            return False

    # ---------- Conflictos ----------
    def add_event_conflict(self, event_id: int, user_id: int, reason: str = "") -> bool:
        """Registrar un conflicto detectado para un usuario en un evento."""
        created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            self.cursor.execute(
                '''
                INSERT INTO event_conflicts (event_id, user_id, reason, created_at)
                VALUES (?, ?, ?, ?)
                ''',
                (event_id, user_id, reason or "", created_at),
            )
            self.conn.commit()
            return True
        except Exception:
            self.conn.rollback()
            return False

    def get_user_event_conflicts(self, user_id: int, limit: int = 50):
        """Obtener conflictos registrados para un usuario."""
        self.cursor.execute(
            '''
            SELECT ec.id, ec.event_id, e.title, e.start_time, e.end_time, ec.reason, ec.created_at
            FROM event_conflicts ec
            JOIN events e ON e.id = ec.event_id
            WHERE ec.user_id = ?
            ORDER BY ec.created_at DESC
            LIMIT ?
            ''',
            (user_id, limit),
        )
        return self.cursor.fetchall()
