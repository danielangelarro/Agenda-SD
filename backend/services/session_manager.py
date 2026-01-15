"""Gestor de sesiones para mantener sesiones activas entre recargas"""
import json
import os
import time
from pathlib import Path
import secrets

class SessionManager:
    def __init__(self, session_dir=".sessions"):
        self.session_dir = Path(session_dir)
        self.session_dir.mkdir(exist_ok=True)
        self.session_timeout = 3600 * 24  # 24 horas

    def create_session(self, username, user_id):
        """Crear una nueva sesión y retornar el token"""
        token = secrets.token_urlsafe(32)
        session_data = {
            "username": username,
            "user_id": user_id,
            "created_at": time.time(),
            "last_activity": time.time()
        }

        session_file = self.session_dir / f"{token}.json"
        with open(session_file, 'w') as f:
            json.dump(session_data, f)

        return token

    def get_session(self, token):
        """Obtener datos de sesión por token"""
        if not token:
            return None

        session_file = self.session_dir / f"{token}.json"

        if not session_file.exists():
            return None

        try:
            with open(session_file, 'r') as f:
                session_data = json.load(f)

            # Verificar si la sesión ha expirado
            if time.time() - session_data['last_activity'] > self.session_timeout:
                self.delete_session(token)
                return None

            # Actualizar última actividad
            session_data['last_activity'] = time.time()
            with open(session_file, 'w') as f:
                json.dump(session_data, f)

            return session_data

        except Exception:
            return None

    def delete_session(self, token):
        """Eliminar una sesión"""
        if not token:
            return

        session_file = self.session_dir / f"{token}.json"
        if session_file.exists():
            session_file.unlink()

    def cleanup_old_sessions(self):
        """Limpiar sesiones expiradas"""
        current_time = time.time()
        for session_file in self.session_dir.glob("*.json"):
            try:
                with open(session_file, 'r') as f:
                    session_data = json.load(f)

                if current_time - session_data['last_activity'] > self.session_timeout:
                    session_file.unlink()
            except Exception:
                # Si hay error al leer, eliminar el archivo
                session_file.unlink()
