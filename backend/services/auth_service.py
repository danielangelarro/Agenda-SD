from database.repository import Database

class AuthService:
    def __init__(self):
        self.db = Database()

    def register(self, username: str, password: str) -> bool:
        """Registrar nuevo usuario."""
        return self.db.add_user(username, password)

    def login(self, username: str, password: str) -> bool:
        """Verificar credenciales."""
        return self.db.check_password(username, password)

    def get_user_id(self, username: str):
        """Obtener ID a partir del nombre de usuario."""
        return self.db.get_user_id(username)

    def get_username(self, user_id: int):
        """Obtener username a partir del ID."""
        return self.db.get_username(user_id)

    def list_users(self):
        """Listar todos los usuarios registrados."""
        return self.db.get_all_users()
