"""
Tests para el módulo de almacenamiento offline.

Ejecutar con: python -m pytest tests/test_offline_storage.py -v
"""

import os
import sys
import tempfile
import pytest
from datetime import datetime

# Agregar path del proyecto
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from front.services.offline_storage import OfflineStorage


@pytest.fixture
def temp_db():
    """Crea una base de datos temporal para tests."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    yield db_path
    # Cleanup
    try:
        os.unlink(db_path)
    except Exception:
        pass


@pytest.fixture
def storage(temp_db):
    """Crea una instancia de OfflineStorage para tests."""
    # Limpiar instancias previas
    OfflineStorage.clear_all_instances()
    storage = OfflineStorage(user_id=999, db_path=temp_db)
    yield storage
    storage.close()


class TestSessionManagement:
    """Tests para gestión de sesión."""
    
    def test_save_and_get_session(self, storage):
        """Debe guardar y recuperar sesión correctamente."""
        storage.save_session(
            user_id=1,
            username="testuser",
            token="test_token_123",
            password="secret123"
        )
        
        session = storage.get_session()
        
        assert session is not None
        assert session['user_id'] == 1
        assert session['username'] == "testuser"
        assert session['token'] == "test_token_123"
        assert session['password_hash'] is not None
    
    def test_verify_offline_password(self, storage):
        """Debe verificar contraseña offline correctamente."""
        storage.save_session(
            user_id=1,
            username="testuser",
            token="token",
            password="correctpassword"
        )
        
        assert storage.verify_offline_password("correctpassword") is True
        assert storage.verify_offline_password("wrongpassword") is False
    
    def test_clear_session(self, storage):
        """Debe eliminar la sesión."""
        storage.save_session(user_id=1, username="test", token="token")
        storage.clear_session()
        
        assert storage.get_session() is None


class TestEventCaching:
    """Tests para cacheo de eventos."""
    
    def test_cache_events(self, storage):
        """Debe cachear lista de eventos."""
        events = [
            {"id": 1, "title": "Evento 1", "description": "Desc 1"},
            {"id": 2, "title": "Evento 2", "description": "Desc 2"},
        ]
        
        storage.cache_events(events)
        cached = storage.get_cached_events()
        
        assert len(cached) == 2
        assert cached[0]['title'] == "Evento 1"
        assert cached[1]['title'] == "Evento 2"
    
    def test_cache_single_event(self, storage):
        """Debe cachear un solo evento."""
        event = {"id": 1, "title": "Mi Evento"}
        
        storage.cache_event("1", event)
        cached = storage.get_cached_events()
        
        assert len(cached) == 1
        assert cached[0]['title'] == "Mi Evento"
    
    def test_dirty_events(self, storage):
        """Debe trackear eventos modificados localmente."""
        # Evento del servidor (no dirty)
        storage.cache_event("1", {"id": 1, "title": "Server Event"}, is_dirty=False)
        
        # Evento local (dirty)
        storage.cache_event("local_123", {"title": "Local Event"}, is_dirty=True)
        
        dirty = storage.get_dirty_events()
        
        assert len(dirty) == 1
        assert dirty[0]['title'] == "Local Event"
    
    def test_mark_event_synced(self, storage):
        """Debe marcar evento como sincronizado."""
        storage.cache_event("local_123", {"title": "Local"}, is_dirty=True)
        
        assert len(storage.get_dirty_events()) == 1
        
        storage.mark_event_synced("local_123", server_id=100)
        
        assert len(storage.get_dirty_events()) == 0
    
    def test_mark_event_deleted(self, storage):
        """Debe marcar evento como eliminado."""
        storage.cache_event("1", {"id": 1, "title": "Event"})
        
        assert len(storage.get_cached_events()) == 1
        
        storage.mark_event_deleted("1")
        
        assert len(storage.get_cached_events()) == 0
        assert len(storage.get_cached_events(include_deleted=True)) == 1


class TestGroupCaching:
    """Tests para cacheo de grupos."""
    
    def test_cache_groups(self, storage):
        """Debe cachear lista de grupos."""
        groups = [
            {"id": 1, "name": "Grupo 1"},
            {"id": 2, "name": "Grupo 2"},
        ]
        
        storage.cache_groups(groups)
        cached = storage.get_cached_groups()
        
        assert len(cached) == 2
    
    def test_cache_group_members(self, storage):
        """Debe cachear miembros de grupo."""
        members = [
            {"id": 1, "username": "user1"},
            {"id": 2, "username": "user2"},
        ]
        
        storage.cache_group_members("group_1", members)
        cached = storage.get_cached_group_members("group_1")
        
        assert len(cached) == 2
        assert cached[0]['username'] == "user1"


class TestUserCaching:
    """Tests para cacheo de usuarios."""
    
    def test_cache_users(self, storage):
        """Debe cachear lista de usuarios."""
        users = [
            {"id": 1, "username": "alice"},
            {"id": 2, "username": "bob"},
        ]
        
        storage.cache_users(users)
        cached = storage.get_cached_users()
        
        assert len(cached) == 2
    
    def test_get_cached_user(self, storage):
        """Debe obtener usuario específico."""
        storage.cache_users([{"id": 1, "username": "alice"}])
        
        user = storage.get_cached_user(1)
        
        assert user is not None
        assert user['username'] == "alice"
        
        assert storage.get_cached_user(999) is None


class TestInvitationCaching:
    """Tests para cacheo de invitaciones."""
    
    def test_cache_group_invitations(self, storage):
        """Debe cachear invitaciones de grupo."""
        invitations = [
            {"id": 1, "group_id": 10, "status": "pending"},
        ]
        
        storage.cache_invitations(invitations, "group")
        cached = storage.get_cached_invitations("group")
        
        assert len(cached) == 1
        assert cached[0]['status'] == "pending"
    
    def test_cache_event_invitations(self, storage):
        """Debe cachear invitaciones de evento."""
        invitations = [
            {"id": 1, "event_id": 20, "is_accepted": False},
        ]
        
        storage.cache_invitations(invitations, "event")
        cached = storage.get_cached_invitations("event")
        
        assert len(cached) == 1


class TestSyncMetadata:
    """Tests para metadatos de sincronización."""
    
    def test_set_and_get_last_sync(self, storage):
        """Debe guardar y recuperar última sincronización."""
        now = datetime.now()
        
        storage.set_last_sync("events", now)
        
        last_sync = storage.get_last_sync("events")
        
        assert last_sync is not None
        assert last_sync.date() == now.date()
    
    def test_get_sync_stats(self, storage):
        """Debe obtener estadísticas de sincronización."""
        storage.cache_events([{"id": 1, "title": "Event"}])
        storage.cache_groups([{"id": 1, "name": "Group"}])
        
        stats = storage.get_sync_stats()
        
        assert stats['events_count'] == 1
        assert stats['groups_count'] == 1
    
    def test_get_storage_summary(self, storage):
        """Debe obtener resumen del almacenamiento."""
        storage.save_session(user_id=1, username="test", token="token")
        storage.cache_events([{"id": 1, "title": "Event"}])
        storage.cache_event("local_1", {"title": "Local"}, is_dirty=True)
        
        summary = storage.get_storage_summary()
        
        assert summary['user_id'] == 999
        assert summary['counts']['events'] == 2
        assert summary['counts']['events_pending_sync'] == 1
        assert summary['has_pending_operations'] is True


class TestCleanup:
    """Tests para limpieza y mantenimiento."""
    
    def test_clear_all_cache(self, storage):
        """Debe limpiar todo el caché."""
        storage.cache_events([{"id": 1, "title": "Event"}])
        storage.cache_groups([{"id": 1, "name": "Group"}])
        storage.save_session(user_id=1, username="test", token="token")
        
        storage.clear_all_cache()
        
        assert len(storage.get_cached_events()) == 0
        assert len(storage.get_cached_groups()) == 0
        # La sesión no debe afectarse
        assert storage.get_session() is not None
    
    def test_clear_synced_deleted(self, storage):
        """Debe eliminar registros eliminados y sincronizados."""
        storage.cache_event("1", {"id": 1, "title": "Event"})
        storage.mark_event_deleted("1")
        storage.mark_event_synced("1")
        
        storage.clear_synced_deleted()
        
        assert len(storage.get_cached_events(include_deleted=True)) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
