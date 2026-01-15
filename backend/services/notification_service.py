from database.repository import Database
from services.websocket_manager import websocket_manager
from services.hierarchy_service import HierarchyService
import logging
from datetime import datetime, timedelta
import asyncio

class NotificationService:
    def __init__(self):
        self.db = Database()
        self.hierarchy = HierarchyService()
        self.notified_events = set()  # Track events already notified
    
    async def notify_group_event(self, event_id: int, group_id: int, creator_id: int):
        """Notificar a miembros del grupo sobre nuevo evento"""
        members = self.db.get_group_members(group_id)
        
        for member_id, username in members:
            if member_id != creator_id:
                await websocket_manager.send_to_user(member_id, {
                    "type": "group_event_invitation",
                    "event_id": event_id,
                    "group_id": group_id,
                    "timestamp": datetime.now().isoformat()
                })
    
    async def notify_event_update(self, event_id: int, update_type: str):
        """Notificar actualización de evento a participantes"""
        self.db.cursor.execute('''
            SELECT user_id FROM event_participants WHERE event_id = ?
        ''', (event_id,))
        
        participants = self.db.cursor.fetchall()
        
        for participant in participants:
            user_id = participant[0]
            await websocket_manager.send_to_user(user_id, {
                "type": f"event_{update_type}",
                "event_id": event_id,
                "timestamp": datetime.now().isoformat()
            })
    
    async def notify_hierarchical_event(self, group_id: int, event_title: str, 
                                      leader_id: int, affected_members: list):
        """Notificar sobre evento jerárquico aplicado"""
        leader_name = self.db.get_username(leader_id)
        
        for member_id in affected_members:
            await websocket_manager.send_to_user(member_id, {
                "type": "hierarchical_event_notification",
                "group_id": group_id,
                "event_title": event_title,
                "leader_name": leader_name,
                "timestamp": datetime.now().isoformat(),
                "message": f"El líder {leader_name} ha programado un evento obligatorio: {event_title}"
            })
    
    def get_user_notifications(self, user_id: int, limit: int = 20):
        """Obtener notificaciones recientes del usuario"""
        # Podría extenderse con una tabla de notificaciones persistente
        # Por ahora usamos notificaciones en tiempo real via WebSocket
        return []  # Placeholder para notificaciones persistentes futuras
    
    async def check_upcoming_events(self):
        """Verificar eventos próximos y enviar recordatorios (1 día antes)"""
        try:
            # Get all events happening in the next 24-48 hours
            tomorrow = datetime.now() + timedelta(days=1)
            day_after = datetime.now() + timedelta(days=2)
            
            tomorrow_str = tomorrow.strftime('%Y-%m-%d 00:00:00')
            day_after_str = day_after.strftime('%Y-%m-%d 00:00:00')
            
            self.db.cursor.execute('''
                SELECT e.id, e.title, e.start_time, e.end_time, e.creator_id
                FROM events e
                WHERE e.start_time >= ? AND e.start_time < ?
            ''', (tomorrow_str, day_after_str))
            
            upcoming_events = self.db.cursor.fetchall()
            
            for event in upcoming_events:
                event_id, title, start_time, end_time, creator_id = event
                
                # Skip if already notified
                if event_id in self.notified_events:
                    continue
                
                # Get all participants for this event
                self.db.cursor.execute('''
                    SELECT user_id FROM event_participants WHERE event_id = ?
                ''', (event_id,))
                
                participants = self.db.cursor.fetchall()
                
                # Notify all participants
                for participant in participants:
                    user_id = participant[0]
                    await websocket_manager.send_to_user(user_id, {
                        "type": "event_reminder",
                        "event_id": event_id,
                        "event_title": title,
                        "start_time": start_time,
                        "end_time": end_time,
                        "timestamp": datetime.now().isoformat(),
                        "message": f"Recordatorio: El evento '{title}' es mañana a las {start_time.split()[1]}"
                    })
                
                # Mark as notified
                self.notified_events.add(event_id)
                logging.info(f"Sent reminder for event {event_id}: {title}")
                
        except Exception as e:
            logging.error(f"Error checking upcoming events: {str(e)}")
    
    async def start_reminder_scheduler(self):
        """Iniciar el scheduler que verifica eventos cada hora"""
        while True:
            try:
                await self.check_upcoming_events()
                # Check every hour
                await asyncio.sleep(3600)
            except Exception as e:
                logging.error(f"Error in reminder scheduler: {str(e)}")
                await asyncio.sleep(3600)