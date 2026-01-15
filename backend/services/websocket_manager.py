import asyncio
import websockets
import json
from typing import Dict, Set
import logging

class WebSocketManager:
    def __init__(self):
        self.connected_clients: Dict[int, Set[websockets.WebSocketServerProtocol]] = {}
        self.user_connections: Dict[int, websockets.WebSocketServerProtocol] = {}
    
    async def connect(self, websocket: websockets.WebSocketServerProtocol, user_id: int):
        """Registrar nueva conexión de usuario"""
        if user_id not in self.connected_clients:
            self.connected_clients[user_id] = set()
        self.connected_clients[user_id].add(websocket)
        self.user_connections[user_id] = websocket
        
        logging.info(f"Usuario {user_id} conectado. Clientes activos: {len(self.connected_clients)}")
    
    async def disconnect(self, websocket: websockets.WebSocketServerProtocol, user_id: int):
        """Remover conexión desconectada"""
        if user_id in self.connected_clients:
            self.connected_clients[user_id].discard(websocket)
            if not self.connected_clients[user_id]:
                del self.connected_clients[user_id]
        
        logging.info(f"Usuario {user_id} desconectado")
    
    async def send_to_user(self, user_id: int, message: dict):
        """Enviar mensaje a un usuario específico"""
        if user_id in self.connected_clients:
            disconnected = set()
            for websocket in self.connected_clients[user_id]:
                try:
                    await websocket.send(json.dumps(message))
                except websockets.exceptions.ConnectionClosed:
                    disconnected.add(websocket)
            
            # Limpiar conexiones desconectadas
            for websocket in disconnected:
                self.connected_clients[user_id].discard(websocket)
    
    async def broadcast_to_group(self, group_members: list, message: dict, exclude_user: int = None):
        """Transmitir mensaje a todos los miembros de un grupo"""
        for user_id in group_members:
            if user_id != exclude_user:
                await self.send_to_user(user_id, message)

# Instancia global del manager
websocket_manager = WebSocketManager()