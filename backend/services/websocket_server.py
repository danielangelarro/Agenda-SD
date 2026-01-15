import asyncio
import websockets
import json
import logging
import os
from services.websocket_manager import websocket_manager
from services.auth_service import AuthService
from services.session_manager import SessionManager

logging.basicConfig(level=logging.INFO)
auth_service = AuthService()
session_manager = SessionManager()

async def websocket_handler(websocket):
    """Manejador principal de conexiones WebSocket"""
    user_id = None
    
    try:
        # Esperar autenticación inicial
        auth_message = await websocket.recv()
        auth_data = json.loads(auth_message)
        
        if auth_data.get('type') == 'auth':
            token = auth_data.get('token')
            requested_user_id = auth_data.get('user_id')

            # Validar sesión
            session_data = session_manager.get_session(token)
            if not session_data:
                await websocket.send(json.dumps({
                    "type": "auth_error",
                    "message": "Sesión inválida o expirada"
                }))
                await websocket.close()
                return

            # Si envían user_id, debe coincidir con el token; si no, usamos el del token
            user_id = session_data.get("user_id")
            if requested_user_id is not None and int(requested_user_id) != int(user_id):
                await websocket.send(json.dumps({
                    "type": "auth_error",
                    "message": "El usuario no coincide con la sesión"
                }))
                await websocket.close()
                return

            await websocket_manager.connect(websocket, int(user_id))
            logging.info(f"Usuario {user_id} conectado via WebSocket desde {websocket.remote_address}")

            await websocket.send(json.dumps({
                "type": "auth_success",
                "message": "Conexión WebSocket establecida",
                "user_id": int(user_id)
            }))
        else:
            await websocket.send(json.dumps({
                "type": "auth_error",
                "message": "Mensaje de autenticación inválido"
            }))
            await websocket.close()
            return
        
        # Mantener conexión activa
        async for message in websocket:
            try:
                data = json.loads(message)
                if data.get('type') == 'ping':
                    await websocket.send(json.dumps({"type": "pong"}))
                    
            except json.JSONDecodeError:
                logging.error("Mensaje JSON inválido")
    
    except websockets.exceptions.ConnectionClosed:
        logging.info(f"Conexión WebSocket cerrada para usuario {user_id}")
    finally:
        if user_id:
            await websocket_manager.disconnect(websocket, user_id)

async def start_websocket_server(host='0.0.0.0', port=8767):
    """Iniciar servidor WebSocket"""
    # Usar host desde variable de entorno o default
    host = os.getenv('WEBSOCKET_HOST', host)
    port = int(os.getenv('WEBSOCKET_PORT', port))
    
    # Try to start server, if port is busy try next port
    original_port = port
    while True:
        try:
            server = await websockets.serve(websocket_handler, host, port)
            logging.info(f"Servidor WebSocket iniciado en ws://{host}:{port}")
            return server
        except OSError as e:
            if "Only one usage of each socket address" in str(e) or "only one usage of each socket address" in str(e).lower():
                logging.warning(f"Puerto {port} ocupado, intentando con {port + 1}")
                port += 1
                if port > original_port + 100:  # Don't go too high
                    raise e
            else:
                raise e
