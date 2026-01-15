import websockets
import asyncio
import json
import os
import logging
import atexit
import warnings
import threading
from urllib.parse import urlparse
from queue import Queue, Empty

# Suppress specific websocket warnings
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*Event loop is closed.*')
warnings.filterwarnings('ignore', category=ResourceWarning, message='.*unclosed.*')

class WebSocketClient:
    def __init__(self):
        self.websocket = None
        self.host = os.getenv('WEBSOCKET_HOST', 'localhost')
        self.port = os.getenv('WEBSOCKET_PORT', '8767')
        self.url = f"ws://{self.host}:{self.port}"
        self.user_id = None
        self.connected = False
        self.message_handlers = {}
        self.logger = logging.getLogger(__name__)
        self.listening = False
        self.listener_task = None
        self.should_stop = False
        self._cleanup_registered = False
        self._incoming: "Queue[dict]" = Queue()
        self._bg_thread: threading.Thread | None = None
        self._bg_stop_event = threading.Event()
        self._bg_running = False
        self._bg_user_id: int | None = None
        self._last_token: str | None = None
        
        # Register cleanup on exit
        if not self._cleanup_registered:
            atexit.register(self._sync_cleanup)
            self._cleanup_registered = True

    def configure_from_base(self, base_url: str, ws_port: str | None = None):
        """Ajusta host/puerto del WS para alinearlo con el coordinador activo."""
        if not base_url:
            return
        parsed = urlparse(base_url)
        host = parsed.hostname or self.host
        port = ws_port or self.port
        new_url = f"ws://{host}:{port}"
        if new_url == self.url:
            return

        was_running = self._bg_thread and self._bg_thread.is_alive()
        bg_user_id = self._bg_user_id
        token = self._last_token

        if was_running:
            self.stop_background()

        self.host = host
        self.port = port
        self.url = new_url

        if was_running and bg_user_id and token:
            self.start_background(bg_user_id, token)
        
    async def connect(self, user_id, token: str):
        """Connect to the WebSocket server"""
        try:
            self.websocket = await websockets.connect(self.url)
            self.user_id = user_id
            
            # Authenticate with the server
            auth_message = {
                "type": "auth",
                "user_id": user_id,
                "token": token,
            }
            await self.websocket.send(json.dumps(auth_message))
            
            # Wait for authentication response
            response = await self.websocket.recv()
            response_data = json.loads(response)
            
            if response_data.get("type") == "auth_success":
                self.connected = True
                self.logger.info(f"Connected to WebSocket server at {self.url}")
                return True
            else:
                self.logger.error(f"Authentication failed: {response_data}")
                await self.websocket.close()
                self.websocket = None
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to connect to WebSocket server: {e}")
            return False
    
    async def disconnect(self):
        """Disconnect from the WebSocket server"""
        self.should_stop = True
        self.listening = False
        
        # Cancel listener task if it exists
        if self.listener_task and not self.listener_task.done():
            self.listener_task.cancel()
            try:
                await asyncio.wait_for(self.listener_task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass  # Ignore cancellation and timeout errors
        
        # Close websocket connection
        if self.websocket:
            try:
                await asyncio.wait_for(self.websocket.close(), timeout=2.0)
            except (asyncio.TimeoutError, Exception):
                pass  # Ignore errors during close
            finally:
                self.websocket = None
                self.connected = False
                self.logger.info("Disconnected from WebSocket server")
    
    def _sync_cleanup(self):
        """Synchronous cleanup for atexit"""
        if self.websocket or self.listener_task:
            try:
                # Try to get or create an event loop
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_closed():
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                # Run cleanup
                if not loop.is_closed():
                    loop.run_until_complete(self._async_cleanup())
            except Exception:
                pass  # Ignore all errors during cleanup
    
    async def _async_cleanup(self):
        """Async cleanup helper"""
        self.should_stop = True
        self.listening = False
        
        # Cancel listener task
        if self.listener_task and not self.listener_task.done():
            self.listener_task.cancel()
            try:
                await asyncio.wait_for(self.listener_task, timeout=1.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            except Exception:
                pass
        
        # Close websocket connection
        if self.websocket:
            try:
                # Set a timeout for closing
                close_task = asyncio.create_task(self.websocket.close())
                await asyncio.wait_for(close_task, timeout=1.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                # Force close if timeout
                if self.websocket and hasattr(self.websocket, 'transport'):
                    try:
                        self.websocket.transport.close()
                    except:
                        pass
            except Exception:
                pass
            finally:
                self.websocket = None
        
        self.connected = False
        
        # Cancel any remaining tasks in the current event loop
        try:
            loop = asyncio.get_event_loop()
            if not loop.is_closed():
                pending = [task for task in asyncio.all_tasks(loop) 
                          if not task.done() and task != asyncio.current_task()]
                for task in pending:
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
        except Exception:
            pass
    
    async def send_message(self, message):
        """Send a message to the server"""
        if not self.connected or not self.websocket:
            return False
            
        try:
            await self.websocket.send(json.dumps(message))
            return True
        except Exception:
            return False
    
    async def listen(self):
        """Listen for messages from the server"""
        if not self.connected or not self.websocket:
            return
            
        if self.listening:
            return
            
        self.listening = True
        self.should_stop = False
        self.logger.info("Starting to listen for WebSocket messages")
        
        try:
            while not self.should_stop and self.connected and self.websocket:
                try:
                    message = await asyncio.wait_for(self.websocket.recv(), timeout=1.0)
                    data = json.loads(message)
                    message_type = data.get("type")

                    if message_type == "ping":
                        await self.send_message({"type": "pong"})
                        continue

                    # Encolar para procesar en el thread principal de Streamlit.
                    if isinstance(data, dict):
                        self._incoming.put(data)

                except asyncio.TimeoutError:
                    if self.should_stop:
                        break
                    continue
                except asyncio.CancelledError:
                    self.logger.info("Listen task cancelled")
                    break
                except json.JSONDecodeError:
                    self.logger.error("Invalid JSON message received")
                except websockets.exceptions.ConnectionClosed:
                    self.logger.info("WebSocket connection closed")
                    self.connected = False
                    break
                except Exception as e:
                    if "Event loop is closed" in str(e):
                        self.connected = False
                        break
                    self.logger.error(f"Error while listening for messages: {e}")
                    if self.should_stop:
                        break
                    continue
        finally:
            self.listening = False
            self.should_stop = True
            self.logger.info("Stopped listening for WebSocket messages")

    def register_handler(self, message_type, handler):
        """Register a handler for a specific message type"""
        if message_type not in self.message_handlers:
            self.message_handlers[message_type] = []
        self.message_handlers[message_type].append(handler)
    
    def unregister_handler(self, message_type, handler):
        """Unregister a handler for a specific message type"""
        if message_type in self.message_handlers:
            if handler in self.message_handlers[message_type]:
                self.message_handlers[message_type].remove(handler)
            if not self.message_handlers[message_type]:
                del self.message_handlers[message_type]

    def drain_messages(self, max_items: int = 200) -> list[dict]:
        """Drenar mensajes recibidos (thread-safe) para procesarlos en el hilo principal."""
        drained: list[dict] = []
        for _ in range(max_items):
            try:
                drained.append(self._incoming.get_nowait())
            except Empty:
                break
        return drained

    def dispatch_pending(self, max_items: int = 200) -> int:
        """Ejecuta handlers registrados para los mensajes encolados."""
        msgs = self.drain_messages(max_items=max_items)
        for data in msgs:
            mtype = data.get("type")
            if mtype in self.message_handlers:
                for handler in list(self.message_handlers[mtype]):
                    try:
                        handler(data)
                    except Exception as e:
                        self.logger.error(f"Error in message handler: {e}")
        return len(msgs)

    def start_background(self, user_id: int, token: str):
        """Inicia conexión/listen en background para evitar que se caiga por reruns."""
        if self._bg_thread and self._bg_thread.is_alive() and self._bg_user_id == int(user_id):
            return

        # Si estaba corriendo para otro usuario, detener primero.
        if self._bg_thread and self._bg_thread.is_alive():
            self.stop_background()

        self._bg_stop_event = threading.Event()
        self._bg_user_id = int(user_id)
        self._last_token = token

        def _runner():
            self._bg_running = True
            try:
                asyncio.run(self._background_main(int(user_id), token))
            except Exception as e:
                self.logger.error(f"Background WS runner error: {e}")
            finally:
                self._bg_running = False

        self._bg_thread = threading.Thread(target=_runner, daemon=True)
        self._bg_thread.start()

    def stop_background(self, timeout_s: float = 2.0):
        """Detiene el listener en background."""
        try:
            self._bg_stop_event.set()
        except Exception:
            pass
        self.should_stop = True
        self.connected = False
        t = self._bg_thread
        if t and t.is_alive():
            t.join(timeout=timeout_s)
        self._bg_thread = None
        self._bg_user_id = None

    async def _background_main(self, user_id: int, token: str):
        """Loop de reconexión y recepción."""
        backoff = 0.5
        while not self._bg_stop_event.is_set():
            try:
                ok = await self.connect(user_id, token)
                if not ok:
                    await asyncio.sleep(backoff)
                    backoff = min(5.0, backoff * 2)
                    continue

                backoff = 0.5
                self.should_stop = False
                await self.listen()
            except Exception:
                self.connected = False
            finally:
                try:
                    if self.websocket:
                        await self.websocket.close()
                except Exception:
                    pass
                self.websocket = None
                self.connected = False

            if not self._bg_stop_event.is_set():
                await asyncio.sleep(backoff)
                backoff = min(5.0, backoff * 2)
