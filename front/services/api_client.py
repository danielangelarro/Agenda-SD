import requests
import os
import time
import threading
from typing import Optional, List
from urllib.parse import urlparse

class APIClient:
    def __init__(self):
        # Permite especificar múltiples coordinadores separados por coma para failover simple
        urls_raw = os.getenv('API_BASE_URLS')
        if urls_raw:
            urls = [u.strip().rstrip('/') for u in urls_raw.split(',') if u.strip()]
        else:
            urls = [os.getenv('API_BASE_URL', 'http://localhost:8766').rstrip('/')]

        self.base_urls = urls
        # Selección actual basada en latencia de /health
        self.base_url: Optional[str] = None
        self._last_probe = 0.0
        self._probe_ttl = float(os.getenv("API_COORD_PROBE_TTL", "30") or 30)
        self._probe_interval = float(os.getenv("API_COORD_PROBE_INTERVAL", "5") or 5)
        self._lock = threading.Lock()
        try:
            self._pick_best_base(force=True)
        except Exception:
            # Si no hay coordinadores vivos al arrancar, seguimos y el hilo de probe intentará descubrir luego.
            self.base_url = None
        self._start_probe_thread()

    def _ping_base(self, base_url: str, timeout: float = 1.5) -> Optional[float]:
        """Devuelve latencia de /health o None si falla."""
        try:
            start = time.perf_counter()
            resp = requests.get(f"{base_url}/health", timeout=timeout)
            resp.raise_for_status()
            if resp.json().get("service") != "coordinator":
                return None
            return time.perf_counter() - start
        except Exception:
            return None

    def _alive_bases(self) -> list[str]:
        """Devuelve lista de coordinadores vivos ordenados por latencia."""
        alive = []
        for candidate in self.base_urls:
            latency = self._ping_base(candidate)
            if latency is None:
                continue
            alive.append((latency, candidate))
        alive.sort(key=lambda x: x[0])
        return [c for _, c in alive]

    def _normalize_url(self, url: str) -> str:
        return url.strip().rstrip("/")

    def _refresh_peers(self):
        """Descubre coordinadores adicionales consultando /coordinators/peers."""
        candidates = list(self.base_urls)
        updated = False
        for base in candidates:
            try:
                resp = requests.get(f"{base}/coordinators/peers", timeout=2.0)
                resp.raise_for_status()
                data = resp.json()
                for c in data.get("coordinators", []):
                    if isinstance(c, str):
                        norm = self._normalize_url(c)
                        with self._lock:
                            if norm and norm not in self.base_urls:
                                self.base_urls.append(norm)
                                updated = True
            except Exception:
                continue
        if updated:
            # Si hay nuevos peers, forzar una selección fresca
            try:
                self._pick_best_base(force=True)
            except Exception:
                pass

    def _pick_best_base(self, force: bool = False) -> Optional[str]:
        """Elige el coordinador con menor latencia entre los que respondan."""
        now = time.time()
        with self._lock:
            if self.base_url and not force and (now - self._last_probe) < self._probe_ttl:
                return self.base_url

        alive = self._alive_bases()
        with self._lock:
            if alive:
                self.base_url = alive[0]
                self._last_probe = now
                return self.base_url
            # Si no hay vivos y teníamos uno anterior, lo descartamos para evitar quedarnos pegados
            self.base_url = None
        return None

    def get_current_base_url(self) -> Optional[str]:
        """Devuelve el coordinador preferido (refrescando si es necesario)."""
        try:
            return self._pick_best_base()
        except Exception:
            return self.base_url

    def get_current_ws_target(self) -> tuple[Optional[str], str]:
        """Host y puerto WS consistentes con el coordinador activo."""
        base = self.get_current_base_url()
        host = urlparse(base).hostname if base else None
        port = os.getenv('WEBSOCKET_PORT', '8767')
        return host, port
        
    def _make_request(self, method, endpoint, token=None, **kwargs):
        """Make HTTP request to the API"""
        self._refresh_peers()
        self._pick_best_base()
        if not self.base_url:
            raise Exception("No hay coordinador disponible")

        headers = {}
        if token:
            headers['Authorization'] = f"Bearer {token}"
            
        last_error = None
        tried = []
        # Intentar con el coordinador seleccionado; si cae, probar otros vivos por latencia
        for attempt in range(len(self.base_urls) or 1):
            if attempt == 0 and self.base_url:
                base = self.base_url
            else:
                alive = [b for b in self._alive_bases() if b not in tried]
                base = alive[0] if alive else None
                if base:
                    with self._lock:
                        self.base_url = base
                        self._last_probe = time.time()
            tried.append(base)
            if not base:
                break
            url = f"{base}{endpoint}"
            try:
                response = requests.request(method, url, headers=headers, **kwargs)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                # Extraer mensaje de error del servidor si existe
                try:
                    error_detail = response.json().get('detail', str(e))
                except Exception:
                    error_detail = str(e)
                
                # Crear mensajes de error más amigables según el código de estado
                if response.status_code == 401:
                    raise Exception("Usuario o contraseña incorrectos")
                elif response.status_code == 400:
                    raise Exception(f"{error_detail}")
                elif response.status_code == 404:
                    raise Exception("Recurso no encontrado")
                elif response.status_code == 500:
                    raise Exception("Error en el servidor. Por favor, intenta más tarde")
                else:
                    raise Exception(f"Error: {error_detail}")
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
                last_error = e
                continue

        raise Exception(
            f"No se pudo conectar a ningún coordinador ({', '.join(self.base_urls)}). "
            f"Último error: {last_error}"
        )

    def _start_probe_thread(self):
        """Lanza un hilo en segundo plano para mantener fresca la selección de coordinador."""
        if self._probe_interval <= 0:
            return

        def _probe_loop():
            while True:
                try:
                    self._refresh_peers()
                    self._pick_best_base(force=True)
                except Exception:
                    pass
                time.sleep(self._probe_interval)

        t = threading.Thread(target=_probe_loop, daemon=True)
        t.start()
    
    # Auth methods
    def register(self, username: str, password: str):
        """Register a new user"""
        data = {"username": username, "password": password}
        return self._make_request("POST", "/auth/register", json=data)
    
    def login(self, username: str, password: str):
        """Login user and get token"""
        data = {"username": username, "password": password}
        return self._make_request("POST", "/auth/login", json=data)
    
    def list_users(self, token: str):
        """List all users"""
        return self._make_request("GET", "/users", params={"token": token})
    
    # Group methods
    def create_group(self, name: str, description: str, is_hierarchical: bool, 
                     token: str, members: Optional[List[int]] = None):
        """Create a new group"""
        data = {
            "name": name,
            "description": description,
            "is_hierarchical": is_hierarchical,
            "members": members or []
        }
        return self._make_request("POST", "/groups", json=data, params={"token": token})
    
    def list_user_groups(self, token: str):
        """List groups for current user (normalized as tuples)"""
        raw = self._make_request("GET", "/groups", params={"token": token})
        # Backend devuelve lista de dicts; convertimos a (id, name, is_hierarchical)
        groups = []
        for g in raw or []:
            gid = g.get("id") if isinstance(g, dict) else None
            name = g.get("name") if isinstance(g, dict) else str(g)
            is_hier = g.get("is_hierarchical", False) if isinstance(g, dict) else False
            if gid is not None and name is not None:
                groups.append((gid, name, is_hier))
        return groups
    
    def list_group_members(self, group_id: int, token: str):
        """List members of a group"""
        return self._make_request("GET", f"/groups/{group_id}/members", params={"token": token})
    
    def invite_user_to_group(self, group_id: int, invited_user_id: int, token: str):
        """Invite user to a group"""
        params = {"token": token, "group_id": group_id, "invited_user_id": invited_user_id}
        return self._make_request("POST", "/groups/invite", params=params)
    
    def get_pending_invitations(self, token: str):
        """Get pending group invitations"""
        return self._make_request("GET", "/groups/invitations", params={"token": token})
    
    def respond_to_group_invitation(self, invitation_id: int, response: str, token: str):
        """Respond to a group invitation (query params per API contract)"""
        params = {"token": token, "invitation_id": invitation_id, "response": response}
        return self._make_request("POST", "/groups/invitations/respond", params=params)
    
    def get_pending_invitations_count(self, token: str):
        """Get count of pending group invitations"""
        return self._make_request("GET", "/groups/invitations/count", params={"token": token})
    
    def update_group(self, group_id: int, name: Optional[str] = None, description: Optional[str] = None, token: str = ""):
        """Update group information (name and/or description)"""
        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        return self._make_request("PUT", f"/groups/{group_id}", json=data, params={"token": token})
    
    def delete_group(self, group_id: int, token: str):
        """Delete a group completely"""
        return self._make_request("DELETE", f"/groups/{group_id}", params={"token": token})
    
    def remove_member(self, group_id: int, member_id: int, token: str):
        """Remove a member from a group"""
        return self._make_request("DELETE", f"/groups/{group_id}/members/{member_id}", params={"token": token})
    
    def get_group_info(self, group_id: int, token: str):
        """Get complete group information"""
        return self._make_request("GET", f"/groups/{group_id}/info", params={"token": token})

    # Visualization / availability
    def get_group_agendas(self, group_id: int, start_date: str, end_date: str, token: str):
        """Get agendas for a group within a time window."""
        return self._make_request(
            "GET",
            f"/groups/{group_id}/agendas",
            params={"token": token, "start_date": start_date, "end_date": end_date},
        )

    def get_common_availability(self, group_id: int, start_date: str, end_date: str, duration_hours: float, token: str):
        """Get common availability slots for a group."""
        return self._make_request(
            "GET",
            f"/groups/{group_id}/availability/common",
            params={
                "token": token,
                "start_date": start_date,
                "end_date": end_date,
                "duration_hours": duration_hours,
            },
        )

    def get_event_conflicts(self, token: str, limit: int = 50):
        """Get conflicts registered for the current user."""
        return self._make_request("GET", "/events/conflicts", params={"token": token, "limit": limit})

    # Event methods
    def create_event(self, title: str, description: str, start_time: str, end_time: str,
                     token: str, group_id: Optional[int] = None, is_group_event: bool = False,
                     participants_ids: Optional[List[int]] = None, is_hierarchical: bool = False):
        """Create a new event"""
        data = {
            "title": title,
            "description": description,
            "start_time": start_time,
            "end_time": end_time,
            "group_id": group_id,
            "is_group_event": is_group_event,
            "participants_ids": participants_ids or [],
            "is_hierarchical": is_hierarchical
        }
        return self._make_request("POST", "/events", json=data, params={"token": token})

    def update_event(
        self,
        event_id: int,
        token: str,
        title: Optional[str] = None,
        description: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        participants_ids: Optional[List[int]] = None,
    ):
        """Update/reschedule an event (creator only)."""
        data = {}
        if title is not None:
            data["title"] = title
        if description is not None:
            data["description"] = description
        if start_time is not None:
            data["start_time"] = start_time
        if end_time is not None:
            data["end_time"] = end_time
        if participants_ids is not None:
            data["participants_ids"] = participants_ids
        return self._make_request("PUT", f"/events/{event_id}", json=data, params={"token": token})
    
    def get_user_events(self, token: str):
        """Get user events"""
        return self._make_request("GET", "/events", params={"token": token})
    
    def get_user_events_detailed(self, token: str, filter_type: str = "all"):
        """Get detailed user events"""
        return self._make_request("GET", "/events/detailed", params={"token": token, "filter_type": filter_type})
    
    def get_pending_event_invitations(self, token: str):
        """Get pending event invitations"""
        return self._make_request("GET", "/events/invitations", params={"token": token})
    
    def respond_to_event_invitation(self, event_id: int, accepted: bool, token: str):
        """Respond to an event invitation (query params per API contract)"""
        params = {"token": token, "event_id": event_id, "accepted": accepted}
        return self._make_request("POST", "/events/invitations/respond", params=params)
    
    def get_pending_event_invitations_count(self, token: str):
        """Get count of pending event invitations"""
        return self._make_request("GET", "/events/invitations/count", params={"token": token})
    
    def cancel_event(self, event_id: int, token: str):
        """Cancel an event (only for creators)"""
        return self._make_request("DELETE", f"/events/{event_id}", params={"token": token})
    
    def leave_event(self, event_id: int, token: str):
        """Leave an event (only for participants)"""
        return self._make_request("DELETE", f"/events/{event_id}/leave", params={"token": token})
    
    def get_event_details(self, event_id: int, token: str):
        """Get complete event details including participants"""
        return self._make_request("GET", f"/events/{event_id}/details", params={"token": token})
