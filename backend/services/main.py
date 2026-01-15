import asyncio
import uvicorn
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional, List
import os

# Fix imports for the new directory structure
from database.repository import Database
from services.websocket_server import start_websocket_server
from services.auth_service import AuthService
from services.group_service import GroupService
from services.event_service import EventService
from services.session_manager import SessionManager
from services.notification_service import NotificationService

app = FastAPI(title="Agenda Distribuida API", version="1.0.0")

# Inicializar servicios
auth_service = AuthService()
group_service = GroupService()
event_service = EventService()
session_manager = SessionManager()
notification_service = NotificationService()

# Modelos para las solicitudes
class UserLogin(BaseModel):
    username: str
    password: str

class UserRegister(BaseModel):
    username: str
    password: str

class CreateGroup(BaseModel):
    name: str
    description: str
    is_hierarchical: bool
    members: Optional[List[int]] = None

class InviteUser(BaseModel):
    group_id: int
    invited_user_id: int

class RespondInvitation(BaseModel):
    invitation_id: int
    response: str

class CreateEvent(BaseModel):
    title: str
    description: str
    start_time: str
    end_time: str
    group_id: Optional[int] = None
    is_group_event: bool = False
    participants_ids: Optional[List[int]] = None
    is_hierarchical: bool = False

class RespondEventInvitation(BaseModel):
    event_id: int
    accepted: bool

# New model for updating groups
class UpdateGroup(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None

class UpdateEvent(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    participants_ids: Optional[List[int]] = None

# Dependencia para obtener el user_id desde el token
def get_current_user(token: str):
    session_data = session_manager.get_session(token)
    if not session_data:
        raise HTTPException(status_code=401, detail="Sesión inválida o expirada")
    return session_data["user_id"]

@app.on_event("startup")
async def startup_event():
    """Iniciar servidor WebSocket y scheduler de recordatorios al arrancar la aplicación"""
    asyncio.create_task(start_websocket_server())
    asyncio.create_task(notification_service.start_reminder_scheduler())

# Auth endpoints
@app.post("/auth/register")
async def register(user: UserRegister):
    success = auth_service.register(user.username, user.password)
    if success:
        return {"message": "Usuario registrado exitosamente"}
    else:
        raise HTTPException(status_code=400, detail="El nombre de usuario ya existe")

@app.post("/auth/login")
async def login(user: UserLogin):
    if auth_service.login(user.username, user.password):
        user_id = auth_service.get_user_id(user.username)
        token = session_manager.create_session(user.username, user_id)
        return {"token": token, "user_id": user_id}
    else:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

# User endpoints
@app.get("/users")
async def list_users(token: str):
    get_current_user(token)
    return auth_service.list_users()

# Group endpoints
@app.post("/groups")
async def create_group(group: CreateGroup, token: str):
    user_id = get_current_user(token)
    group_id, message = await group_service.create_group(
        group.name, group.description, group.is_hierarchical, user_id, group.members
    )
    if group_id:
        return {"group_id": group_id, "message": message}
    else:
        raise HTTPException(status_code=400, detail=message)

@app.get("/groups")
async def list_user_groups(token: str):
    user_id = get_current_user(token)
    return group_service.list_user_groups(user_id)

@app.get("/groups/invitations")
async def pending_invitations(token: str):
    user_id = get_current_user(token)
    return group_service.pending_invitations(user_id)

@app.get("/groups/invitations/count")
async def get_pending_invitations_count(token: str):
    user_id = get_current_user(token)
    count = group_service.get_pending_invitations_count(user_id)
    return {"count": count}

@app.get("/groups/{group_id}/info")
async def get_group_info(group_id: int, token: str):
    get_current_user(token)
    group_info = group_service.get_group_info(group_id)
    if group_info:
        return {
            "id": group_info[0],
            "name": group_info[1],
            "description": group_info[2],
            "is_hierarchical": group_info[3],
            "creator_id": group_info[4]
        }
    else:
        raise HTTPException(status_code=404, detail="Grupo no encontrado")

@app.get("/groups/{group_id}/members")
async def list_group_members(group_id: int, token: str):
    get_current_user(token)
    return group_service.list_group_members(group_id)

@app.post("/groups/invite")
async def invite_user(invite: InviteUser, token: str):
    user_id = get_current_user(token)
    success, message = await group_service.invite_user(invite.group_id, invite.invited_user_id, user_id)
    if success:
        return {"message": message}
    else:
        raise HTTPException(status_code=400, detail=message)

@app.post("/groups/invitations/respond")
async def respond_invitation(response: RespondInvitation, token: str):
    user_id = get_current_user(token)
    success = await group_service.respond_invitation(response.invitation_id, response.response, user_id)
    if success:
        return {"message": "Respuesta registrada"}
    else:
        raise HTTPException(status_code=400, detail="Error al responder invitación")

# New endpoints for group management
@app.put("/groups/{group_id}")
async def update_group(group_id: int, group_data: UpdateGroup, token: str):
    user_id = get_current_user(token)
    success, message = group_service.update_group(
        group_id, user_id, group_data.name, group_data.description
    )
    if success:
        return {"message": message}
    else:
        raise HTTPException(status_code=400, detail=message)

@app.delete("/groups/{group_id}")
async def delete_group(group_id: int, token: str):
    user_id = get_current_user(token)
    success, message = await group_service.delete_group(group_id, user_id)
    if success:
        return {"message": message}
    else:
        raise HTTPException(status_code=400, detail=message)

@app.delete("/groups/{group_id}/members/{member_id}")
async def remove_member(group_id: int, member_id: int, token: str):
    user_id = get_current_user(token)
    success, message = await group_service.remove_member(group_id, user_id, member_id)
    if success:
        return {"message": message}
    else:
        raise HTTPException(status_code=400, detail=message)

# Event endpoints
@app.post("/events")
async def create_event(event: CreateEvent, token: str):
    user_id = get_current_user(token)
    event_id, message = await event_service.create_event(
        event.title, event.description, event.start_time, event.end_time,
        user_id, event.group_id, event.is_group_event, event.participants_ids, event.is_hierarchical
    )
    if event_id:
        return {"event_id": event_id, "message": "Evento creado exitosamente"}
    else:
        raise HTTPException(status_code=400, detail=message)

@app.get("/events")
async def get_user_events(token: str):
    user_id = get_current_user(token)
    return event_service.get_user_events(user_id)

@app.get("/events/detailed")
async def get_user_events_detailed(token: str, filter_type: str = "all"):
    user_id = get_current_user(token)
    return event_service.get_user_events_detailed(user_id, filter_type)

@app.get("/events/invitations")
async def get_pending_event_invitations(token: str):
    user_id = get_current_user(token)
    return event_service.get_pending_event_invitations(user_id)

@app.post("/events/invitations/respond")
async def respond_event_invitation(response: RespondEventInvitation, token: str):
    user_id = get_current_user(token)
    success, message = await event_service.respond_to_event_invitation(
        response.event_id, user_id, response.accepted
    )
    if success:
        return {"message": message}
    else:
        raise HTTPException(status_code=400, detail=message)

@app.get("/events/invitations/count")
async def get_pending_event_invitations_count(token: str):
    user_id = get_current_user(token)
    count = event_service.get_pending_invitations_count(user_id)
    return {"count": count}

# New endpoints for canceling events and leaving events
@app.delete("/events/{event_id}")
async def cancel_event(event_id: int, token: str):
    user_id = get_current_user(token)
    success, message = await event_service.cancel_event(event_id, user_id)
    if success:
        return {"message": message}
    else:
        raise HTTPException(status_code=400, detail=message)

@app.delete("/events/{event_id}/leave")
async def leave_event(event_id: int, token: str):
    user_id = get_current_user(token)
    success, message = await event_service.leave_event(event_id, user_id)
    if success:
        return {"message": message}
    else:
        raise HTTPException(status_code=400, detail=message)

@app.put("/events/{event_id}")
async def update_event(event_id: int, update: UpdateEvent, token: str):
    user_id = get_current_user(token)
    payload = update.dict(exclude_unset=True)
    success, message = await event_service.update_event(event_id, user_id, **payload)
    if success:
        return {"message": message}
    raise HTTPException(status_code=400, detail=message)

@app.get("/events/{event_id}/details")
async def get_event_details(event_id: int, token: str):
    user_id = get_current_user(token)
    event_details, message = event_service.get_event_details(event_id, user_id)
    if event_details:
        return event_details
    else:
        raise HTTPException(status_code=400, detail=message)

@app.get("/")
async def root():
    return {"message": "Agenda Distribuida API"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8766)
