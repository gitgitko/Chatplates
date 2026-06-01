"""
ChatRooms - WebSocket Server
Requires: pip install fastapi uvicorn websockets
Run with: uvicorn Chatplates:app --reload
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# rooms[room_id] = {name: websocket}
rooms: dict[str, dict[str, WebSocket]] = {}


def now() -> str:
    return datetime.now().strftime("%I:%M %p")


async def broadcast(room_id: str, payload: dict, exclude: str = None):
    """Send a message to all clients in a room."""
    if room_id not in rooms:
        return
    dead = []
    for name, ws in rooms[room_id].items():
        if name == exclude:
            continue
        try:
            await ws.send_text(json.dumps(payload))
        except Exception:
            dead.append(name)
    for name in dead:
        rooms[room_id].pop(name, None)


async def broadcast_user_list(room_id: str):
    """Push the current user list to everyone in the room."""
    if room_id not in rooms:
        return
    users = list(rooms[room_id].keys())
    await broadcast(room_id, {"type": "users", "users": users})


@app.websocket("/ws/{room_id}")
async def websocket_endpoint(ws: WebSocket, room_id: str, name: str = "Anonymous"):
    await ws.accept()

    # Sanitise inputs
    room_id = room_id.strip().upper() if room_id.lower() != "public" else "public"
    name = name.strip()[:20] or "Anonymous"

    # Handle duplicate names in the same room
    if room_id in rooms and name in rooms[room_id]:
        suffix = 2
        base = name
        while f"{base}{suffix}" in rooms[room_id]:
            suffix += 1
        name = f"{base}{suffix}"

    # Register
    rooms.setdefault(room_id, {})[name] = ws

    # Greet the newcomer
    await ws.send_text(json.dumps({
        "type": "system",
        "text": f"Welcome to {'the Public Room' if room_id == 'public' else f'room {room_id}'}, {name}!",
        "time": now(),
    }))

    # Tell everyone else
    await broadcast(room_id, {
        "type": "system",
        "text": f"{name} joined the room.",
        "time": now(),
    }, exclude=name)

    # Push updated user list
    await broadcast_user_list(room_id)

    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)

            if data.get("type") == "message":
                text = data.get("text", "").strip()
                if not text:
                    continue
                payload = {
                    "type": "message",
                    "sender": name,
                    "text": text,
                    "time": now(),
                }
                # Echo back to sender too
                await ws.send_text(json.dumps(payload))
                await broadcast(room_id, payload, exclude=name)

    except WebSocketDisconnect:
        rooms[room_id].pop(name, None)
        if not rooms[room_id]:
            del rooms[room_id]
        else:
            await broadcast(room_id, {
                "type": "system",
                "text": f"{name} left the room.",
                "time": now(),
            })
            await broadcast_user_list(room_id)
