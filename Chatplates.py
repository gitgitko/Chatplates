"""
Chatplates - WebSocket Server with Auth + DMs
Requires: pip install fastapi uvicorn websockets
Run with: uvicorn Chatplates:app --reload
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import json, random, string

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

OWNER_PIN = "637r8yw98eydy28qwu8qbysu2e9ur9sua8shy82yq78y8et783t75y8ew9ye8yqx7287e7t2i73qt7wy7eh6sqis2ym8ay8jse8y3w7t8etq8628qy8wyehqa2e7t"

# invite_codes[code] = name (unused) or None if used
invite_codes: dict[str, str] = {}

# sessions[name] = websocket (main connection)
sessions: dict[str, WebSocket] = {}

# rooms[room_id] = {name: websocket}
rooms: dict[str, dict[str, WebSocket]] = {}

# dm_rooms are just rooms with id = "dm_{sorted(a,b)}"
def dm_id(a: str, b: str) -> str:
    return "dm_" + "_".join(sorted([a, b]))

def now() -> str:
    return datetime.now().strftime("%I:%M %p")

def gen_invite() -> str:
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choices(chars, k=10))

async def broadcast(room_id: str, payload: dict, exclude: str = None):
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
    for n in dead:
        rooms[room_id].pop(n, None)

async def broadcast_user_list(room_id: str):
    if room_id not in rooms:
        return
    users = list(rooms[room_id].keys())
    await broadcast(room_id, {"type": "users", "users": users})

async def send_online_list():
    """Send global online user list to everyone."""
    online = list(sessions.keys())
    for ws in sessions.values():
        try:
            await ws.send_text(json.dumps({"type": "online_users", "users": online}))
        except Exception:
            pass

@app.websocket("/ws/{room_id}")
async def websocket_endpoint(ws: WebSocket, room_id: str, name: str = ""):
    await ws.accept()

    room_id = room_id.strip()
    name = name.strip()[:20]

    # ── AUTH HANDSHAKE ──────────────────────────────────────────
    # First message must be auth
    try:
        raw = await ws.receive_text()
        auth = json.loads(raw)
    except Exception:
        await ws.close(); return

    if auth.get("type") != "auth":
        await ws.send_text(json.dumps({"type": "auth_fail", "reason": "No auth provided."}))
        await ws.close(); return

    is_owner = False

    if auth.get("pin"):
        if auth["pin"] != OWNER_PIN:
            await ws.send_text(json.dumps({"type": "auth_fail", "reason": "Wrong owner PIN."}))
            await ws.close(); return
        is_owner = True
        name = auth.get("name", "Owner").strip()[:20] or "Owner"
    else:
        code = auth.get("code", "").strip().upper()
        name = auth.get("name", "").strip()[:20]
        if not name:
            await ws.send_text(json.dumps({"type": "auth_fail", "reason": "Name required."}))
            await ws.close(); return
        # Allow joining public room without invite code
        if room_id_norm != "public":
            if code not in invite_codes:
                await ws.send_text(json.dumps({"type": "auth_fail", "reason": "Invalid or already used invite code."}))
                await ws.close(); return
            del invite_codes[code]

    # Handle duplicate names
    base = name
    suffix = 2
    while name in sessions:
        name = f"{base}{suffix}"; suffix += 1

    # Register session
    sessions[name] = ws
    await ws.send_text(json.dumps({
        "type": "auth_ok",
        "name": name,
        "is_owner": is_owner,
    }))

    # Join the requested room
    room_id_norm = room_id.upper() if room_id.lower() != "public" else "public"
    rooms.setdefault(room_id_norm, {})[name] = ws

    await ws.send_text(json.dumps({
        "type": "system",
        "text": f"Welcome, {name}! You joined {'the Public Room' if room_id_norm == 'public' else room_id_norm}.",
        "time": now(),
    }))
    await broadcast(room_id_norm, {"type": "system", "text": f"{name} joined.", "time": now()}, exclude=name)
    await broadcast_user_list(room_id_norm)
    await send_online_list()

    # ── MESSAGE LOOP ────────────────────────────────────────────
    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)
            t = data.get("type")

            # Chat message in a room
            if t == "message":
                text = data.get("text", "").strip()
                rid = data.get("room", room_id_norm)
                if not text: continue
                payload = {"type": "message", "sender": name, "text": text, "time": now(), "room": rid}
                await ws.send_text(json.dumps(payload))
                await broadcast(rid, payload, exclude=name)

            # Join a different room
            elif t == "join_room":
                new_room = data.get("room", "").strip().upper()
                if not new_room: continue
                rooms.setdefault(new_room, {})[name] = ws
                room_id_norm = new_room
                await ws.send_text(json.dumps({"type": "system", "text": f"You joined room {new_room}.", "time": now()}))
                await broadcast(new_room, {"type": "system", "text": f"{name} joined.", "time": now()}, exclude=name)
                await broadcast_user_list(new_room)

            # Open a DM
            elif t == "dm":
                target = data.get("target", "").strip()
                text = data.get("text", "").strip()
                if not target or not text: continue
                if target not in sessions:
                    await ws.send_text(json.dumps({"type": "system", "text": f"{target} is not online.", "time": now()}))
                    continue
                dm_room = dm_id(name, target)
                rooms.setdefault(dm_room, {})[name] = ws
                rooms[dm_room][target] = sessions[target]
                payload = {"type": "dm", "sender": name, "target": target, "text": text, "time": now(), "room": dm_room}
                await ws.send_text(json.dumps(payload))
                await sessions[target].send_text(json.dumps(payload))

            # Owner: create invite
            elif t == "create_invite":
                if not is_owner:
                    await ws.send_text(json.dumps({"type": "system", "text": "Not authorized.", "time": now()}))
                    continue
                invited_name = data.get("invited_name", "User").strip()
                code = gen_invite()
                invite_codes[code] = invited_name
                await ws.send_text(json.dumps({"type": "invite_created", "code": code, "invited_name": invited_name}))

    except WebSocketDisconnect:
        sessions.pop(name, None)
        for rid, members in list(rooms.items()):
            if name in members:
                members.pop(name)
                if members:
                    await broadcast(rid, {"type": "system", "text": f"{name} left.", "time": now()})
                    await broadcast_user_list(rid)
                else:
                    del rooms[rid]
        await send_online_list()
