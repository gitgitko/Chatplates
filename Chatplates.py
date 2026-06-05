"""
Chatplates - WebSocket Server v5
- Owner creates invite codes with pre‑assigned names
- Guest room (free, no code)
- Public/secret rooms require invite code
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

# invite_codes[code] = fixed name (string)
invite_codes: dict[str, str] = {}

# sessions[name] = websocket
sessions: dict[str, WebSocket] = {}

# rooms[room_id] = {name: websocket}
rooms: dict[str, dict[str, WebSocket]] = {}

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
    online = list(sessions.keys())
    for ws in sessions.values():
        try:
            await ws.send_text(json.dumps({"type": "online_users", "users": online}))
        except Exception:
            pass

async def register_user(ws, name: str, is_owner: bool, room_id: str):
    """Handle duplicate names and add to session/room."""
    base = name
    suffix = 2
    while name in sessions:
        name = f"{base}{suffix}"
        suffix += 1
    sessions[name] = ws
    rooms.setdefault(room_id, {})[name] = ws
    await ws.send_text(json.dumps({
        "type": "auth_ok",
        "name": name,
        "is_owner": is_owner,
    }))
    await ws.send_text(json.dumps({
        "type": "system",
        "text": f"Welcome, {name}! You joined {room_id}.",
        "time": now(),
    }))
    await broadcast(room_id, {"type": "system", "text": f"{name} joined.", "time": now()}, exclude=name)
    await broadcast_user_list(room_id)
    await send_online_list()
    return name

@app.websocket("/ws/{room_id}")
async def websocket_endpoint(ws: WebSocket, room_id: str):
    await ws.accept()
    room_raw = room_id.strip()
    is_public_room = room_raw.lower() == "public"
    room_id_norm = "public" if is_public_room else room_raw.upper()

    # ----- AUTH HANDSHAKE -----
    try:
        raw = await ws.receive_text()
        auth = json.loads(raw)
    except Exception:
        await ws.close()
        return

    if auth.get("type") != "auth":
        await ws.send_text(json.dumps({"type": "auth_fail", "reason": "No auth provided."}))
        await ws.close()
        return

    # Owner login
    if auth.get("pin"):
        if auth["pin"] != OWNER_PIN:
            await ws.send_text(json.dumps({"type": "auth_fail", "reason": "Wrong owner PIN."}))
            await ws.close()
            return
        name = auth.get("name", "Owner").strip()[:20] or "Owner"
        await register_user(ws, name, True, room_id_norm)
    else:
        # Regular user with invite code
        code = auth.get("code", "").strip().upper()
        if not code or code not in invite_codes:
            await ws.send_text(json.dumps({"type": "auth_fail", "reason": "Invalid invite code."}))
            await ws.close()
            return
        fixed_name = invite_codes.pop(code)
        await register_user(ws, fixed_name, False, room_id_norm)

    # ----- MESSAGE LOOP -----
    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)
            t = data.get("type")

            if t == "message":
                text = data.get("text", "").strip()
                rid = data.get("room", room_id_norm)
                if not text:
                    continue
                payload = {"type": "message", "sender": name, "text": text, "time": now(), "room": rid}
                await ws.send_text(json.dumps(payload))
                await broadcast(rid, payload, exclude=name)

            elif t == "join_room":
                new_room = data.get("room", "").strip().upper()
                if not new_room:
                    continue
                rooms.setdefault(new_room, {})[name] = ws
                room_id_norm = new_room
                is_public_room = (new_room.lower() == "public")
                await ws.send_text(json.dumps({"type": "system", "text": f"You joined room {new_room}.", "time": now()}))
                await broadcast(new_room, {"type": "system", "text": f"{name} joined.", "time": now()}, exclude=name)
                await broadcast_user_list(new_room)

            elif t == "dm":
                target = data.get("target", "").strip()
                text = data.get("text", "").strip()
                if not target or not text:
                    continue
                if target not in sessions:
                    await ws.send_text(json.dumps({"type": "system", "text": f"{target} is not online.", "time": now()}))
                    continue
                dm_room = dm_id(name, target)
                rooms.setdefault(dm_room, {})[name] = ws
                rooms[dm_room][target] = sessions[target]
                payload = {"type": "dm", "sender": name, "target": target, "text": text, "time": now(), "room": dm_room}
                await ws.send_text(json.dumps(payload))
                await sessions[target].send_text(json.dumps(payload))

            elif t == "create_invite":
                if not is_owner:
                    await ws.send_text(json.dumps({"type": "system", "text": "Not authorized.", "time": now()}))
                    continue
                invited_name = data.get("invited_name", "").strip()
                if not invited_name:
                    await ws.send_text(json.dumps({"type": "system", "text": "Name required for invite.", "time": now()}))
                    continue
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

@app.websocket("/ws/guest")
async def guest_endpoint(ws: WebSocket):
    await ws.accept()
    room_id = "guest_lounge"

    try:
        raw = await ws.receive_text()
        auth = json.loads(raw)
    except Exception:
        await ws.close()
        return

    if auth.get("type") != "auth":
        await ws.send_text(json.dumps({"type": "auth_fail", "reason": "No auth provided."}))
        await ws.close()
        return

    name = auth.get("name", "").strip()[:20]
    if not name:
        await ws.send_text(json.dumps({"type": "auth_fail", "reason": "Name required for guest lounge."}))
        await ws.close()
        return

    await register_user(ws, name, False, room_id)

    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)
            t = data.get("type")

            if t == "message":
                text = data.get("text", "").strip()
                rid = data.get("room", room_id)
                if not text:
                    continue
                payload = {"type": "message", "sender": name, "text": text, "time": now(), "room": rid}
                await ws.send_text(json.dumps(payload))
                await broadcast(rid, payload, exclude=name)

            elif t == "join_room":
                new_room = data.get("room", "").strip().upper()
                if not new_room:
                    continue
                rooms.setdefault(new_room, {})[name] = ws
                room_id = new_room
                await ws.send_text(json.dumps({"type": "system", "text": f"You joined room {new_room}.", "time": now()}))
                await broadcast(new_room, {"type": "system", "text": f"{name} joined.", "time": now()}, exclude=name)
                await broadcast_user_list(new_room)

            elif t == "dm":
                target = data.get("target", "").strip()
                text = data.get("text", "").strip()
                if not target or not text:
                    continue
                if target not in sessions:
                    await ws.send_text(json.dumps({"type": "system", "text": f"{target} is not online.", "time": now()}))
                    continue
                dm_room = dm_id(name, target)
                rooms.setdefault(dm_room, {})[name] = ws
                rooms[dm_room][target] = sessions[target]
                payload = {"type": "dm", "sender": name, "target": target, "text": text, "time": now(), "room": dm_room}
                await ws.send_text(json.dumps(payload))
                await sessions[target].send_text(json.dumps(payload))

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
