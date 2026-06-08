from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
from datetime import datetime

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"])

sessions = {}          # name -> websocket
rooms = {}             # room_id -> {name: websocket}

def now():
    return datetime.now().strftime("%I:%M %p")

async def broadcast(room_id, payload, exclude=None):
    if room_id not in rooms:
        return
    dead = []
    for name, ws in rooms[room_id].items():
        if name == exclude:
            continue
        try:
            await ws.send_text(json.dumps(payload))
        except:
            dead.append(name)
    for n in dead:
        rooms[room_id].pop(n, None)

async def register_user(ws, name, room_id):
    base = name
    suffix = 2
    while name in sessions:
        name = f"{base}{suffix}"
        suffix += 1
    sessions[name] = ws
    rooms.setdefault(room_id, {})[name] = ws
    await ws.send_text(json.dumps({"type": "auth_ok", "name": name}))
    await ws.send_text(json.dumps({"type": "system", "text": f"Welcome, {name}! You joined {room_id}.", "time": now()}))
    await broadcast(room_id, {"type": "system", "text": f"{name} joined.", "time": now()}, exclude=name)
    await broadcast(room_id, {"type": "users", "users": list(rooms[room_id].keys())})
    # global online list
    online = list(sessions.keys())
    for w in sessions.values():
        try:
            await w.send_text(json.dumps({"type": "online_users", "users": online}))
        except:
            pass
    return name

# ---------- PUBLIC ROOM ----------
@app.websocket("/ws/public")
async def public_endpoint(ws: WebSocket):
    await ws.accept()
    room_id = "public"
    try:
        raw = await ws.receive_text()
        auth = json.loads(raw)
    except:
        await ws.close()
        return
    if auth.get("type") != "auth":
        await ws.send_text(json.dumps({"type": "auth_fail", "reason": "No auth"}))
        await ws.close()
        return
    name = auth.get("name", "").strip()[:20]
    if not name:
        await ws.send_text(json.dumps({"type": "auth_fail", "reason": "Name required"}))
        await ws.close()
        return
    await register_user(ws, name, room_id)
    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)
            if data.get("type") == "message":
                text = data.get("text", "").strip()
                if not text:
                    continue
                rid = data.get("room", room_id)
                payload = {"type": "message", "sender": name, "text": text, "time": now(), "room": rid}
                await ws.send_text(json.dumps(payload))
                await broadcast(rid, payload, exclude=name)
            elif data.get("type") == "join_room":
                new_room = data.get("room", "").strip().upper()
                if new_room:
                    rooms.setdefault(new_room, {})[name] = ws
                    room_id = new_room
                    await ws.send_text(json.dumps({"type": "system", "text": f"You joined room {new_room}.", "time": now()}))
                    await broadcast(new_room, {"type": "system", "text": f"{name} joined.", "time": now()}, exclude=name)
                    await broadcast(new_room, {"type": "users", "users": list(rooms[new_room].keys())})
    except WebSocketDisconnect:
        sessions.pop(name, None)
        for rid, members in list(rooms.items()):
            if name in members:
                members.pop(name)
                if members:
                    await broadcast(rid, {"type": "system", "text": f"{name} left.", "time": now()})
                    await broadcast(rid, {"type": "users", "users": list(members.keys())})
                else:
                    del rooms[rid]
        online = list(sessions.keys())
        for w in sessions.values():
            try:
                await w.send_text(json.dumps({"type": "online_users", "users": online}))
            except:
                pass

# ---------- SECRET ROOMS (anyone can create/join) ----------
@app.websocket("/ws/room/{room_name}")
async def room_endpoint(ws: WebSocket, room_name: str):
    await ws.accept()
    room_id = room_name.strip().upper()
    try:
        raw = await ws.receive_text()
        auth = json.loads(raw)
    except:
        await ws.close()
        return
    if auth.get("type") != "auth":
        await ws.send_text(json.dumps({"type": "auth_fail", "reason": "No auth"}))
        await ws.close()
        return
    name = auth.get("name", "").strip()[:20]
    if not name:
        await ws.send_text(json.dumps({"type": "auth_fail", "reason": "Name required"}))
        await ws.close()
        return
    await register_user(ws, name, room_id)
    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)
            if data.get("type") == "message":
                text = data.get("text", "").strip()
                if not text:
                    continue
                rid = data.get("room", room_id)
                payload = {"type": "message", "sender": name, "text": text, "time": now(), "room": rid}
                await ws.send_text(json.dumps(payload))
                await broadcast(rid, payload, exclude=name)
            elif data.get("type") == "join_room":
                new_room = data.get("room", "").strip().upper()
                if new_room:
                    rooms.setdefault(new_room, {})[name] = ws
                    room_id = new_room
                    await ws.send_text(json.dumps({"type": "system", "text": f"You joined room {new_room}.", "time": now()}))
                    await broadcast(new_room, {"type": "system", "text": f"{name} joined.", "time": now()}, exclude=name)
                    await broadcast(new_room, {"type": "users", "users": list(rooms[new_room].keys())})
    except WebSocketDisconnect:
        sessions.pop(name, None)
        for rid, members in list(rooms.items()):
            if name in members:
                members.pop(name)
                if members:
                    await broadcast(rid, {"type": "system", "text": f"{name} left.", "time": now()})
                    await broadcast(rid, {"type": "users", "users": list(members.keys())})
                else:
                    del rooms[rid]
        online = list(sessions.keys())
        for w in sessions.values():
            try:
                await w.send_text(json.dumps({"type": "online_users", "users": online}))
            except:
                pass
