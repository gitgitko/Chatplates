from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json, random, string
from datetime import datetime

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"])

OWNER_PIN = "637r8yw98eydy28qwu8qbysu2e9ur9sua8shy82yq78y8et783t75y8ew9ye8yqx7287e7t2i73qt7wy7eh6sqis2ym8ay8jse8y3w7t8etq8628qy8wyehqa2e7t"

invite_codes = {}
sessions = {}
rooms = {}

def now(): return datetime.now().strftime("%I:%M %p")
def gen_code(): return "".join(random.choices(string.ascii_uppercase + string.digits, k=10))

async def broadcast(room, msg, exclude=None):
    if room not in rooms: return
    dead = []
    for name, ws in rooms[room].items():
        if name == exclude: continue
        try: await ws.send_text(json.dumps(msg))
        except: dead.append(name)
    for n in dead: rooms[room].pop(n, None)

async def register(ws, name, owner, room):
    base = name
    n = 2
    while name in sessions:
        name = f"{base}{n}"
        n += 1
    sessions[name] = ws
    rooms.setdefault(room, {})[name] = ws
    await ws.send_text(json.dumps({"type": "auth_ok", "name": name, "is_owner": owner}))
    await ws.send_text(json.dumps({"type": "system", "text": f"Welcome {name} to {room}", "time": now()}))
    await broadcast(room, {"type": "system", "text": f"{name} joined", "time": now()}, exclude=name)
    await broadcast(room, {"type": "users", "users": list(rooms[room].keys())})
    for ws2 in sessions.values():
        try: await ws2.send_text(json.dumps({"type": "online_users", "users": list(sessions.keys())}))
        except: pass
    return name

# ========== GUEST ENDPOINT – MUST COME FIRST ==========
@app.websocket("/ws/guest")
async def ws_guest(ws: WebSocket):
    await ws.accept()
    room = "guest_lounge"
    try:
        data = await ws.receive_text()
        auth = json.loads(data)
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
    # Register as guest (no invite code check)
    base = name
    n = 2
    while name in sessions:
        name = f"{base}{n}"
        n += 1
    sessions[name] = ws
    rooms.setdefault(room, {})[name] = ws
    await ws.send_text(json.dumps({"type": "auth_ok", "name": name, "is_owner": False}))
    await ws.send_text(json.dumps({"type": "system", "text": f"Welcome {name} to Guest Lounge", "time": now()}))
    await broadcast(room, {"type": "system", "text": f"{name} joined", "time": now()}, exclude=name)
    await broadcast(room, {"type": "users", "users": list(rooms[room].keys())})
    for ws2 in sessions.values():
        try: await ws2.send_text(json.dumps({"type": "online_users", "users": list(sessions.keys())}))
        except: pass
    # Message loop
    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)
            t = msg.get("type")
            if t == "message":
                text = msg.get("text", "").strip()
                rid = msg.get("room", room)
                if not text: continue
                payload = {"type": "message", "sender": name, "text": text, "time": now(), "room": rid}
                await ws.send_text(json.dumps(payload))
                await broadcast(rid, payload, exclude=name)
            elif t == "join_room":
                new_room = msg.get("room", "").strip().upper()
                if new_room:
                    rooms.setdefault(new_room, {})[name] = ws
                    room = new_room
                    await ws.send_text(json.dumps({"type": "system", "text": f"Joined {new_room}", "time": now()}))
                    await broadcast(new_room, {"type": "system", "text": f"{name} joined", "time": now()}, exclude=name)
                    await broadcast(new_room, {"type": "users", "users": list(rooms[new_room].keys())})
            elif t == "dm":
                target = msg.get("target", "").strip()
                text = msg.get("text", "").strip()
                if target in sessions and text:
                    dm_room = "dm_" + "_".join(sorted([name, target]))
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
                    await broadcast(rid, {"type": "system", "text": f"{name} left", "time": now()})
                    await broadcast(rid, {"type": "users", "users": list(members.keys())})
                else:
                    del rooms[rid]
        for ws2 in sessions.values():
            try: await ws2.send_text(json.dumps({"type": "online_users", "users": list(sessions.keys())}))
            except: pass

# ========== MAIN ENDPOINT (for invite codes) ==========
@app.websocket("/ws/{room_id}")
async def ws_main(ws: WebSocket, room_id: str):
    await ws.accept()
    room = room_id.strip().upper()
    try:
        data = await ws.receive_text()
        auth = json.loads(data)
    except:
        await ws.close()
        return
    if auth.get("type") != "auth":
        await ws.send_text(json.dumps({"type": "auth_fail", "reason": "Invalid auth"}))
        await ws.close()
        return
    if auth.get("pin"):
        if auth["pin"] != OWNER_PIN:
            await ws.send_text(json.dumps({"type": "auth_fail", "reason": "Wrong PIN"}))
            await ws.close()
            return
        name = auth.get("name", "Owner")[:20]
        owner = True
        await register(ws, name, owner, room)
    else:
        code = auth.get("code", "").strip().upper()
        if not code or code not in invite_codes:
            await ws.send_text(json.dumps({"type": "auth_fail", "reason": "Invalid invite code."}))
            await ws.close()
            return
        fixed_name = invite_codes.pop(code)
        owner = False
        await register(ws, fixed_name, owner, room)
    # Message loop (same as guest)
    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)
            t = msg.get("type")
            if t == "message":
                text = msg.get("text", "").strip()
                rid = msg.get("room", room)
                if not text: continue
                payload = {"type": "message", "sender": name, "text": text, "time": now(), "room": rid}
                await ws.send_text(json.dumps(payload))
                await broadcast(rid, payload, exclude=name)
            elif t == "join_room":
                new_room = msg.get("room", "").strip().upper()
                if new_room:
                    rooms.setdefault(new_room, {})[name] = ws
                    room = new_room
                    await ws.send_text(json.dumps({"type": "system", "text": f"Joined {new_room}", "time": now()}))
                    await broadcast(new_room, {"type": "system", "text": f"{name} joined", "time": now()}, exclude=name)
                    await broadcast(new_room, {"type": "users", "users": list(rooms[new_room].keys())})
            elif t == "dm":
                target = msg.get("target", "").strip()
                text = msg.get("text", "").strip()
                if target in sessions and text:
                    dm_room = "dm_" + "_".join(sorted([name, target]))
                    rooms.setdefault(dm_room, {})[name] = ws
                    rooms[dm_room][target] = sessions[target]
                    payload = {"type": "dm", "sender": name, "target": target, "text": text, "time": now(), "room": dm_room}
                    await ws.send_text(json.dumps(payload))
                    await sessions[target].send_text(json.dumps(payload))
            elif t == "create_invite" and owner:
                invited = msg.get("invited_name", "").strip()
                if invited:
                    code = gen_code()
                    invite_codes[code] = invited
                    await ws.send_text(json.dumps({"type": "invite_created", "code": code, "invited_name": invited}))
    except WebSocketDisconnect:
        sessions.pop(name, None)
        for rid, members in list(rooms.items()):
            if name in members:
                members.pop(name)
                if members:
                    await broadcast(rid, {"type": "system", "text": f"{name} left", "time": now()})
                    await broadcast(rid, {"type": "users", "users": list(members.keys())})
                else:
                    del rooms[rid]
        for ws2 in sessions.values():
            try: await ws2.send_text(json.dumps({"type": "online_users", "users": list(sessions.keys())}))
            except: pass
