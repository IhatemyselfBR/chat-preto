import asyncio, json, base64, uuid, mimetypes
from pathlib import Path
from datetime import datetime
from aiohttp import web, WSMsgType

BASE = Path(__file__).resolve().parent
STICKERS = BASE / "stickers"
HISTORY = BASE / "chat.json"
STICKERS.mkdir(exist_ok=True)
clients = set()
lock = asyncio.Lock()

def load_history():
    try:
        data = json.loads(HISTORY.read_text())
        return data if isinstance(data, list) else []
    except (OSError, ValueError):
        return []

messages = load_history()

async def broadcast(data):
    for ws in list(clients):
        try:
            await ws.send_json(data)
        except Exception:
            clients.discard(ws)

async def chat(request):
    ws = web.WebSocketResponse(heartbeat=25, max_msg_size=5*1024*1024)
    await ws.prepare(request)
    clients.add(ws)
    await ws.send_json({"type":"history","messages":messages})

    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            try:
                data = json.loads(msg.data)
                sender = str(data.get("sender","Pessoa"))[:30]
                action = data.get("action","text")
                item = {
                    "id": uuid.uuid4().hex,
                    "sender": sender,
                    "time": datetime.now().strftime("%H:%M"),
                    "kind": action
                }

                if action == "text":
                    text = str(data.get("text","")).strip()
                    if not text or len(text) > 2000:
                        continue
                    item["text"] = text
                    reply = data.get("reply")
                    if isinstance(reply, dict):
                        item["reply"] = {
                            "sender": str(reply.get("sender",""))[:30],
                            "text": str(reply.get("text",""))[:200],
                            "kind": str(reply.get("kind","text"))
                        }

                elif action == "sticker":
                    image = str(data.get("image",""))
                    allowed = {
                        "image/png":".png",
                        "image/jpeg":".jpg",
                        "image/webp":".webp",
                        "image/gif":".gif"
                    }
                    if "," not in image:
                        continue
                    header, encoded = image.split(",",1)
                    mime = header.removeprefix("data:").removesuffix(";base64")
                    if mime not in allowed:
                        continue
                    raw = base64.b64decode(encoded, validate=True)
                    if not raw or len(raw) > 3*1024*1024:
                        continue
                    name = uuid.uuid4().hex + allowed[mime]
                    (STICKERS / name).write_bytes(raw)
                    item["image"] = "/stickers/" + name
                else:
                    continue

                async with lock:
                    messages.append(item)
                    del messages[:-500]
                    HISTORY.write_text(json.dumps(messages, ensure_ascii=False, indent=2))
                await broadcast({"type":"message","message":item})
            except (ValueError, TypeError, KeyError):
                continue
    finally:
        clients.discard(ws)
    return ws

app = web.Application()
app.router.add_get("/ws", chat)
app.router.add_static("/stickers/", str(STICKERS), show_index=False)
web.run_app(app, host="127.0.0.1", port=8080)
