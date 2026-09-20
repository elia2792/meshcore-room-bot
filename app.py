import asyncio
import os
import re
import struct
import sqlite3
import time
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, JSONResponse
import httpx
from web_client import get_web_client_html

# Configuration
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8799543336:AAExe5g_-6ud-4kAX4p_EVtovS_R7KqSToM")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "365061699")
DB_PATH = os.getenv("DB_PATH", "room_messages.db")

app = FastAPI(title="MeshCore Telegram Room Bridge")

# Global State
active_heltec_ws: Optional[WebSocket] = None
active_browser_clients: set = set()
stats = {
    "packets_rx": 0,
    "packets_tx": 0,
    "connected_since": None,
    "last_seen": None
}

async def broadcast_to_browsers(payload: dict):
    if not active_browser_clients:
        return
    dead = set()
    for ws in list(active_browser_clients):
        try:
            await ws.send_json(payload)
        except Exception:
            dead.add(ws)
    for ws in dead:
        active_browser_clients.discard(ws)

# MeshCore Channels and Node Cache
discovered_channels: Dict[int, str] = {0: "Public"}
chat_active_channel: Dict[str, int] = {}  # chat_id -> channel_idx
default_channel_idx: int = 0
node_info: Dict[str, Any] = {
    "name": "Buscate",
    "firmware": "MeshCore",
    "freq_mhz": 869.618,
    "bw_khz": 62.5,
    "sf": 8,
    "cr": 8,
    "max_channels": 40
}

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            source TEXT,
            sender TEXT,
            channel TEXT,
            content TEXT,
            snr REAL,
            hops INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS subscriptions (
            chat_id TEXT PRIMARY KEY,
            chat_type TEXT,
            chat_title TEXT,
            active_channel INTEGER DEFAULT 0,
            last_active DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS heard_nodes (
            node_name TEXT PRIMARY KEY,
            last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_snr REAL,
            last_hops INTEGER,
            last_channel TEXT,
            lat REAL,
            lon REAL,
            packets_count INTEGER DEFAULT 1
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS topic_bindings (
            chat_id TEXT,
            lora_channel_idx INTEGER,
            message_thread_id INTEGER,
            PRIMARY KEY(chat_id, lora_channel_idx)
        )
    ''')
    conn.commit()
    conn.close()

def register_subscription(chat_id: str, chat_type: str = "private", chat_title: str = "Chat"):
    if not chat_id:
        return
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO subscriptions (chat_id, chat_type, chat_title, last_active)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(chat_id) DO UPDATE SET
                chat_type = excluded.chat_type,
                chat_title = excluded.chat_title,
                last_active = CURRENT_TIMESTAMP
        ''', (chat_id, chat_type, chat_title))
        conn.commit()
        conn.close()
    except Exception as e:
        print("DB register_subscription error:", e)

def get_all_subscriptions() -> List[str]:
    recipients = set()
    if TELEGRAM_CHAT_ID:
        recipients.add(str(TELEGRAM_CHAT_ID))
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT chat_id FROM subscriptions")
        for row in cursor.fetchall():
            recipients.add(str(row[0]))
        conn.close()
    except Exception as e:
        print("DB get_all_subscriptions error:", e)
    return list(recipients)

def record_heard_node(node_name: str, snr: Optional[float], hops: int, channel: str, lat: Optional[float] = None, lon: Optional[float] = None):
    if not node_name or node_name in ("Nodo Radio", "Unknown", "Utente"):
        return
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO heard_nodes (node_name, last_seen, last_snr, last_hops, last_channel, lat, lon, packets_count)
            VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(node_name) DO UPDATE SET
                last_seen = CURRENT_TIMESTAMP,
                last_snr = COALESCE(excluded.last_snr, heard_nodes.last_snr),
                last_hops = excluded.last_hops,
                last_channel = excluded.last_channel,
                lat = COALESCE(excluded.lat, heard_nodes.lat),
                lon = COALESCE(excluded.lon, heard_nodes.lon),
                packets_count = heard_nodes.packets_count + 1
        ''', (node_name, snr, hops, channel, lat, lon))
        conn.commit()
        conn.close()
    except Exception as e:
        print("DB record_heard_node error:", e)

def get_recent_heard_nodes(limit: int = 10) -> List[dict]:
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT node_name, last_seen, last_snr, last_hops, last_channel, lat, lon, packets_count FROM heard_nodes ORDER BY last_seen DESC LIMIT ?",
            (limit,)
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        print("DB get_recent_heard_nodes error:", e)
        return []

def bind_topic(chat_id: str, lora_channel_idx: int, thread_id: int):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO topic_bindings (chat_id, lora_channel_idx, message_thread_id)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id, lora_channel_idx) DO UPDATE SET
                message_thread_id = excluded.message_thread_id
        ''', (chat_id, lora_channel_idx, thread_id))
        conn.commit()
        conn.close()
    except Exception as e:
        print("DB bind_topic error:", e)

def unbind_topic(chat_id: str, lora_channel_idx: int):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM topic_bindings WHERE chat_id = ? AND lora_channel_idx = ?", (chat_id, lora_channel_idx))
        conn.commit()
        conn.close()
    except Exception as e:
        print("DB unbind_topic error:", e)

def get_topic_binding(chat_id: str, lora_channel_idx: int) -> Optional[int]:
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT message_thread_id FROM topic_bindings WHERE chat_id = ? AND lora_channel_idx = ?",
            (chat_id, lora_channel_idx)
        )
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        print("DB get_topic_binding error:", e)
        return None

def save_message(source: str, sender: str, channel: str, content: str, snr: Optional[float] = None, hops: Optional[int] = None):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO messages (source, sender, channel, content, snr, hops) VALUES (?, ?, ?, ?, ?, ?)",
            (source, sender, channel, content, snr, hops)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print("DB save_message error:", e)

def get_recent_messages(limit: int = 15, channel: Optional[str] = None) -> List[dict]:
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        if channel:
            cursor.execute(
                "SELECT id, timestamp, source, sender, channel, content, snr, hops FROM messages WHERE channel = ? ORDER BY id DESC LIMIT ?",
                (channel, limit)
            )
        else:
            cursor.execute(
                "SELECT id, timestamp, source, sender, channel, content, snr, hops FROM messages ORDER BY id DESC LIMIT ?",
                (limit,)
            )
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in reversed(rows)]
    except Exception as e:
        print("DB fetch error:", e)
        return []

def extract_gps_coords(text: str) -> Tuple[Optional[float], Optional[float]]:
    pattern = r"(-?\d{1,2}\.\d{3,7})[\s,;|/]+(-?\d{1,3}\.\d{3,7})"
    m = re.search(pattern, text)
    if m:
        try:
            lat, lon = float(m.group(1)), float(m.group(2))
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return lat, lon
        except Exception:
            pass
    return None, None

def format_signal_info(snr: Optional[float], path_len: Optional[int]) -> str:
    if snr is not None:
        if snr >= 5:
            q = "🟢 Ottimo"
        elif snr >= 0:
            q = "🟡 Buono"
        else:
            q = "🟠 Debole"
        snr_str = f"{snr:+.1f} dB ({q})"
    else:
        snr_str = "N/A"
    
    if path_len is None or path_len == 0 or path_len == 255:
        hops_str = "Diretto (0 salti)"
    else:
        hop_word = "salto" if path_len == 1 else "salti"
        hops_str = f"{path_len} {hop_word}"
    
    now_time = datetime.now().strftime("%H:%M")
    return f"📡 Segnale: <b>{snr_str}</b> | Salti: <b>{hops_str}</b> | ⏱️ {now_time}"

def build_message_inline_keyboard(ch_idx: int, ch_name: str) -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": f"💬 Rispondi su [{ch_name}]", "url": f"https://t.me/Meshcoreeliaxs_bot?start=c_{ch_idx}"},
                {"text": "👥 Nodi Ascoltati", "callback_data": "heard_nodes"}
            ]
        ]
    }

async def send_telegram(text: str, chat_id: str = TELEGRAM_CHAT_ID, reply_markup: Optional[dict] = None, message_thread_id: Optional[int] = None):
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    if message_thread_id:
        payload["message_thread_id"] = message_thread_id
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(url, json=payload)
    except Exception as e:
        print(f"Telegram send error to {chat_id}:", e)

async def broadcast_telegram(text: str, reply_markup: Optional[dict] = None, lora_channel_idx: Optional[int] = None):
    recipients = get_all_subscriptions()
    for cid in recipients:
        thread_id = None
        if lora_channel_idx is not None:
            thread_id = get_topic_binding(cid, lora_channel_idx)
        await send_telegram(text, chat_id=cid, reply_markup=reply_markup, message_thread_id=thread_id)

async def send_to_heltec(raw_bytes: bytes) -> bool:
    global active_heltec_ws, stats
    if active_heltec_ws:
        try:
            await active_heltec_ws.send_bytes(raw_bytes)
            stats["packets_tx"] += 1
            return True
        except Exception as e:
            print("Error sending to Heltec WebSocket:", e)
            active_heltec_ws = None
    return False

def build_channel_send_frame(channel_idx: int, text: str) -> bytes:
    now_ts = int(time.time())
    text_bytes = text.encode("utf-8")
    payload = bytes([3, 0, channel_idx]) + struct.pack("<I", now_ts) + text_bytes
    length = len(payload)
    return b"<" + struct.pack("<H", length) + payload

def build_get_channel_frame(channel_idx: int) -> bytes:
    payload = bytes([31, channel_idx])
    return b"<" + struct.pack("<H", len(payload)) + payload

def build_app_start_frame() -> bytes:
    payload = bytes([1, 0, 0, 0, 0, 0, 0, 0]) + b"TelegramBridge\x00"
    return b"<" + struct.pack("<H", len(payload)) + payload

def build_device_query_frame() -> bytes:
    payload = bytes([22, 3])
    return b"<" + struct.pack("<H", len(payload)) + payload

def build_sync_next_msg_frame() -> bytes:
    payload = bytes([10])
    return b"<" + struct.pack("<H", len(payload)) + payload

async def query_all_heltec_channels():
    if not active_heltec_ws:
        return
    await send_to_heltec(build_device_query_frame())
    await asyncio.sleep(0.05)
    await send_to_heltec(build_app_start_frame())
    await asyncio.sleep(0.05)
    for idx in range(16):
        await send_to_heltec(build_get_channel_frame(idx))
        await asyncio.sleep(0.05)

def resolve_channel(target_str: str) -> Optional[int]:
    clean = target_str.strip().lstrip("#")
    if clean.isdigit():
        idx = int(clean)
        if idx in discovered_channels:
            return idx
    clean_lower = clean.lower()
    for idx, name in discovered_channels.items():
        if name.lower() == clean_lower:
            return idx
    return None

def build_channels_keyboard(current_idx: int) -> dict:
    buttons = []
    row = []
    for idx in sorted(discovered_channels.keys()):
        name = discovered_channels[idx]
        indicator = "🟢 " if idx == current_idx else ""
        row.append({"text": f"{indicator}[{idx}] {name}", "callback_data": f"ch:{idx}"})
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([
        {"text": "🔄 Aggiorna lista", "callback_data": "refresh_channels"},
        {"text": "👥 Nodi Ascoltati", "callback_data": "heard_nodes"}
    ])
    return {"inline_keyboard": buttons}

async def generate_report_text() -> str:
    uptime_since = stats.get("connected_since", "N/A")
    heltec_status = "🟢 Connessa al cloud" if active_heltec_ws else "🔴 Non connessa"
    recent_nodes = get_recent_heard_nodes(6)
    nodes_summary = ""
    if recent_nodes:
        nodes_summary = "\n<b>Ultimi nodi ascoltati via radio:</b>\n" + "\n".join(
            f"• <b>{n['node_name']}</b> (SNR: {n['last_snr']:+.1f} dB, {n['last_hops']} hops)" if n['last_snr'] is not None else f"• <b>{n['node_name']}</b> ({n['last_hops']} hops)"
            for n in recent_nodes
        )
    else:
        nodes_summary = "\n<i>Nessun nodo ascoltato nelle ultime ore.</i>"

    ch_list = ", ".join([f"[{k}] {v}" for k, v in sorted(discovered_channels.items())])

    return (
        f"📋 <b>BOLLETTINO STAZIONE MESHCORE</b>\n\n"
        f"• <b>Nodo Locale:</b> <b>{node_info.get('name', 'Buscate')}</b>\n"
        f"• <b>Stato Heltec V3:</b> {heltec_status}\n"
        f"• <b>Connessione attiva da:</b> {uptime_since}\n"
        f"• <b>Parametri Radio:</b> {node_info.get('freq_mhz', 869.618)} MHz | BW {node_info.get('bw_khz', 62.5)} kHz | SF{node_info.get('sf', 8)} CR{node_info.get('cr', 8)}\n"
        f"• <b>Canali monitorati:</b> {ch_list}\n"
        f"• <b>Pacchetti totali:</b> RX {stats['packets_rx']} | TX {stats['packets_tx']}\n"
        f"{nodes_summary}"
    )

async def periodic_heartbeat_loop():
    # Wait 60s before initial check, then send report every 24 hours
    await asyncio.sleep(60)
    while True:
        try:
            await asyncio.sleep(3600 * 24)
            report = await generate_report_text()
            await broadcast_telegram(f"⏰ <i>Report Giornaliero Automatico:</i>\n\n{report}")
        except Exception as e:
            print("Heartbeat loop error:", e)
            await asyncio.sleep(60)

@app.on_event("startup")
async def startup_event():
    init_db()
    asyncio.create_task(telegram_polling_loop())
    asyncio.create_task(periodic_heartbeat_loop())

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "heltec_connected": active_heltec_ws is not None,
        "discovered_channels": discovered_channels,
        "node_info": node_info,
        "stats": stats,
        "active_web_clients": len(active_browser_clients),
        "recent_nodes": get_recent_heard_nodes(5)
    }

@app.get("/", response_class=HTMLResponse)
async def home():
    is_connected = active_heltec_ws is not None
    status_badge = '<span style="color: #22c55e; font-weight: bold;">● CONNESSA</span>' if is_connected else '<span style="color: #ef4444; font-weight: bold;">○ NON CONNESSA</span>'
    recent = get_recent_messages(25)
    recent_nodes = get_recent_heard_nodes(8)

    msg_blocks = []
    for m in recent:
        snr_badge = f" • SNR: {m['snr']:+.1f}dB" if m.get("snr") is not None else ""
        msg_blocks.append(
            f'<div style="margin-bottom: 8px; padding: 10px; background: #f3f4f6; border-radius: 8px;">'
            f'<small style="color: #6b7280;">[{m["timestamp"]}] ({m["source"]}) • Canale: <b style="color: #2563eb;">{m["channel"]}</b> • <b>{m["sender"]}</b>'
            f'{snr_badge}</small><br>'
            f'<div style="margin-top: 4px; font-size: 1.05rem;">{m["content"]}</div></div>'
        )
    messages_html = "".join(msg_blocks) or "<p style='color: #9ca3af;'>Nessun messaggio presente nella Room.</p>"

    channels_list_html = "".join(
        f'<span style="display:inline-block; margin: 4px 6px; padding: 4px 10px; background: #e0e7ff; color: #3730a3; border-radius: 20px; font-size: 0.9rem; font-weight: 500;">[{idx}] {name}</span>'
        for idx, name in sorted(discovered_channels.items())
    )

    node_blocks = []
    for n in recent_nodes:
        snr_badge = f", SNR: {n['last_snr']:+.1f}dB" if n.get("last_snr") is not None else ""
        node_blocks.append(
            f'<li style="margin-bottom: 4px;"><b>{n["node_name"]}</b>: {n["last_seen"]} '
            f'(Canale: {n["last_channel"]}{snr_badge})</li>'
        )
    nodes_html = "".join(node_blocks) or "<p style='color: #9ca3af;'>Nessun nodo ascoltato finora.</p>"

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>MeshCore Room Server & Telegram Bridge</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 20px; max-width: 760px; margin: auto; background: #fafafa; color: #111827; }}
            .card {{ background: white; padding: 20px; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); margin-bottom: 20px; }}
            h1 {{ font-size: 1.4rem; color: #1f2937; margin-top: 0; }}
            .stat {{ display: inline-block; margin-right: 20px; font-size: 0.95rem; margin-top: 6px; }}
            .cta-banner {{ background: linear-gradient(135deg, #10b981, #059669); color: white; padding: 18px 20px; border-radius: 12px; margin-bottom: 20px; box-shadow: 0 4px 14px rgba(16,185,129,0.3); display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 12px; }}
            .cta-btn {{ background: white; color: #047857; padding: 10px 20px; border-radius: 8px; font-weight: 700; text-decoration: none; display: inline-flex; align-items: center; gap: 6px; box-shadow: 0 2px 6px rgba(0,0,0,0.15); }}
        </style>
    </head>
    <body>
        <div class="cta-banner">
            <div>
                <div style="font-size: 1.15rem; font-weight: bold;">📱 MeshCore Web Client Disponibile!</div>
                <div style="font-size: 0.88rem; opacity: 0.95; margin-top: 2px;">Chat in tempo reale, canali e trasmissione radio LoRa dal tuo browser.</div>
            </div>
            <a href="/app" class="cta-btn">🚀 Apri Web App</a>
        </div>

        <div class="card">
            <h1>📡 MeshCore Room Server & Bridge</h1>
            <p>Stato Heltec V3: {status_badge} &nbsp;|&nbsp; Nodo: <b>{node_info.get('name', 'N/A')}</b> ({node_info.get('freq_mhz', 868.0)} MHz)</p>
            <div class="stat">RX: <b>{stats['packets_rx']}</b> pacchetti</div>
            <div class="stat">TX: <b>{stats['packets_tx']}</b> pacchetti</div>
            <div class="stat">Bot Telegram: <b>@Meshcoreeliaxs_bot</b></div>
            <div style="margin-top: 14px;">
                <b>Canali Heltec salvati:</b><br>
                {channels_list_html}
            </div>
        </div>
        <div class="card">
            <h2>👥 Nodi Ascoltati via Radio</h2>
            <ul>{nodes_html}</ul>
        </div>
        <div class="card">
            <h2>📜 Storico Room Server (tutti i canali)</h2>
            {messages_html}
        </div>
    </body>
    </html>
    """

@app.get("/app", response_class=HTMLResponse)
async def web_app():
    return HTMLResponse(get_web_client_html())

@app.get("/web", response_class=HTMLResponse)
async def web_alias():
    return HTMLResponse(get_web_client_html())

@app.websocket("/ws/client")
async def websocket_client_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_browser_clients.add(websocket)
    try:
        init_payload = {
            "type": "init",
            "heltec_connected": active_heltec_ws is not None,
            "node_info": node_info,
            "channels": discovered_channels,
            "stats": stats,
            "recent_messages": get_recent_messages(60),
            "recent_nodes": get_recent_heard_nodes(25)
        }
        await websocket.send_json(init_payload)

        while True:
            data = await websocket.receive_json()
            action = data.get("action")
            if action == "send_message":
                ch_idx = int(data.get("channel_idx", 0))
                text = str(data.get("text", "")).strip()
                sender = str(data.get("sender", "Web-Operatore")).strip() or "Web-Operatore"
                if text:
                    ch_name = discovered_channels.get(ch_idx, f"Canale {ch_idx}")
                    save_message("Web Client", sender, ch_name, text)
                    frame = build_channel_send_frame(ch_idx, f"[{sender}]: {text}")
                    sent = await send_to_heltec(frame)

                    tg_msg = f"🌐 <b>[Web Client ➔ Canale {ch_idx}: {ch_name}]</b>\n👤 <b>{sender}</b>: {text}"
                    await broadcast_telegram(tg_msg, lora_channel_idx=ch_idx)

                    await broadcast_to_browsers({
                        "type": "new_message",
                        "source": "Web Client",
                        "sender": sender,
                        "channel": ch_name,
                        "channel_idx": ch_idx,
                        "content": text,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "snr": None,
                        "hops": 0,
                        "sent_to_radio": sent
                    })
            elif action == "refresh_channels":
                if active_heltec_ws:
                    await query_all_heltec_channels()
            elif action == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print("Web client socket error:", e)
    finally:
        active_browser_clients.discard(websocket)

@app.get("/api/status")
async def api_status():
    return {
        "status": "ok",
        "heltec_connected": active_heltec_ws is not None,
        "discovered_channels": discovered_channels,
        "node_info": node_info,
        "stats": stats,
        "active_web_clients": len(active_browser_clients)
    }

@app.get("/api/messages")
async def api_messages(limit: int = 50, channel: Optional[str] = None):
    return get_recent_messages(limit, channel=channel)

@app.get("/api/nodes")
async def api_nodes(limit: int = 30):
    return get_recent_heard_nodes(limit)

@app.post("/api/send")
async def api_send(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    text = str(body.get("text", "")).strip()
    if not text:
        return JSONResponse({"error": "Empty message"}, status_code=400)

    ch_idx = int(body.get("channel_idx", 0))
    ch_name = discovered_channels.get(ch_idx, f"Canale {ch_idx}")
    sender = str(body.get("sender", "Web-Operatore")).strip() or "Web-Operatore"

    save_message("Web API", sender, ch_name, text)
    frame = build_channel_send_frame(ch_idx, f"[{sender}]: {text}")
    sent = await send_to_heltec(frame)

    tg_msg = f"🌐 <b>[Web API ➔ Canale {ch_idx}: {ch_name}]</b>\n👤 <b>{sender}</b>: {text}"
    await broadcast_telegram(tg_msg, lora_channel_idx=ch_idx)

    await broadcast_to_browsers({
        "type": "new_message",
        "source": "Web API",
        "sender": sender,
        "channel": ch_name,
        "channel_idx": ch_idx,
        "content": text,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "snr": None,
        "hops": 0,
        "sent_to_radio": sent
    })

    return {"success": True, "sent_to_heltec": sent, "channel": ch_name}

@app.websocket("/ws/mesh")
async def websocket_mesh_endpoint(websocket: WebSocket):
    global active_heltec_ws, stats, discovered_channels, node_info
    await websocket.accept()
    active_heltec_ws = websocket
    stats["connected_since"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stats["last_seen"] = stats["connected_since"]
    
    print("Heltec V3 connected via WebSocket!")
    await broadcast_telegram("🟢 <b>Heltec V3 collegata con successo al server Render!</b>\nInterrogo la scheda per leggere i canali configurati...")
    await broadcast_to_browsers({
        "type": "status",
        "heltec_connected": True,
        "node_info": node_info,
        "channels": discovered_channels,
        "stats": stats
    })

    asyncio.create_task(query_all_heltec_channels())

    try:
        while True:
            data = await websocket.receive_bytes()
            stats["packets_rx"] += 1
            stats["last_seen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if len(data) >= 3 and data[0] == ord('>'):
                length = struct.unpack("<H", data[1:3])[0]
                payload = data[3:3+length] if len(data) >= 3 + length else data[3:]
                if not payload:
                    continue

                code = payload[0]

                # RESP_CODE_CHANNEL_INFO = 18 (0x12)
                if code == 18 and len(payload) >= 34:
                    ch_idx = payload[1]
                    raw_name = payload[2:34]
                    ch_name = raw_name.split(b"\x00")[0].decode("utf-8", errors="ignore").strip()
                    if ch_name:
                        discovered_channels[ch_idx] = ch_name
                        print(f"Heltec Channel [{ch_idx}]: {ch_name}")
                        await broadcast_to_browsers({
                            "type": "channels",
                            "channels": discovered_channels
                        })

                # RESP_CODE_SELF_INFO = 5
                elif code == 5 and len(payload) >= 58:
                    try:
                        node_name = payload[58:].split(b"\x00")[0].decode("utf-8", errors="ignore").strip()
                        freq_khz = struct.unpack("<I", payload[48:52])[0]
                        bw_khz = struct.unpack("<I", payload[52:56])[0]
                        node_info["name"] = node_name or node_info["name"]
                        node_info["freq_mhz"] = freq_khz / 1000.0
                        node_info["bw_khz"] = bw_khz / 1000.0
                        node_info["sf"] = payload[56]
                        node_info["cr"] = payload[57]
                        print(f"Heltec Node Info: {node_name}, {freq_khz/1000.0} MHz")
                        await broadcast_to_browsers({
                            "type": "node_info",
                            "node_info": node_info
                        })
                    except Exception as e:
                        print("Error parsing SELF_INFO:", e)

                # RESP_CODE_DEVICE_INFO = 13
                elif code == 13 and len(payload) >= 4:
                    max_ch = payload[3]
                    node_info["max_channels"] = max_ch

                # PUSH_CODE_MSG_WAITING = 0x83 (131)
                elif code == 0x83:
                    await send_to_heltec(build_sync_next_msg_frame())

                # RESP_CODE_CHANNEL_MSG_RECV_V3 = 17 (0x11)
                elif code == 17 and len(payload) >= 11:
                    snr_raw = struct.unpack("b", bytes([payload[1]]))[0]
                    snr = snr_raw / 4.0
                    ch_idx = payload[4]
                    path_len = payload[5]
                    ch_name = discovered_channels.get(ch_idx, f"Canale #{ch_idx}")
                    msg_text = payload[11:].decode("utf-8", errors="ignore").strip()

                    # Parse sender and content
                    sender_name = "Nodo Radio"
                    content_text = msg_text
                    if ": " in msg_text:
                        parts = msg_text.split(": ", 1)
                        sender_name = parts[0].strip()
                        content_text = parts[1].strip()

                    # Check GPS coordinates
                    lat, lon = extract_gps_coords(content_text)
                    record_heard_node(sender_name, snr, path_len, ch_name, lat, lon)
                    save_message("LoRa Mesh", sender_name, ch_name, content_text, snr=snr, hops=path_len)

                    # Build rich Telegram post
                    formatted_post = (
                        f"📻 <b>[Canale {ch_idx}: {ch_name}]</b>\n"
                        f"👤 <b>{sender_name}</b>: {content_text}\n\n"
                        f"{format_signal_info(snr, path_len)}"
                    )
                    if lat and lon:
                        formatted_post += f"\n📍 <a href='https://www.openstreetmap.org/?mlat={lat}&mlon={lon}#map=14/{lat}/{lon}'>Apri Posizione su Mappa ({lat:.4f}, {lon:.4f})</a>"

                    reply_markup = build_message_inline_keyboard(ch_idx, ch_name)
                    await broadcast_telegram(formatted_post, reply_markup=reply_markup, lora_channel_idx=ch_idx)
                    await broadcast_to_browsers({
                        "type": "new_message",
                        "source": "LoRa Mesh",
                        "sender": sender_name,
                        "channel": ch_name,
                        "channel_idx": ch_idx,
                        "content": content_text,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "snr": snr,
                        "hops": path_len,
                        "lat": lat,
                        "lon": lon
                    })
                    await send_to_heltec(build_sync_next_msg_frame())

                # RESP_CODE_CHANNEL_MSG_RECV = 8 (0x08)
                elif code == 8 and len(payload) >= 8:
                    ch_idx = payload[1]
                    path_len = payload[2]
                    ch_name = discovered_channels.get(ch_idx, f"Canale #{ch_idx}")
                    msg_text = payload[8:].decode("utf-8", errors="ignore").strip()

                    sender_name = "Nodo Radio"
                    content_text = msg_text
                    if ": " in msg_text:
                        parts = msg_text.split(": ", 1)
                        sender_name = parts[0].strip()
                        content_text = parts[1].strip()

                    lat, lon = extract_gps_coords(content_text)
                    record_heard_node(sender_name, None, path_len, ch_name, lat, lon)
                    save_message("LoRa Mesh", sender_name, ch_name, content_text, hops=path_len)

                    formatted_post = (
                        f"📻 <b>[Canale {ch_idx}: {ch_name}]</b>\n"
                        f"👤 <b>{sender_name}</b>: {content_text}\n\n"
                        f"{format_signal_info(None, path_len)}"
                    )
                    if lat and lon:
                        formatted_post += f"\n📍 <a href='https://www.openstreetmap.org/?mlat={lat}&mlon={lon}#map=14/{lat}/{lon}'>Apri Posizione su Mappa ({lat:.4f}, {lon:.4f})</a>"

                    reply_markup = build_message_inline_keyboard(ch_idx, ch_name)
                    await broadcast_telegram(formatted_post, reply_markup=reply_markup, lora_channel_idx=ch_idx)
                    await broadcast_to_browsers({
                        "type": "new_message",
                        "source": "LoRa Mesh",
                        "sender": sender_name,
                        "channel": ch_name,
                        "channel_idx": ch_idx,
                        "content": content_text,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "snr": None,
                        "hops": path_len,
                        "lat": lat,
                        "lon": lon
                    })
                    await send_to_heltec(build_sync_next_msg_frame())

                # PUSH_CODE_NEW_ADVERT (0x8A) or PUSH_CODE_ADVERT (0x80)
                elif code in (0x8A, 0x80) and len(payload) >= 50:
                    try:
                        # Contact info from advert
                        out_path_len = payload[35] if len(payload) > 35 else 0
                        contact_name = payload[44:76].split(b"\x00")[0].decode("utf-8", errors="ignore").strip()
                        gps_lat_raw = struct.unpack("<i", payload[80:84])[0] if len(payload) >= 84 else 0
                        gps_lon_raw = struct.unpack("<i", payload[84:88])[0] if len(payload) >= 88 else 0
                        adv_lat = (gps_lat_raw / 1000000.0) if gps_lat_raw != 0 else None
                        adv_lon = (gps_lon_raw / 1000000.0) if gps_lon_raw != 0 else None
                        if contact_name:
                            record_heard_node(contact_name, None, out_path_len, "Advert", adv_lat, adv_lon)
                            print(f"Discovered LoRa Advert from: {contact_name}")
                            await broadcast_to_browsers({
                                "type": "nodes",
                                "nodes": get_recent_heard_nodes(25)
                            })
                    except Exception as e:
                        print("Error parsing advert frame:", e)

                # RESP_CODE_CONTACT_MSG_RECV_V3 = 16 or RESP_CODE_CONTACT_MSG_RECV = 7
                elif code in (16, 7):
                    offset_txt = 16 if code == 16 else 13
                    if len(payload) > offset_txt:
                        msg_text = payload[offset_txt:].decode("utf-8", errors="ignore").strip()
                        save_message("LoRa Mesh", "Nodo Radio", "Direct", msg_text)
                        await broadcast_telegram(f"💬 <b>[Messaggio Diretto LoRa]</b>\n{msg_text}")
                        await broadcast_to_browsers({
                            "type": "new_message",
                            "source": "LoRa Mesh",
                            "sender": "Nodo Radio",
                            "channel": "Direct",
                            "channel_idx": 0,
                            "content": msg_text,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "snr": None,
                            "hops": 0
                        })
                    await send_to_heltec(build_sync_next_msg_frame())

                # RESP_CODE_NO_MORE_MESSAGES = 10
                elif code == 10:
                    pass

                # RESP_CODE_OK = 0
                elif code == 0:
                    pass

    except WebSocketDisconnect:
        print("Heltec V3 disconnected.")
    except Exception as e:
        print("WebSocket exception:", e)
    finally:
        if active_heltec_ws == websocket:
            active_heltec_ws = None
        await broadcast_telegram("🔴 <b>Heltec V3 disconnessa dal server Render.</b>\nIn attesa di riconnessione automatica...")
        await broadcast_to_browsers({
            "type": "status",
            "heltec_connected": False,
            "node_info": node_info,
            "channels": discovered_channels,
            "stats": stats
        })

async def handle_callback_query(cq: dict, client: httpx.AsyncClient):
    cq_id = cq["id"]
    chat_id = str(cq.get("message", {}).get("chat", {}).get("id", ""))
    data = cq.get("data", "")

    if data.startswith("ch:"):
        ch_idx = int(data.split(":")[1])
        chat_active_channel[chat_id] = ch_idx
        ch_name = discovered_channels.get(ch_idx, f"Canale {ch_idx}")
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
            json={"callback_query_id": cq_id, "text": f"Canale attivo impostato: [{ch_idx}] {ch_name}"}
        )
        await send_telegram(
            f"✅ <b>Canale selezionato: [{ch_idx}] {ch_name}</b>\n"
            f"Tutti i messaggi successivi senza prefisso verranno trasmessi su questo canale.\n\n"
            f"<i>Suggerimento: puoi anche scrivere a un canale specifico usando <code>#canale messaggio</code> o <code>/c {ch_idx} messaggio</code></i>.",
            chat_id=chat_id,
            reply_markup=build_channels_keyboard(ch_idx)
        )
    elif data == "refresh_channels":
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
            json={"callback_query_id": cq_id, "text": "Aggiorno i canali dalla Heltec..."}
        )
        if active_heltec_ws:
            await query_all_heltec_channels()
            await asyncio.sleep(1.0)
            active_idx = chat_active_channel.get(chat_id, default_channel_idx)
            await send_telegram(
                "🔄 <b>Canali aggiornati dalla Heltec!</b>",
                chat_id=chat_id,
                reply_markup=build_channels_keyboard(active_idx)
            )
        else:
            await send_telegram("⚠️ Heltec non connessa al cloud in questo momento.", chat_id=chat_id)
    elif data == "heard_nodes":
        await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
            json={"callback_query_id": cq_id}
        )
        nodes = get_recent_heard_nodes(10)
        if not nodes:
            await send_telegram("ℹ️ Nessun nodo radio rilevato nelle ultime ore.", chat_id=chat_id)
        else:
            lines = ["👥 <b>Nodi Radio Ascoltati di Recente:</b>\n"]
            for n in nodes:
                snr_info = f", SNR: {n['last_snr']:+.1f}dB" if n["last_snr"] is not None else ""
                lines.append(f"• <b>{n['node_name']}</b> (Canale: {n['last_channel']}{snr_info}, {n['last_hops']} salti)")
                n_lat = n.get("lat")
                n_lon = n.get("lon")
                if n_lat and n_lon:
                    lines.append(f"  📍 <a href='https://www.openstreetmap.org/?mlat={n_lat}&mlon={n_lon}#map=14/{n_lat}/{n_lon}'>Posizione GPS ({n_lat:.4f}, {n_lon:.4f})</a>")
            await send_telegram("\n".join(lines), chat_id=chat_id)

async def telegram_polling_loop():
    if not TELEGRAM_BOT_TOKEN:
        return

    offset = 0
    client = httpx.AsyncClient(timeout=30.0)

    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                for update in data.get("result", []):
                    offset = update["update_id"] + 1

                    if "callback_query" in update:
                        await handle_callback_query(update["callback_query"], client)
                        continue

                    msg = update.get("message") or update.get("channel_post")
                    if not msg:
                        continue

                    text = msg.get("text", "")
                    sender_obj = msg.get("from") or {}
                    chat_obj = msg.get("chat") or {}
                    chat_id = str(chat_obj.get("id", ""))
                    chat_type = chat_obj.get("type", "private")
                    chat_title = chat_obj.get("title") or sender_obj.get("first_name") or "Canale"
                    sender_name = sender_obj.get("first_name") or msg.get("author_signature") or chat_title
                    thread_id = msg.get("message_thread_id")

                    if chat_id:
                        register_subscription(chat_id, chat_type, chat_title)

                    if not text:
                        continue

                    active_idx = chat_active_channel.get(chat_id, default_channel_idx)
                    active_name = discovered_channels.get(active_idx, f"Canale #{active_idx}")

                    if text.startswith("/start"):
                        parts = text.split(maxsplit=1)
                        if len(parts) > 1 and parts[1].startswith("c_"):
                            # Deep link: /start c_1 -> set active channel to 1
                            target_c = parts[1][2:]
                            resolved = resolve_channel(target_c)
                            if resolved is not None:
                                chat_active_channel[chat_id] = resolved
                                active_idx = resolved
                                active_name = discovered_channels[resolved]
                                await send_telegram(
                                    f"✅ Canale impostato su: <b>[{resolved}] {active_name}</b>!\nScrivi il tuo messaggio per trasmetterlo via radio.",
                                    chat_id=chat_id,
                                    reply_markup=build_channels_keyboard(active_idx)
                                )
                                continue

                        status_str = "🟢 Connessa" if active_heltec_ws else "🔴 Non connessa (in attesa di segnale)"
                        welcome_text = (
                            f"👋 Ciao <b>{sender_name}</b>!\n\n"
                            f"Sono la tua stazione <b>MeshCore Room Bot & Bridge Avanzato</b>.\n"
                            f"• Stato Heltec V3: <b>{status_str}</b>\n"
                            f"• Canale LoRa attivo: <b>[{active_idx}] {active_name}</b>\n\n"
                            f"<b>Comandi Canali:</b>\n"
                            f"• <code>/canali</code>: Lista canali e bottoni di selezione\n"
                            f"• <code>/canale [nome|numero]</code>: Seleziona canale attivo\n"
                            f"• <code>/c [nome|numero] [testo]</code>: Invia a un canale specifico\n"
                            f"• <code>#canale [testo]</code>: Invia a un canale al volo\n\n"
                            f"<b>Funzioni Avanzate:</b>\n"
                            f"• <code>/app</code>: Apri il Web Client grafico dal browser\n"
                            f"• <code>/nodi</code>: Registro dei nodi radio ascoltati\n"
                            f"• <code>/report</code>: Bollettino tecnico della stazione\n"
                            f"• <code>/associa_topic [canale]</code>: Collega questo topic Telegram al canale LoRa\n"
                            f"• <code>/room</code>: Mostra gli ultimi messaggi salvati"
                        )
                        await send_telegram(welcome_text, chat_id=chat_id, reply_markup=build_channels_keyboard(active_idx), message_thread_id=thread_id)

                    elif text.startswith("/status"):
                        status_str = "🟢 Connessa" if active_heltec_ws else "🔴 Non connessa"
                        ch_summary = ", ".join([f"[{k}] {v}" for k, v in sorted(discovered_channels.items())])
                        await send_telegram(
                            f"📊 <b>Stato MeshCore Bridge</b>\n"
                            f"• Heltec V3: {status_str}\n"
                            f"• Nome Nodo: <b>{node_info.get('name', 'N/A')}</b>\n"
                            f"• Frequenza: <b>{node_info.get('freq_mhz', 868.0)} MHz</b>\n"
                            f"• Canale chat attivo: <b>[{active_idx}] {active_name}</b>\n"
                            f"• Canali noti: {ch_summary}\n"
                            f"• Pacchetti RX: {stats['packets_rx']}\n"
                            f"• Pacchetti TX: {stats['packets_tx']}\n"
                            f"• Ultimo contatto: {stats.get('last_seen', 'N/A')}",
                            chat_id=chat_id,
                            reply_markup=build_channels_keyboard(active_idx),
                            message_thread_id=thread_id
                        )

                    elif text.startswith("/nodi") or text.startswith("/heard"):
                        nodes = get_recent_heard_nodes(12)
                        if not nodes:
                            await send_telegram("ℹ️ Nessun nodo radio memorizzato di recente.", chat_id=chat_id, message_thread_id=thread_id)
                        else:
                            lines = ["👥 <b>Nodi Radio Ascoltati di Recente:</b>\n"]
                            for n in nodes:
                                snr_info = f", SNR: {n['last_snr']:+.1f}dB" if n["last_snr"] is not None else ""
                                lines.append(f"• <b>{n['node_name']}</b> (Canale: {n['last_channel']}{snr_info}, {n['last_hops']} salti)")
                                n_lat = n.get("lat")
                                n_lon = n.get("lon")
                                if n_lat and n_lon:
                                    lines.append(f"  📍 <a href='https://www.openstreetmap.org/?mlat={n_lat}&mlon={n_lon}#map=14/{n_lat}/{n_lon}'>Posizione GPS ({n_lat:.4f}, {n_lon:.4f})</a>")
                            await send_telegram("\n".join(lines), chat_id=chat_id, message_thread_id=thread_id)

                    elif text.startswith("/report") or text.startswith("/bollettino"):
                        rep = await generate_report_text()
                        await send_telegram(rep, chat_id=chat_id, message_thread_id=thread_id)

                    elif text.startswith("/associa_topic") or text.startswith("/bind_topic"):
                        if not thread_id:
                            await send_telegram("⚠️ Questo comando va eseguito dentro al Topic (Forum) di Telegram che vuoi associare.", chat_id=chat_id)
                        else:
                            parts = text.split(maxsplit=1)
                            if len(parts) < 2:
                                await send_telegram("ℹ️ Formato: <code>/associa_topic &lt;canale&gt;</code> (es: <code>/associa_topic Piemonte</code>)", chat_id=chat_id, message_thread_id=thread_id)
                            else:
                                target_ch = parts[1].strip()
                                resolved = resolve_channel(target_ch)
                                if resolved is None:
                                    await send_telegram(f"⚠️ Canale '{target_ch}' non trovato.", chat_id=chat_id, message_thread_id=thread_id)
                                else:
                                    bind_topic(chat_id, resolved, thread_id)
                                    ch_name = discovered_channels[resolved]
                                    await send_telegram(
                                        f"✅ <b>Topic associato con successo!</b>\nDa adesso i messaggi del canale radio <b>[{resolved}] {ch_name}</b> appariranno qui.",
                                        chat_id=chat_id,
                                        message_thread_id=thread_id
                                    )

                    elif text.startswith("/canali") or text.startswith("/channels"):
                        ch_lines = []
                        for idx, name in sorted(discovered_channels.items()):
                            indicator = "🟢 <b>(Attivo)</b>" if idx == active_idx else ""
                            ch_lines.append(f"• <b>[{idx}] {name}</b> {indicator}")
                        ch_text = "\n".join(ch_lines)
                        await send_telegram(
                            f"📻 <b>Canali salvati sulla scheda Heltec:</b>\n\n"
                            f"{ch_text}\n\n"
                            f"<i>Puoi toccare un bottone in basso per selezionare il canale in cui scrivere, oppure usare <code>/canale [nome|numero]</code>.</i>",
                            chat_id=chat_id,
                            reply_markup=build_channels_keyboard(active_idx),
                            message_thread_id=thread_id
                        )

                    elif text.startswith("/canale") or text.startswith("/channel") or text.startswith("/setchannel"):
                        parts = text.split(maxsplit=1)
                        if len(parts) < 2:
                            await send_telegram(
                                f"ℹ️ Canale attualmente attivo: <b>[{active_idx}] {active_name}</b>\n"
                                f"Usa <code>/canale &lt;numero o nome&gt;</code> per cambiarlo, oppure usa <code>/canali</code>.",
                                chat_id=chat_id,
                                reply_markup=build_channels_keyboard(active_idx),
                                message_thread_id=thread_id
                            )
                        else:
                            target = parts[1].strip()
                            resolved = resolve_channel(target)
                            if resolved is not None:
                                chat_active_channel[chat_id] = resolved
                                new_name = discovered_channels[resolved]
                                await send_telegram(
                                    f"✅ Canale attivo impostato su: <b>[{resolved}] {new_name}</b>.\n"
                                    f"Tutti i tuoi prossimi messaggi verranno trasmessi su questo canale.",
                                    chat_id=chat_id,
                                    reply_markup=build_channels_keyboard(resolved),
                                    message_thread_id=thread_id
                                )
                            else:
                                await send_telegram(
                                    f"⚠️ Canale '<code>{target}</code>' non trovato sulla scheda Heltec.\n"
                                    f"Digita <code>/canali</code> per vedere i canali disponibili.",
                                    chat_id=chat_id,
                                    message_thread_id=thread_id
                                )

                    elif text.startswith("/app") or text.startswith("/web"):
                        await send_telegram(
                            "🌐 <b>MeshCore Web Client</b>\n\n"
                            "Puoi aprire la console web interattiva direttamente dal browser del telefono o PC:\n"
                            "🔗 https://meshcore-room-bot.onrender.com/app\n\n"
                            "<i>Include chat in tempo reale, cambio canali con un tocco, telemetria radio e lista dei nodi ascoltati.</i>",
                            chat_id=chat_id,
                            message_thread_id=thread_id
                        )

                    elif text.startswith("/c ") or text.startswith("/channel_msg "):
                        parts = text.split(maxsplit=2)
                        if len(parts) < 3:
                            await send_telegram("ℹ️ Formato: <code>/c &lt;canale&gt; &lt;messaggio&gt;</code> (es: <code>/c 1 Ciao a tutti</code>)", chat_id=chat_id, message_thread_id=thread_id)
                        else:
                            target_ch = parts[1]
                            content = parts[2].strip()
                            resolved = resolve_channel(target_ch)
                            if resolved is None:
                                await send_telegram(f"⚠️ Canale '{target_ch}' non trovato. Usa <code>/canali</code>.", chat_id=chat_id, message_thread_id=thread_id)
                            else:
                                ch_name = discovered_channels[resolved]
                                save_message("Telegram", sender_name, ch_name, content)
                                frame = build_channel_send_frame(resolved, f"[{sender_name}]: {content}")
                                sent = await send_to_heltec(frame)
                                await broadcast_to_browsers({
                                    "type": "new_message",
                                    "source": "Telegram",
                                    "sender": sender_name,
                                    "channel": ch_name,
                                    "channel_idx": resolved,
                                    "content": content,
                                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    "snr": None,
                                    "hops": 0
                                })
                                if sent:
                                    await send_telegram(f"📡 <i>Trasmesso su [Canale {resolved}: {ch_name}] via LoRa:</i>\n\"{content}\"", chat_id=chat_id, message_thread_id=thread_id)
                                else:
                                    await send_telegram(f"💾 Salvato nella Room locale su [{ch_name}]. (Heltec offline).", chat_id=chat_id, message_thread_id=thread_id)

                    elif text.startswith("/room") or text.startswith("/history"):
                        recent = get_recent_messages(12)
                        if not recent:
                            await send_telegram("📭 Nessun messaggio memorizzato nella Room al momento.", chat_id=chat_id, message_thread_id=thread_id)
                        else:
                            history_text = "📜 <b>Ultimi messaggi Room Server:</b>\n\n"
                            for m in recent:
                                snr_str = f" (SNR {m['snr']:+.1f}dB)" if m.get("snr") is not None else ""
                                history_text += f"• <i>[{m['timestamp'][11:16]}]</i> [<b>{m['channel']}</b>] <b>{m['sender']}</b>{snr_str}: {m['content']}\n"
                            await send_telegram(history_text, chat_id=chat_id, message_thread_id=thread_id)

                    elif text.startswith("/aggiorna_canali") or text.startswith("/refresh"):
                        if active_heltec_ws:
                            await query_all_heltec_channels()
                            await asyncio.sleep(1.0)
                            await send_telegram("🔄 Interrogazione canali completata!", chat_id=chat_id, reply_markup=build_channels_keyboard(active_idx), message_thread_id=thread_id)
                        else:
                            await send_telegram("⚠️ Heltec non connessa al cloud in questo momento.", chat_id=chat_id, message_thread_id=thread_id)

                    else:
                        target_ch_idx = active_idx
                        target_ch_name = active_name
                        payload_text = text

                        if text.startswith("#"):
                            tokens = text.split(maxsplit=1)
                            first_token = tokens[0][1:]
                            resolved = resolve_channel(first_token)
                            if resolved is not None:
                                target_ch_idx = resolved
                                target_ch_name = discovered_channels[resolved]
                                payload_text = tokens[1] if len(tokens) > 1 else ""

                        if not payload_text:
                            continue

                        save_message("Telegram", sender_name, target_ch_name, payload_text)
                        frame = build_channel_send_frame(target_ch_idx, f"[{sender_name}]: {payload_text}")
                        sent = await send_to_heltec(frame)
                        await broadcast_to_browsers({
                            "type": "new_message",
                            "source": "Telegram",
                            "sender": sender_name,
                            "channel": target_ch_name,
                            "channel_idx": target_ch_idx,
                            "content": payload_text,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "snr": None,
                            "hops": 0
                        })
                        if sent:
                            await send_telegram(f"📡 <i>Trasmesso su [Canale {target_ch_idx}: {target_ch_name}] via LoRa:</i>\n\"{payload_text}\"", chat_id=chat_id, message_thread_id=thread_id)
                        else:
                            await send_telegram(f"💾 Salvato nella Room locale [{target_ch_name}]. (Heltec offline, non trasmesso via radio).", chat_id=chat_id, message_thread_id=thread_id)

        except Exception as e:
            print("Telegram polling error:", e)
            await asyncio.sleep(5)

        await asyncio.sleep(1)


