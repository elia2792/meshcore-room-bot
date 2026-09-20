import asyncio
import os
import struct
import sqlite3
import time
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
import httpx

# Configuration
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8799543336:AAExe5g_-6ud-4kAX4p_EVtovS_R7KqSToM")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "365061699")
DB_PATH = os.getenv("DB_PATH", "room_messages.db")

app = FastAPI(title="MeshCore Telegram Room Bridge")

# Global State
active_heltec_ws: Optional[WebSocket] = None
stats = {
    "packets_rx": 0,
    "packets_tx": 0,
    "connected_since": None,
    "last_seen": None
}

# MeshCore Channels and Node Cache
# Default: channel 0 is always Public
discovered_channels: Dict[int, str] = {0: "Public"}
chat_active_channel: Dict[str, int] = {}  # chat_id -> channel_idx
default_channel_idx: int = 0
node_info: Dict[str, Any] = {
    "name": "MeshNode",
    "firmware": "MeshCore",
    "freq_mhz": 868.0,
    "bw_khz": 250.0,
    "sf": 10,
    "cr": 5,
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
            content TEXT
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

def save_message(source: str, sender: str, channel: str, content: str):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO messages (source, sender, channel, content) VALUES (?, ?, ?, ?)",
            (source, sender, channel, content)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print("DB error:", e)

def get_recent_messages(limit: int = 15, channel: Optional[str] = None) -> List[dict]:
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        if channel:
            cursor.execute(
                "SELECT id, timestamp, source, sender, channel, content FROM messages WHERE channel = ? ORDER BY id DESC LIMIT ?",
                (channel, limit)
            )
        else:
            cursor.execute(
                "SELECT id, timestamp, source, sender, channel, content FROM messages ORDER BY id DESC LIMIT ?",
                (limit,)
            )
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in reversed(rows)]
    except Exception as e:
        print("DB fetch error:", e)
        return []

async def send_telegram(text: str, chat_id: str = TELEGRAM_CHAT_ID, reply_markup: Optional[dict] = None):
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(url, json=payload)
    except Exception as e:
        print(f"Telegram send error to {chat_id}:", e)

async def broadcast_telegram(text: str, reply_markup: Optional[dict] = None):
    recipients = get_all_subscriptions()
    for cid in recipients:
        await send_telegram(text, chat_id=cid, reply_markup=reply_markup)

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
    # CMD_SEND_CHANNEL_TXT_MSG = 3
    now_ts = int(time.time())
    text_bytes = text.encode("utf-8")
    payload = bytes([3, 0, channel_idx]) + struct.pack("<I", now_ts) + text_bytes
    length = len(payload)
    return b"<" + struct.pack("<H", length) + payload

def build_get_channel_frame(channel_idx: int) -> bytes:
    # CMD_GET_CHANNEL = 31 (0x1F)
    payload = bytes([31, channel_idx])
    return b"<" + struct.pack("<H", len(payload)) + payload

def build_app_start_frame() -> bytes:
    # CMD_APP_START = 1
    payload = bytes([1, 0, 0, 0, 0, 0, 0, 0]) + b"TelegramBridge\x00"
    return b"<" + struct.pack("<H", len(payload)) + payload

def build_device_query_frame() -> bytes:
    # CMD_DEVICE_QUERY = 22, app_target_ver = 3
    payload = bytes([22, 3])
    return b"<" + struct.pack("<H", len(payload)) + payload

def build_sync_next_msg_frame() -> bytes:
    # CMD_SYNC_NEXT_MESSAGE = 10
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
    buttons.append([{"text": "🔄 Aggiorna lista dalla Heltec", "callback_data": "refresh_channels"}])
    return {"inline_keyboard": buttons}

@app.on_event("startup")
async def startup_event():
    init_db()
    asyncio.create_task(telegram_polling_loop())

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "heltec_connected": active_heltec_ws is not None,
        "discovered_channels": discovered_channels,
        "node_info": node_info,
        "stats": stats
    }

@app.get("/", response_class=HTMLResponse)
async def home():
    is_connected = active_heltec_ws is not None
    status_badge = '<span style="color: #22c55e; font-weight: bold;">● CONNESSA</span>' if is_connected else '<span style="color: #ef4444; font-weight: bold;">○ NON CONNESSA</span>'
    recent = get_recent_messages(25)
    messages_html = "".join(
        f'<div style="margin-bottom: 8px; padding: 10px; background: #f3f4f6; border-radius: 8px;">'
        f'<small style="color: #6b7280;">[{m["timestamp"]}] ({m["source"]}) • Canale: <b style="color: #2563eb;">{m["channel"]}</b> • <b>{m["sender"]}</b>:</small><br>'
        f'<div style="margin-top: 4px; font-size: 1.05rem;">{m["content"]}</div></div>'
        for m in recent
    ) or "<p style='color: #9ca3af;'>Nessun messaggio presente nella Room.</p>"

    channels_list_html = "".join(
        f'<span style="display:inline-block; margin: 4px 6px; padding: 4px 10px; background: #e0e7ff; color: #3730a3; border-radius: 20px; font-size: 0.9rem; font-weight: 500;">[{idx}] {name}</span>'
        for idx, name in sorted(discovered_channels.items())
    )

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
        </style>
    </head>
    <body>
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
            <h2>📜 Storico Room Server (tutti i canali)</h2>
            {messages_html}
        </div>
    </body>
    </html>
    """

@app.websocket("/ws/mesh")
async def websocket_mesh_endpoint(websocket: WebSocket):
    global active_heltec_ws, stats, discovered_channels, node_info
    await websocket.accept()
    active_heltec_ws = websocket
    stats["connected_since"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stats["last_seen"] = stats["connected_since"]
    
    print("Heltec V3 connected via WebSocket!")
    await broadcast_telegram("🟢 <b>Heltec V3 collegata con successo al server Render!</b>\nInterrogo la scheda per leggere i canali configurati...")

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
                    ch_idx = payload[4]
                    ch_name = discovered_channels.get(ch_idx, f"Canale #{ch_idx}")
                    msg_text = payload[11:].decode("utf-8", errors="ignore").strip()
                    save_message("LoRa Mesh", "Nodo Radio", ch_name, msg_text)
                    await broadcast_telegram(f"📻 <b>[Canale {ch_idx}: {ch_name}]</b>\n{msg_text}")
                    await send_to_heltec(build_sync_next_msg_frame())

                # RESP_CODE_CHANNEL_MSG_RECV = 8 (0x08)
                elif code == 8 and len(payload) >= 8:
                    ch_idx = payload[1]
                    ch_name = discovered_channels.get(ch_idx, f"Canale #{ch_idx}")
                    msg_text = payload[8:].decode("utf-8", errors="ignore").strip()
                    save_message("LoRa Mesh", "Nodo Radio", ch_name, msg_text)
                    await broadcast_telegram(f"📻 <b>[Canale {ch_idx}: {ch_name}]</b>\n{msg_text}")
                    await send_to_heltec(build_sync_next_msg_frame())

                # RESP_CODE_CONTACT_MSG_RECV_V3 = 16 or RESP_CODE_CONTACT_MSG_RECV = 7
                elif code in (16, 7):
                    offset_txt = 16 if code == 16 else 13
                    if len(payload) > offset_txt:
                        msg_text = payload[offset_txt:].decode("utf-8", errors="ignore").strip()
                        save_message("LoRa Mesh", "Nodo Radio", "Direct", msg_text)
                        await broadcast_telegram(f"💬 <b>[Messaggio Diretto LoRa]</b>\n{msg_text}")
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
                    chat_type = chat_obj.get("type", "private") # "private", "group", "supergroup", "channel"
                    chat_title = chat_obj.get("title") or sender_obj.get("first_name") or "Canale"
                    sender_name = sender_obj.get("first_name") or msg.get("author_signature") or chat_title

                    if chat_id:
                        register_subscription(chat_id, chat_type, chat_title)

                    if not text:
                        continue

                    active_idx = chat_active_channel.get(chat_id, default_channel_idx)
                    active_name = discovered_channels.get(active_idx, f"Canale #{active_idx}")

                    if text.startswith("/start"):
                        status_str = "🟢 Connessa" if active_heltec_ws else "🔴 Non connessa (in attesa di segnale)"
                        welcome_text = (
                            f"👋 Ciao <b>{sender_name}</b>!\n\n"
                            f"Sono il tuo <b>MeshCore Room Bot & Canali Bridge</b>.\n"
                            f"• Stato Heltec V3: <b>{status_str}</b>\n"
                            f"• Canale LoRa attivo: <b>[{active_idx}] {active_name}</b>\n\n"
                            f"<b>Comandi Canali:</b>\n"
                            f"• <code>/canali</code> o <code>/channels</code>: Lista canali e bottoni di selezione\n"
                            f"• <code>/canale [nome|numero]</code>: Seleziona canale attivo\n"
                            f"• <code>/c [nome|numero] [testo]</code>: Invia a un canale specifico\n"
                            f"• <code>#canale [testo]</code>: Invia a un canale al volo (es: <i>#emergenza test</i>)\n\n"
                            f"<b>Comandi Generali:</b>\n"
                            f"• <code>/status</code>: Info tecniche sul nodo e pacchetti\n"
                            f"• <code>/room</code>: Mostra gli ultimi messaggi salvati nella bacheca"
                        )
                        await send_telegram(welcome_text, chat_id=chat_id, reply_markup=build_channels_keyboard(active_idx))

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
                            reply_markup=build_channels_keyboard(active_idx)
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
                            reply_markup=build_channels_keyboard(active_idx)
                        )

                    elif text.startswith("/canale") or text.startswith("/channel") or text.startswith("/setchannel"):
                        parts = text.split(maxsplit=1)
                        if len(parts) < 2:
                            await send_telegram(
                                f"ℹ️ Canale attualmente attivo: <b>[{active_idx}] {active_name}</b>\n"
                                f"Usa <code>/canale &lt;numero o nome&gt;</code> per cambiarlo, oppure usa <code>/canali</code>.",
                                chat_id=chat_id,
                                reply_markup=build_channels_keyboard(active_idx)
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
                                    reply_markup=build_channels_keyboard(resolved)
                                )
                            else:
                                await send_telegram(
                                    f"⚠️ Canale '<code>{target}</code>' non trovato sulla scheda Heltec.\n"
                                    f"Digita <code>/canali</code> per vedere i canali disponibili.",
                                    chat_id=chat_id
                                )

                    elif text.startswith("/c ") or text.startswith("/channel_msg "):
                        parts = text.split(maxsplit=2)
                        if len(parts) < 3:
                            await send_telegram("ℹ️ Formato: <code>/c &lt;canale&gt; &lt;messaggio&gt;</code> (es: <code>/c 1 Ciao a tutti</code>)", chat_id=chat_id)
                        else:
                            target_ch = parts[1]
                            content = parts[2].strip()
                            resolved = resolve_channel(target_ch)
                            if resolved is None:
                                await send_telegram(f"⚠️ Canale '{target_ch}' non trovato. Usa <code>/canali</code>.", chat_id=chat_id)
                            else:
                                ch_name = discovered_channels[resolved]
                                save_message("Telegram", sender_name, ch_name, content)
                                frame = build_channel_send_frame(resolved, f"[{sender_name}]: {content}")
                                sent = await send_to_heltec(frame)
                                if sent:
                                    await send_telegram(f"📡 <i>Trasmesso su [Canale {resolved}: {ch_name}] via LoRa:</i>\n\"{content}\"", chat_id=chat_id)
                                else:
                                    await send_telegram(f"💾 Salvato nella Room locale su [{ch_name}]. (Heltec offline).", chat_id=chat_id)

                    elif text.startswith("/room") or text.startswith("/history"):
                        recent = get_recent_messages(12)
                        if not recent:
                            await send_telegram("📭 Nessun messaggio memorizzato nella Room al momento.", chat_id=chat_id)
                        else:
                            history_text = "📜 <b>Ultimi messaggi Room Server:</b>\n\n"
                            for m in recent:
                                history_text += f"• <i>[{m['timestamp'][11:16]}]</i> [<b>{m['channel']}</b>] <b>{m['sender']}</b>: {m['content']}\n"
                            await send_telegram(history_text, chat_id=chat_id)

                    elif text.startswith("/aggiorna_canali") or text.startswith("/refresh"):
                        if active_heltec_ws:
                            await query_all_heltec_channels()
                            await asyncio.sleep(1.0)
                            await send_telegram("🔄 Interrogazione canali completata!", chat_id=chat_id, reply_markup=build_channels_keyboard(active_idx))
                        else:
                            await send_telegram("⚠️ Heltec non connessa al cloud in questo momento.", chat_id=chat_id)

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
                        if sent:
                            await send_telegram(f"📡 <i>Trasmesso su [Canale {target_ch_idx}: {target_ch_name}] via LoRa:</i>\n\"{payload_text}\"", chat_id=chat_id)
                        else:
                            await send_telegram(f"💾 Salvato nella Room locale [{target_ch_name}]. (Heltec offline, non trasmesso via radio).", chat_id=chat_id)

        except Exception as e:
            print("Telegram polling error:", e)
            await asyncio.sleep(5)

        await asyncio.sleep(1)

