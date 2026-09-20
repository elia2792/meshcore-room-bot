import asyncio
import os
import struct
import sqlite3
import time
from datetime import datetime
from typing import Optional, List

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
    conn.commit()
    conn.close()

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

def get_recent_messages(limit: int = 15) -> List[dict]:
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
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

async def send_telegram(text: str, chat_id: str = TELEGRAM_CHAT_ID):
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"})
    except Exception as e:
        print("Telegram send error:", e)

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

def build_meshcore_send_frame(text: str) -> bytes:
    # MeshCore companion command: start '<' + len(2 bytes LE) + CMD_SEND_TXT_MSG (or broadcast payload)
    # CMD 2 is typically send text message in Companion API
    payload = bytes([0x02]) + text.encode("utf-8")
    length = len(payload)
    return b"<" + struct.pack("<H", length) + payload

@app.on_event("startup")
async def startup_event():
    init_db()
    asyncio.create_task(telegram_polling_loop())

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "heltec_connected": active_heltec_ws is not None,
        "stats": stats
    }

@app.get("/", response_class=HTMLResponse)
async def home():
    is_connected = active_heltec_ws is not None
    status_badge = '<span style="color: #22c55e; font-weight: bold;">● CONNESSA</span>' if is_connected else '<span style="color: #ef4444; font-weight: bold;">○ NON CONNESSA</span>'
    recent = get_recent_messages(20)
    messages_html = "".join(
        f'<div style="margin-bottom: 8px; padding: 8px; background: #f3f4f6; border-radius: 6px;">'
        f'<small style="color: #6b7280;">[{m["timestamp"]}] ({m["source"]}) <b>{m["sender"]}</b>:</small><br>'
        f'<span>{m["content"]}</span></div>'
        for m in recent
    ) or "<p style='color: #9ca3af;'>Nessun messaggio presente nella Room.</p>"

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>MeshCore Room Server & Telegram Bridge</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 20px; max-width: 700px; margin: auto; background: #fafafa; color: #111827; }}
            .card {{ background: white; padding: 20px; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); margin-bottom: 20px; }}
            h1 {{ font-size: 1.4rem; color: #1f2937; margin-top: 0; }}
            .stat {{ display: inline-block; margin-right: 20px; font-size: 0.95rem; }}
        </style>
    </head>
    <body>
        <div class="card">
            <h1>📡 MeshCore Room Server & Bridge</h1>
            <p>Stato Heltec V3: {status_badge}</p>
            <div class="stat">RX: <b>{stats['packets_rx']}</b> pacchetti</div>
            <div class="stat">TX: <b>{stats['packets_tx']}</b> pacchetti</div>
            <div class="stat">Bot Telegram: <b>@Meshcoreeliaxs_bot</b></div>
        </div>
        <div class="card">
            <h2>📜 Storico Room Server (ultimi messaggi)</h2>
            {messages_html}
        </div>
    </body>
    </html>
    """

@app.websocket("/ws/mesh")
async def websocket_mesh_endpoint(websocket: WebSocket):
    global active_heltec_ws, stats
    await websocket.accept()
    active_heltec_ws = websocket
    stats["connected_since"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stats["last_seen"] = stats["connected_since"]
    
    print("Heltec V3 connected via WebSocket!")
    await send_telegram("🟢 <b>Heltec V3 collegata con successo al server Render!</b>\nLa rete LoRa MeshCore è ora attiva e collegata a Telegram.")

    try:
        while True:
            data = await websocket.receive_bytes()
            stats["packets_rx"] += 1
            stats["last_seen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Basic parsing of MeshCore frame
            # Frames start with '>' (from radio to app)
            if len(data) >= 3 and data[0] == ord('>'):
                payload = data[3:]
                # Clean text interpretation or payload
                try:
                    text_content = payload[1:].decode("utf-8", errors="ignore").strip()
                except Exception:
                    text_content = str(payload)

                if text_content:
                    save_message("LoRa Mesh", "Nodo Radio", "Default", text_content)
                    await send_telegram(f"📻 <b>[LoRa MeshCore]</b>\n{text_content}")

    except WebSocketDisconnect:
        print("Heltec V3 disconnected.")
    except Exception as e:
        print("WebSocket exception:", e)
    finally:
        if active_heltec_ws == websocket:
            active_heltec_ws = None
        await send_telegram("🔴 <b>Heltec V3 disconnessa dal server Render.</b>\nIn attesa di riconnessione automatica...")

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
                    msg = update.get("message", {})
                    text = msg.get("text", "")
                    sender_name = msg.get("from", {}).get("first_name", "Utente")
                    chat_id = str(msg.get("chat", {}).get("id", ""))

                    if not text:
                        continue

                    if text.startswith("/start"):
                        status_str = "🟢 Connessa" if active_heltec_ws else "🔴 Non connessa (in attesa di segnale)"
                        await send_telegram(
                            f"👋 Ciao <b>{sender_name}</b>!\n\n"
                            f"Sono il tuo <b>MeshCore Room Bot</b>.\n"
                            f"Stato Heltec V3: <b>{status_str}</b>\n\n"
                            f"Comandi disponibili:\n"
                            f"• <code>/status</code>: Stato del collegamento e pacchetti\n"
                            f"• <code>/room</code>: Mostra gli ultimi messaggi salvati\n"
                            f"• Invia qualsiasi testo per trasmetterlo via radio LoRa!",
                            chat_id=chat_id
                        )
                    elif text.startswith("/status"):
                        status_str = "🟢 Connessa" if active_heltec_ws else "🔴 Non connessa"
                        await send_telegram(
                            f"📊 <b>Stato MeshCore Bridge</b>\n"
                            f"• Heltec V3: {status_str}\n"
                            f"• Pacchetti RX: {stats['packets_rx']}\n"
                            f"• Pacchetti TX: {stats['packets_tx']}\n"
                            f"• Ultimo contatto: {stats.get('last_seen', 'N/A')}",
                            chat_id=chat_id
                        )
                    elif text.startswith("/room") or text.startswith("/history"):
                        recent = get_recent_messages(10)
                        if not recent:
                            await send_telegram("📭 Nessun messaggio memorizzato nella Room al momento.", chat_id=chat_id)
                        else:
                            history_text = "📜 <b>Ultimi messaggi Room Server:</b>\n\n"
                            for m in recent:
                                history_text += f"• <i>[{m['timestamp'][11:16]}]</i> <b>{m['sender']}</b>: {m['content']}\n"
                            await send_telegram(history_text, chat_id=chat_id)
                    else:
                        # Forward plain message to MeshCore LoRa
                        save_message("Telegram", sender_name, "Default", text)
                        frame = build_meshcore_send_frame(f"[{sender_name}]: {text}")
                        sent = await send_to_heltec(frame)
                        if sent:
                            await send_telegram(f"📡 <i>Messaggio inviato via radio LoRa:</i>\n\"{text}\"", chat_id=chat_id)
                        else:
                            await send_telegram(f"💾 Salvato nella Room locale. (Heltec offline, non trasmesso via radio).", chat_id=chat_id)

        except Exception as e:
            print("Telegram polling error:", e)
            await asyncio.sleep(5)

        await asyncio.sleep(1)
