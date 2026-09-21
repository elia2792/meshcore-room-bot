import asyncio
import os
import sys
import re
import struct
import sqlite3
import time
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

# Ensure current directory is in sys.path for Render/Docker uvicorn runners
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, JSONResponse
import httpx
from web_client import get_web_client_html

# Configuration
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
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
discovered_channels: Dict[int, str] = {
    0: "Public",
    1: "Piemonte",
    2: "Italia",
    3: "Lombardia",
    4: "Veneto",
    6: "#it-pi"
}
chat_active_channel: Dict[str, int] = {}  # chat_id -> channel_idx
chat_pending_input: Dict[str, str] = {}   # chat_id -> pending setting type (for state machine)
last_telegram_sender: Dict[str, Any] = {}  # {chat_id, message_id, thread_id, timestamp}
default_channel_idx: int = 0
node_info: Dict[str, Any] = {
    "name": "Buscate",
    "firmware": "MeshCore",
    "model": "Heltec V3",
    "freq_mhz": 869.618,
    "bw_khz": 62.5,
    "sf": 8,
    "cr": 8,
    "tx_power": 20,
    "lat": None,
    "lon": None,
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
            hops INTEGER,
            ack_status TEXT DEFAULT 'pending',
            rtt_ms INTEGER DEFAULT NULL
        )
    ''')
    # Migrate columns if existing db lacks ack_status
    try:
        cursor.execute("ALTER TABLE messages ADD COLUMN ack_status TEXT DEFAULT 'pending'")
    except Exception:
        pass
    try:
        cursor.execute("ALTER TABLE messages ADD COLUMN rtt_ms INTEGER DEFAULT NULL")
    except Exception:
        pass
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

def get_recent_heard_nodes(limit: int = 50) -> List[dict]:
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

def save_message(source: str, sender: str, channel: str, content: str, snr: Optional[float] = None, hops: Optional[int] = None, ack_status: str = "confirmed", rtt_ms: Optional[int] = None) -> int:
    msg_id = 0
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO messages (source, sender, channel, content, snr, hops, ack_status, rtt_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (source, sender, channel, content, snr, hops, ack_status, rtt_ms)
        )
        msg_id = cursor.lastrowid
        conn.commit()
        conn.close()
    except Exception as e:
        print("DB save_message error:", e)
    return msg_id

def update_message_ack(msg_id: int, ack_status: str, rtt_ms: Optional[int] = None, hops: Optional[int] = None):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        if rtt_ms is not None and hops is not None:
            cursor.execute("UPDATE messages SET ack_status = ?, rtt_ms = ?, hops = ? WHERE id = ?", (ack_status, rtt_ms, hops, msg_id))
        elif rtt_ms is not None:
            cursor.execute("UPDATE messages SET ack_status = ?, rtt_ms = ? WHERE id = ?", (ack_status, rtt_ms, msg_id))
        elif hops is not None:
            cursor.execute("UPDATE messages SET ack_status = ?, hops = ? WHERE id = ?", (ack_status, hops, msg_id))
        else:
            cursor.execute("UPDATE messages SET ack_status = ? WHERE id = ?", (ack_status, msg_id))
        conn.commit()
        conn.close()
    except Exception as e:
        print("DB update_message_ack error:", e)

def get_recent_messages(limit: int = 250, channel: Optional[str] = None) -> List[dict]:
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        if channel:
            cursor.execute(
                "SELECT id, timestamp, source, sender, channel, content, snr, hops, ack_status, rtt_ms FROM messages WHERE channel = ? ORDER BY id DESC LIMIT ?",
                (channel, limit)
            )
        else:
            cursor.execute(
                "SELECT id, timestamp, source, sender, channel, content, snr, hops, ack_status, rtt_ms FROM messages ORDER BY id DESC LIMIT ?",
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

async def send_telegram(text: str, chat_id: str = TELEGRAM_CHAT_ID, reply_markup: Optional[dict] = None, message_thread_id: Optional[int] = None, reply_to_message_id: Optional[int] = None):
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    if message_thread_id:
        payload["message_thread_id"] = message_thread_id
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
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

def build_set_radio_params_frame(freq_mhz: float, bw_khz: float, sf: int, cr: int) -> bytes:
    freq_khz_x1000 = int(round(freq_mhz * 1000.0 * 1000.0))
    bw_hz = int(round(bw_khz * 1000.0))
    payload = bytes([11]) + struct.pack("<I", freq_khz_x1000) + struct.pack("<I", bw_hz) + bytes([int(sf), int(cr)])
    return b"<" + struct.pack("<H", len(payload)) + payload

def build_set_tx_power_frame(tx_power_dbm: int) -> bytes:
    payload = bytes([12, int(tx_power_dbm)])
    return b"<" + struct.pack("<H", len(payload)) + payload

def build_set_advert_name_frame(name: str) -> bytes:
    name_bytes = name.encode("utf-8")[:31] + b"\x00"
    payload = bytes([8]) + name_bytes
    return b"<" + struct.pack("<H", len(payload)) + payload

def build_set_advert_latlon_frame(lat: float, lon: float) -> bytes:
    lat_i = int(round(lat * 1000000.0))
    lon_i = int(round(lon * 1000000.0))
    payload = bytes([14]) + struct.pack("<i", lat_i) + struct.pack("<i", lon_i)
    return b"<" + struct.pack("<H", len(payload)) + payload

def build_set_channel_frame(channel_idx: int, channel_name: str, psk_bytes: bytes = None) -> bytes:
    name_buf = channel_name.encode("utf-8")[:31].ljust(32, b"\x00")
    if psk_bytes is None or len(psk_bytes) != 16:
        # Default Public PSK
        psk_bytes = bytes([0x8b, 0x33, 0x47, 0x11, 0x1d, 0x60, 0x14, 0x67, 0x96, 0x64, 0x7c, 0xa5, 0x77, 0x05, 0x5b, 0x78])
    payload = bytes([32, int(channel_idx)]) + name_buf + psk_bytes
    return b"<" + struct.pack("<H", len(payload)) + payload

def build_send_self_advert_frame(flood: bool = True) -> bytes:
    adv_type = 1 if flood else 0
    payload = bytes([7, adv_type])
    return b"<" + struct.pack("<H", len(payload)) + payload

def build_get_contacts_frame(since_ts: int = 0) -> bytes:
    payload = bytes([4]) + struct.pack("<I", int(since_ts))
    return b"<" + struct.pack("<H", len(payload)) + payload

def build_reboot_frame() -> bytes:
    payload = bytes([19]) + b"reboot"
    return b"<" + struct.pack("<H", len(payload)) + payload

def build_set_device_time_frame(epoch_secs: Optional[int] = None) -> bytes:
    if epoch_secs is None:
        epoch_secs = int(time.time())
    payload = bytes([6]) + struct.pack("<I", epoch_secs)
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
    await asyncio.sleep(0.1)
    await send_to_heltec(build_get_contacts_frame(0))
    await asyncio.sleep(0.1)
    await send_to_heltec(build_sync_next_msg_frame())

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
    buttons.append([
        {"text": "◀️ Torna al Menu Principale", "callback_data": "back_to_menu"}
    ])
    return {"inline_keyboard": buttons}

import math

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calcola la distanza in km tra due coordinate GPS (formula Haversine)."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def build_main_menu_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "📻 Canali", "callback_data": "menu_channels"},
                {"text": "👥 Nodi Ascoltati", "callback_data": "menu_heard"}
            ],
            [
                {"text": "🔍 Scan Nodi Vicini", "callback_data": "scan_nodes"},
                {"text": "📋 Report Stazione", "callback_data": "menu_report"}
            ],
            [
                {"text": "📊 Stato Live", "callback_data": "menu_status"},
                {"text": "🗺️ Mappa Live", "url": "https://livemapnew.meshcoreitalia.it/"}
            ],
            [
                {"text": "⚙️ Impostazioni", "callback_data": "menu_settings"},
                {"text": "📜 Storico Room", "callback_data": "menu_history"}
            ],
            [
                {"text": "🌐 Apri Web Client UI", "url": "https://meshcore-room-bot.onrender.com/app"}
            ]
        ]
    }

def build_settings_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "📻 Radio (Freq/SF/BW/CR)", "callback_data": "set_radio"},
                {"text": "⚡ TX Power", "callback_data": "set_tx_power"}
            ],
            [
                {"text": "🏷️ Nome Nodo", "callback_data": "set_name"},
                {"text": "📍 Posizione GPS", "callback_data": "set_gps"}
            ],
            [
                {"text": "📢 Invia Beacon Advert", "callback_data": "send_beacon"},
                {"text": "🕐 Sync Orario", "callback_data": "sync_time"}
            ],
            [
                {"text": "🔁 Riavvia Heltec ⚠️", "callback_data": "reboot_confirm"},
                {"text": "◀️ Menu Principale", "callback_data": "back_to_menu"}
            ]
        ]
    }

def build_reboot_confirm_keyboard() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "✅ Sì, riavvia ora", "callback_data": "reboot_yes"},
                {"text": "❌ Annulla", "callback_data": "menu_settings"}
            ]
        ]
    }

def build_main_menu_text(chat_id: str) -> str:
    status_str = "🟢 CONNESSA" if active_heltec_ws else "🔴 NON CONNESSA"
    active_idx = chat_active_channel.get(chat_id, default_channel_idx)
    active_name = discovered_channels.get(active_idx, f"Canale #{active_idx}")
    freq = node_info.get("freq_mhz", 868.0)
    name = node_info.get("name", "N/A")
    return (
        f"🔷 <b>MeshCore Control Panel</b>\n\n"
        f"• Heltec V3: <b>{status_str}</b>\n"
        f"• Nodo: <b>{name}</b> ({freq} MHz)\n"
        f"• Canale TX attivo: <b>[{active_idx}] {active_name}</b>\n"
        f"• Pacchetti RX: <b>{stats['packets_rx']}</b> | TX: <b>{stats['packets_tx']}</b>\n\n"
        f"<i>Seleziona un'azione:</i>"
    )

def format_node_list_rich(nodes: list, my_lat: float = None, my_lon: float = None) -> str:
    if not nodes:
        return "ℹ️ Nessun nodo rilevato nelle ultime ore."
    lines = [f"👥 <b>Nodi Radio Rilevati ({len(nodes)}):</b>\n"]
    for n in nodes:
        snr_str = f"{n['last_snr']:+.1f} dB" if n["last_snr"] is not None else "N/A"
        h = n.get("last_hops", 0)
        if h == 0 or h == 255:
            hops_str = "Diretto RF"
        else:
            hops_str = f"{h} salto" if h == 1 else f"{h} salti"

        dist_str = ""
        n_lat = n.get("lat")
        n_lon = n.get("lon")
        if n_lat and n_lon and my_lat and my_lon:
            dist = haversine_km(my_lat, my_lon, n_lat, n_lon)
            dist_str = f"📏 {int(dist * 1000)} m" if dist < 1.0 else f"📏 {dist:.1f} km"

        last_time = n.get("last_seen", "")
        last_time_str = last_time[11:16] if last_time and len(last_time) >= 16 else "N/A"

        block = (
            f"━━━━━━━━━━━━━\n"
            f"📟 <b>{n['node_name']}</b>\n"
            f"  📶 SNR: <b>{snr_str}</b> | 🔀 <b>{hops_str}</b>\n"
            f"  📦 Pkt RX: {n.get('packets_count', '?')} | 📻 {n['last_channel']}\n"
            f"  ⏱️ Ultimo: {last_time_str}"
        )
        if dist_str:
            block += f"\n  {dist_str}"
        if n_lat and n_lon:
            block += f"\n  📍 <a href='https://www.openstreetmap.org/?mlat={n_lat}&mlon={n_lon}#map=14/{n_lat}/{n_lon}'>GPS ({n_lat:.4f}, {n_lon:.4f})</a>"
        lines.append(block)
    return "\n".join(lines)

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

async def keep_alive_loop():
    """Pinga il proprio endpoint /health ogni 10 minuti per evitare il sleep di Render (piano free)."""
    await asyncio.sleep(30)  # aspetta l'avvio completo
    self_url = "https://meshcore-room-bot.onrender.com/health"
    while True:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(self_url)
                print(f"[KeepAlive] ping → {resp.status_code}")
        except Exception as e:
            print(f"[KeepAlive] errore: {e}")
        await asyncio.sleep(600)  # ogni 10 minuti

@app.on_event("startup")
async def startup_event():
    init_db()
    asyncio.create_task(telegram_polling_loop())
    asyncio.create_task(periodic_heartbeat_loop())
    asyncio.create_task(keep_alive_loop())

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
            "recent_messages": get_recent_messages(250),
            "recent_nodes": get_recent_heard_nodes(100)
        }
        await websocket.send_json(init_payload)

        while True:
            data = await websocket.receive_json()
            action = data.get("action")
            if action == "send_message":
                ch_idx = int(data.get("channel_idx", 0))
                text = str(data.get("text", "")).strip()
                sender = str(data.get("sender", "Web-Operatore")).strip() or "Web-Operatore"
                reply_to_sender = data.get("reply_to_sender")
                reply_to_text = data.get("reply_to_text")
                if text:
                    ch_name = discovered_channels.get(ch_idx, f"Canale {ch_idx}")
                    msg_id = save_message("Web Client", sender, ch_name, text, ack_status="sent_to_radio", rtt_ms=None)
                    frame = build_channel_send_frame(ch_idx, f"[{sender}]: {text}")
                    sent = await send_to_heltec(frame)

                    if reply_to_sender:
                        tg_msg = f"🌐 <b>[Web Client ➔ Canale {ch_idx}: {ch_name}]</b>\n↩️ <i>Risposta a @{reply_to_sender}</i>\n👤 <b>{sender}</b>: {text}"
                    else:
                        tg_msg = f"🌐 <b>[Web Client ➔ Canale {ch_idx}: {ch_name}]</b>\n👤 <b>{sender}</b>: {text}"
                    await broadcast_telegram(tg_msg, lora_channel_idx=ch_idx)

                    await broadcast_to_browsers({
                        "type": "new_message",
                        "id": msg_id,
                        "client_id": client_id,
                        "source": "Web Client",
                        "sender": sender,
                        "channel": ch_name,
                        "channel_idx": ch_idx,
                        "content": text,
                        "reply_to_sender": reply_to_sender,
                        "reply_to_text": reply_to_text,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "snr": None,
                        "hops": 0,
                        "sent_to_radio": sent,
                        "ack_status": "sent_to_radio" if sent else "queued",
                        "rtt_ms": None
                    })
            elif action == "refresh_channels":
                if active_heltec_ws:
                    await query_all_heltec_channels()
                    await websocket.send_json({"type": "channels", "channels": discovered_channels})
            elif action == "set_radio_params":
                freq = float(data.get("freq_mhz", node_info.get("freq_mhz", 869.618)))
                bw = float(data.get("bw_khz", node_info.get("bw_khz", 62.5)))
                sf = int(data.get("sf", node_info.get("sf", 8)))
                cr = int(data.get("cr", node_info.get("cr", 8)))
                frame = build_set_radio_params_frame(freq, bw, sf, cr)
                sent = await send_to_heltec(frame)
                if sent:
                    node_info["freq_mhz"] = freq
                    node_info["bw_khz"] = bw
                    node_info["sf"] = sf
                    node_info["cr"] = cr
                    await broadcast_to_browsers({"type": "node_info", "node_info": node_info})
                    await broadcast_telegram(f"⚙️ <b>Parametri Radio aggiornati:</b>\n{freq} MHz | BW {bw} kHz | SF{sf} CR{cr}")
                await websocket.send_json({"type": "action_result", "action": "set_radio_params", "success": sent})
            elif action == "set_radio_tx_power":
                tx_pwr = int(data.get("tx_power", 20))
                frame = build_set_tx_power_frame(tx_pwr)
                sent = await send_to_heltec(frame)
                if sent:
                    node_info["tx_power"] = tx_pwr
                    await broadcast_to_browsers({"type": "node_info", "node_info": node_info})
                    await broadcast_telegram(f"⚡ <b>Potenza TX aggiornata:</b> {tx_pwr} dBm")
                await websocket.send_json({"type": "action_result", "action": "set_radio_tx_power", "success": sent})
            elif action == "set_advert_name":
                new_name = str(data.get("name", "")).strip()
                if new_name:
                    frame = build_set_advert_name_frame(new_name)
                    sent = await send_to_heltec(frame)
                    if sent:
                        node_info["name"] = new_name
                        await broadcast_to_browsers({"type": "node_info", "node_info": node_info})
                        await broadcast_telegram(f"🏷️ <b>Nome nodo aggiornato:</b> <b>{new_name}</b>")
                    await websocket.send_json({"type": "action_result", "action": "set_advert_name", "success": sent})
            elif action == "set_advert_latlon":
                try:
                    lat = float(data.get("lat", 0.0))
                    lon = float(data.get("lon", 0.0))
                    frame = build_set_advert_latlon_frame(lat, lon)
                    sent = await send_to_heltec(frame)
                    if sent:
                        node_info["lat"] = lat
                        node_info["lon"] = lon
                        await broadcast_to_browsers({"type": "node_info", "node_info": node_info})
                        await broadcast_telegram(f"📍 <b>Posizione GPS nodo aggiornata:</b> ({lat:.5f}, {lon:.5f})")
                    await websocket.send_json({"type": "action_result", "action": "set_advert_latlon", "success": sent})
                except Exception as e:
                    await websocket.send_json({"type": "action_result", "action": "set_advert_latlon", "success": False, "error": str(e)})
            elif action == "set_channel":
                ch_idx = int(data.get("channel_idx", 0))
                ch_name = str(data.get("name", "")).strip()
                psk_hex = str(data.get("psk", "")).strip()
                psk_bytes = None
                if psk_hex:
                    try:
                        psk_bytes = bytes.fromhex(psk_hex)
                    except Exception:
                        pass
                if ch_name:
                    frame = build_set_channel_frame(ch_idx, ch_name, psk_bytes)
                    sent = await send_to_heltec(frame)
                    if sent:
                        discovered_channels[ch_idx] = ch_name
                        await broadcast_to_browsers({"type": "channels", "channels": discovered_channels})
                        await broadcast_telegram(f"📻 <b>Canale [{ch_idx}] configurato:</b> <b>{ch_name}</b>")
                    await websocket.send_json({"type": "action_result", "action": "set_channel", "success": sent, "channel_idx": ch_idx})
            elif action == "send_self_advert":
                flood = bool(data.get("flood", True))
                frame = build_send_self_advert_frame(flood=flood)
                sent = await send_to_heltec(frame)
                await broadcast_telegram(f"📢 <b>Beacon Advert trasmesso via radio in {'flood' if flood else 'zero-hop'}!</b>")
                await websocket.send_json({"type": "action_result", "action": "send_self_advert", "success": sent})
            elif action == "find_nearby_nodes":
                # Send zero-hop advert to make nearby nodes reply, then query contacts table
                frame_adv = build_send_self_advert_frame(flood=False)
                sent_adv = await send_to_heltec(frame_adv)
                await asyncio.sleep(0.1)
                frame_contacts = build_get_contacts_frame(0)
                sent_contacts = await send_to_heltec(frame_contacts)
                await broadcast_telegram("🔍 <b>Scansione nodi vicini avviata (Advert zero-hop + sync contatti)</b>")
                await websocket.send_json({
                    "type": "action_result",
                    "action": "find_nearby_nodes",
                    "success": sent_adv or sent_contacts,
                    "nodes": get_recent_heard_nodes(50)
                })
            elif action == "reboot":
                frame = build_reboot_frame()
                sent = await send_to_heltec(frame)
                await broadcast_telegram("⚠️ <b>Comando di riavvio inviato alla scheda Heltec V3!</b>")
                await websocket.send_json({"type": "action_result", "action": "reboot", "success": sent})
            elif action == "sync_time":
                frame = build_set_device_time_frame()
                sent = await send_to_heltec(frame)
                await websocket.send_json({"type": "action_result", "action": "sync_time", "success": sent})
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
async def api_nodes(limit: int = 100):
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

@app.post("/api/settings/radio")
async def api_settings_radio(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    freq = float(body.get("freq_mhz", node_info.get("freq_mhz", 869.618)))
    bw = float(body.get("bw_khz", node_info.get("bw_khz", 62.5)))
    sf = int(body.get("sf", node_info.get("sf", 8)))
    cr = int(body.get("cr", node_info.get("cr", 8)))
    tx_power = int(body.get("tx_power", node_info.get("tx_power", 20)))

    frame_radio = build_set_radio_params_frame(freq, bw, sf, cr)
    sent_radio = await send_to_heltec(frame_radio)
    frame_tx = build_set_tx_power_frame(tx_power)
    sent_tx = await send_to_heltec(frame_tx)

    if sent_radio or sent_tx:
        node_info["freq_mhz"] = freq
        node_info["bw_khz"] = bw
        node_info["sf"] = sf
        node_info["cr"] = cr
        node_info["tx_power"] = tx_power
        await broadcast_to_browsers({"type": "node_info", "node_info": node_info})
        await broadcast_telegram(f"⚙️ <b>Parametri Radio aggiornati via Web:</b>\n{freq} MHz | BW {bw} kHz | SF{sf} CR{cr} | {tx_power} dBm")

    return {"success": sent_radio and sent_tx, "node_info": node_info}

@app.post("/api/settings/node")
async def api_settings_node(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    name = str(body.get("name", "")).strip()
    lat = body.get("lat")
    lon = body.get("lon")

    sent = False
    if name:
        frame_name = build_set_advert_name_frame(name)
        sent = await send_to_heltec(frame_name)
        if sent:
            node_info["name"] = name
            await broadcast_telegram(f"🏷️ <b>Nome nodo aggiornato:</b> <b>{name}</b>")

    if lat is not None and lon is not None:
        try:
            f_lat, f_lon = float(lat), float(lon)
            frame_ll = build_set_advert_latlon_frame(f_lat, f_lon)
            sent_ll = await send_to_heltec(frame_ll)
            if sent_ll:
                node_info["lat"] = f_lat
                node_info["lon"] = f_lon
        except Exception:
            pass

    await broadcast_to_browsers({"type": "node_info", "node_info": node_info})
    return {"success": sent, "node_info": node_info}

@app.post("/api/reboot")
async def api_reboot():
    frame = build_reboot_frame()
    sent = await send_to_heltec(frame)
    if sent:
        await broadcast_telegram("⚠️ <b>Riavvio della scheda Heltec richiesto da Web!</b>")
    return {"success": sent}

@app.post("/api/advert/send")
async def api_advert_send():
    frame = build_send_self_advert_frame(flood=True)
    sent = await send_to_heltec(frame)
    if sent:
        await broadcast_telegram("📢 <b>Beacon Advert inviato via radio in flood da Web!</b>")
    return {"success": sent}

@app.post("/api/nodes/scan")
async def api_nodes_scan():
    frame_adv = build_send_self_advert_frame(flood=False)
    sent_adv = await send_to_heltec(frame_adv)
    await asyncio.sleep(0.1)
    frame_contacts = build_get_contacts_frame(0)
    sent_contacts = await send_to_heltec(frame_contacts)
    return {"success": sent_adv or sent_contacts, "nodes": get_recent_heard_nodes(50)}

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

                # RESP_CODE_CONTACT_INFO = 15 (0x0F)
                elif code == 15 and len(payload) >= 50:
                    try:
                        out_path_len = payload[35] if len(payload) > 35 else 0
                        contact_name = payload[44:76].split(b"\x00")[0].decode("utf-8", errors="ignore").strip()
                        gps_lat_raw = struct.unpack("<i", payload[80:84])[0] if len(payload) >= 84 else 0
                        gps_lon_raw = struct.unpack("<i", payload[84:88])[0] if len(payload) >= 88 else 0
                        c_lat = (gps_lat_raw / 1000000.0) if gps_lat_raw != 0 else None
                        c_lon = (gps_lon_raw / 1000000.0) if gps_lon_raw != 0 else None
                        if contact_name:
                            record_heard_node(contact_name, None, out_path_len, "Mesh Contact", c_lat, c_lon)
                            await broadcast_to_browsers({
                                "type": "nodes",
                                "nodes": get_recent_heard_nodes(30)
                            })
                    except Exception as e:
                        print("Error parsing contact info frame:", e)

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

                # PUSH_CODE_SEND_CONFIRMED = 0x82 (130)
                elif code == 0x82 and len(payload) >= 5:
                    ack_code = struct.unpack("<I", payload[1:5])[0]
                    round_trip_ms = struct.unpack("<I", payload[5:9])[0] if len(payload) >= 9 else None
                    
                    hops = None
                    if len(payload) >= 10 and payload[9] <= 15:
                        hops = payload[9]
                    elif round_trip_ms is not None:
                        # Stima del numero di salti mesh basata sull'airtime LoRa (SF8, BW62.5kHz)
                        if round_trip_ms < 750:
                            hops = 0  # Diretto RF
                        elif round_trip_ms < 1600:
                            hops = 1  # 1 ripetitore
                        elif round_trip_ms < 2800:
                            hops = 2  # 2 salti
                        else:
                            hops = 3  # 3 o più salti

                    hops_str = "Diretto RF (0 salti)" if hops == 0 else (f"{hops} salto" if hops == 1 else f"{hops} salti")
                    rtt_str = f"{round_trip_ms} ms" if round_trip_ms is not None else "N/A"
                    print(f"LoRa ACK Confirmed: code={ack_code}, RTT={rtt_str}, hops={hops_str}")

                    # Update database for the most recent outgoing message
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        cur = conn.cursor()
                        cur.execute("SELECT id FROM messages WHERE source IN ('Web Client', 'Web API', 'Telegram') ORDER BY id DESC LIMIT 1")
                        last_m = cur.fetchone()
                        if last_m:
                            update_message_ack(last_m[0], "confirmed", round_trip_ms, hops)
                        conn.close()
                    except Exception as e:
                        print("Error updating ACK:", e)

                    await broadcast_to_browsers({
                        "type": "message_ack",
                        "ack_code": ack_code,
                        "round_trip_ms": round_trip_ms,
                        "hops": hops,
                        "hops_str": hops_str,
                        "status": "confirmed"
                    })

                    # Notifica Telegram di ricezione confermata
                    ack_tg_text = (
                        f"🟢 <b>✓✓ RECAPITATO CON SUCCESSO!</b>\n"
                        f"📡 Il tuo messaggio è stato ricevuto e confermato da un nodo della rete mesh!\n"
                        f"• 📶 <b>Percorso:</b> {hops_str}\n"
                        f"• ⏱️ <b>Round-Trip:</b> {rtt_str}"
                    )
                    if last_telegram_sender and (time.time() - last_telegram_sender.get("timestamp", 0) < 120):
                        await send_telegram(
                            ack_tg_text,
                            chat_id=str(last_telegram_sender["chat_id"]),
                            reply_to_message_id=last_telegram_sender.get("message_id"),
                            message_thread_id=last_telegram_sender.get("thread_id")
                        )
                    await broadcast_telegram(ack_tg_text)

                # RESP_CODE_SENT = 6
                elif code == 6 and len(payload) >= 2:
                    route_flag = payload[1] if len(payload) >= 2 else 0
                    expected_ack = struct.unpack("<I", payload[2:6])[0] if len(payload) >= 6 else None
                    timeout_ms = struct.unpack("<I", payload[6:10])[0] if len(payload) >= 10 else 5000
                    print(f"LoRa Packet On Air: route_flag={route_flag}, expected_ack={expected_ack}, timeout={timeout_ms}ms")
                    await broadcast_to_browsers({
                        "type": "message_in_flight",
                        "expected_ack": expected_ack,
                        "status": "air"
                    })

                # RESP_CODE_NO_MORE_MESSAGES = 10
                elif code == 10:
                    pass

                # RESP_CODE_OK = 0 (Heltec radio successfully transmitted the packet)
                elif code == 0:
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        cur = conn.cursor()
                        cur.execute("SELECT id FROM messages WHERE source IN ('Web Client', 'Web API', 'Telegram') AND ack_status NOT IN ('confirmed', 'transmitted') ORDER BY id DESC LIMIT 1")
                        last_m = cur.fetchone()
                        if last_m:
                            update_message_ack(last_m[0], "transmitted")
                        conn.close()
                    except Exception as e:
                        print("Error updating transmitted status:", e)

                    await broadcast_to_browsers({
                        "type": "message_in_flight",
                        "status": "transmitted"
                    })

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

    async def ack(text: str = ""):
        try:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
                json={"callback_query_id": cq_id, "text": text}
            )
        except Exception:
            pass

    # ── Selezione canale ────────────────────────────────────────────────────
    if data.startswith("ch:"):
        ch_idx = int(data.split(":")[1])
        chat_active_channel[chat_id] = ch_idx
        ch_name = discovered_channels.get(ch_idx, f"Canale {ch_idx}")
        await ack(f"✅ Canale [{ch_idx}] {ch_name}")
        await send_telegram(
            f"✅ <b>Canale selezionato: [{ch_idx}] {ch_name}</b>\n"
            f"I tuoi messaggi verranno trasmessi su questo canale.\n\n"
            f"<i>Usa <code>#canale testo</code> per scrivere su un canale al volo.</i>",
            chat_id=chat_id,
            reply_markup=build_channels_keyboard(ch_idx)
        )

    # ── Aggiorna canali dalla Heltec ────────────────────────────────────────
    elif data == "refresh_channels":
        await ack("Aggiorno canali dalla Heltec...")
        if active_heltec_ws:
            await query_all_heltec_channels()
            await asyncio.sleep(1.0)
            active_idx = chat_active_channel.get(chat_id, default_channel_idx)
            await send_telegram("🔄 <b>Canali aggiornati dalla Heltec!</b>", chat_id=chat_id, reply_markup=build_channels_keyboard(active_idx))
        else:
            await send_telegram("⚠️ Heltec non connessa al cloud in questo momento.", chat_id=chat_id)

    # ── Menu principale ─────────────────────────────────────────────────────
    elif data == "back_to_menu":
        await ack()
        await send_telegram(build_main_menu_text(chat_id), chat_id=chat_id, reply_markup=build_main_menu_keyboard())

    # ── Canali (da menu) ────────────────────────────────────────────────────
    elif data == "menu_channels":
        await ack()
        active_idx = chat_active_channel.get(chat_id, default_channel_idx)
        ch_lines = [f"• <b>[{idx}] {name}</b> {'🟢 <b>(Attivo)</b>' if idx == active_idx else ''}" for idx, name in sorted(discovered_channels.items())]
        await send_telegram(
            f"📻 <b>Canali Heltec disponibili:</b>\n\n" + "\n".join(ch_lines) +
            f"\n\n<i>Tocca un bottone per cambiare il canale TX attivo.</i>",
            chat_id=chat_id,
            reply_markup=build_channels_keyboard(active_idx)
        )

    # ── Nodi ascoltati (da menu) ────────────────────────────────────────────
    elif data in ("menu_heard", "heard_nodes"):
        await ack()
        nodes = get_recent_heard_nodes(50)
        my_lat = node_info.get("lat")
        my_lon = node_info.get("lon")
        await send_telegram(
            format_node_list_rich(nodes, my_lat, my_lon),
            chat_id=chat_id,
            reply_markup=build_main_menu_keyboard()
        )

    # ── Report stazione ─────────────────────────────────────────────────────
    elif data == "menu_report":
        await ack()
        rep = await generate_report_text()
        await send_telegram(rep, chat_id=chat_id, reply_markup=build_main_menu_keyboard())

    # ── Stato live ──────────────────────────────────────────────────────────
    elif data == "menu_status":
        await ack()
        active_idx = chat_active_channel.get(chat_id, default_channel_idx)
        active_name = discovered_channels.get(active_idx, f"Canale #{active_idx}")
        status_str = "🟢 Connessa" if active_heltec_ws else "🔴 Non connessa"
        ch_summary = ", ".join([f"[{k}] {v}" for k, v in sorted(discovered_channels.items())])
        await send_telegram(
            f"📊 <b>Stato MeshCore Bridge</b>\n"
            f"• Heltec V3: {status_str}\n"
            f"• Nome Nodo: <b>{node_info.get('name', 'N/A')}</b>\n"
            f"• Frequenza: <b>{node_info.get('freq_mhz', 868.0)} MHz</b>\n"
            f"• BW: <b>{node_info.get('bw_khz', 62.5)} kHz</b> | SF: <b>{node_info.get('sf', 8)}</b> | CR: <b>4/{node_info.get('cr', 8)}</b>\n"
            f"• TX Power: <b>{node_info.get('tx_power', 20)} dBm</b>\n"
            f"• Canale attivo: <b>[{active_idx}] {active_name}</b>\n"
            f"• Canali noti: {ch_summary}\n"
            f"• Pacchetti RX: {stats['packets_rx']} | TX: {stats['packets_tx']}\n"
            f"• Ultimo contatto: {stats.get('last_seen', 'N/A')}",
            chat_id=chat_id,
            reply_markup=build_main_menu_keyboard()
        )

    # ── Storico messaggi ────────────────────────────────────────────────────
    elif data == "menu_history":
        await ack()
        recent = get_recent_messages(12)
        if not recent:
            await send_telegram("📭 Nessun messaggio nella Room.", chat_id=chat_id, reply_markup=build_main_menu_keyboard())
        else:
            lines = ["📜 <b>Ultimi messaggi Room Server:</b>\n"]
            for m in recent:
                snr_str = f" (SNR {m['snr']:+.1f}dB)" if m.get("snr") is not None else ""
                lines.append(f"• <i>[{m['timestamp'][11:16]}]</i> [<b>{m['channel']}</b>] <b>{m['sender']}</b>{snr_str}: {m['content']}")
            await send_telegram("\n".join(lines), chat_id=chat_id, reply_markup=build_main_menu_keyboard())

    # ── Scansione nodi vicini ───────────────────────────────────────────────
    elif data == "scan_nodes":
        await ack("🔍 Scansione avviata...")
        if not active_heltec_ws:
            await send_telegram("⚠️ Heltec non connessa. Impossibile eseguire la scansione.", chat_id=chat_id, reply_markup=build_main_menu_keyboard())
        else:
            await send_telegram("🔍 <b>Scansione nodi vicini avviata...</b>\n<i>Invio beacon zero-hop e interrogazione contatti mesh. Attendi ~5 secondi...</i>", chat_id=chat_id)
            frame_adv = build_send_self_advert_frame(flood=False)
            await send_to_heltec(frame_adv)
            await asyncio.sleep(0.2)
            frame_contacts = build_get_contacts_frame(0)
            await send_to_heltec(frame_contacts)
            await asyncio.sleep(5.0)
            nodes = get_recent_heard_nodes(50)
            my_lat = node_info.get("lat")
            my_lon = node_info.get("lon")
            result_text = (
                f"📡 <b>Risultati Scansione Nodi Vicini</b>\n\n"
                + format_node_list_rich(nodes, my_lat, my_lon)
            )
            await send_telegram(result_text, chat_id=chat_id, reply_markup=build_main_menu_keyboard())

    # ── Menu Impostazioni ───────────────────────────────────────────────────
    elif data == "menu_settings":
        await ack()
        freq = node_info.get("freq_mhz", 868.0)
        bw = node_info.get("bw_khz", 62.5)
        sf = node_info.get("sf", 8)
        cr = node_info.get("cr", 8)
        tx = node_info.get("tx_power", 20)
        name = node_info.get("name", "N/A")
        lat = node_info.get("lat", "N/A")
        lon = node_info.get("lon", "N/A")
        heltec_ok = "🟢" if active_heltec_ws else "🔴"
        await send_telegram(
            f"⚙️ <b>Impostazioni Heltec V3</b>\n\n"
            f"📻 <b>Radio:</b> {freq} MHz | BW {bw} kHz | SF{sf} CR4/{cr}\n"
            f"⚡ <b>TX Power:</b> {tx} dBm\n"
            f"🏷️ <b>Nome Nodo:</b> {name}\n"
            f"📍 <b>GPS:</b> {lat}, {lon}\n"
            f"🔌 <b>Heltec:</b> {heltec_ok} {'Connessa' if active_heltec_ws else 'Non connessa'}\n\n"
            f"<i>Seleziona un'impostazione da modificare:</i>",
            chat_id=chat_id,
            reply_markup=build_settings_keyboard()
        )

    # ── Imposta parametri radio ─────────────────────────────────────────────
    elif data == "set_radio":
        await ack()
        freq = node_info.get("freq_mhz", 868.0)
        bw = node_info.get("bw_khz", 62.5)
        sf = node_info.get("sf", 8)
        cr = node_info.get("cr", 8)
        chat_pending_input[chat_id] = "set_radio"
        await send_telegram(
            f"📻 <b>Parametri Radio Attuali:</b>\n"
            f"• Frequenza: <b>{freq} MHz</b>\n"
            f"• Bandwidth: <b>{bw} kHz</b>\n"
            f"• Spreading Factor: <b>SF{sf}</b>\n"
            f"• Coding Rate: <b>4/{cr}</b>\n\n"
            f"Invia i nuovi parametri nel formato:\n"
            f"<code>FREQ BW SF CR</code>\n\n"
            f"<b>Esempio:</b> <code>869.618 62.5 8 8</code>\n\n"
            f"<i>Oppure invia /annulla per annullare.</i>",
            chat_id=chat_id
        )

    # ── Imposta TX Power ────────────────────────────────────────────────────
    elif data == "set_tx_power":
        await ack()
        chat_pending_input[chat_id] = "set_tx_power"
        await send_telegram(
            f"⚡ <b>TX Power attuale:</b> {node_info.get('tx_power', 20)} dBm\n\n"
            f"Invia il nuovo valore in dBm (2-22):\n"
            f"<b>Esempio:</b> <code>20</code>\n\n"
            f"<i>Oppure invia /annulla per annullare.</i>",
            chat_id=chat_id
        )

    # ── Imposta nome nodo ───────────────────────────────────────────────────
    elif data == "set_name":
        await ack()
        chat_pending_input[chat_id] = "set_name"
        await send_telegram(
            f"🏷️ <b>Nome nodo attuale:</b> {node_info.get('name', 'N/A')}\n\n"
            f"Invia il nuovo nome (max 31 caratteri):\n\n"
            f"<i>Oppure invia /annulla per annullare.</i>",
            chat_id=chat_id
        )

    # ── Imposta posizione GPS ───────────────────────────────────────────────
    elif data == "set_gps":
        await ack()
        chat_pending_input[chat_id] = "set_gps"
        await send_telegram(
            f"📍 <b>GPS attuale:</b> lat={node_info.get('lat', 'N/A')}, lon={node_info.get('lon', 'N/A')}\n\n"
            f"Invia le coordinate nel formato:\n"
            f"<code>LATITUDINE LONGITUDINE</code>\n\n"
            f"<b>Esempio:</b> <code>45.5231 8.8742</code>\n\n"
            f"<i>Oppure invia /annulla per annullare.</i>",
            chat_id=chat_id
        )

    # ── Invia beacon advert ─────────────────────────────────────────────────
    elif data == "send_beacon":
        await ack("Invio beacon...")
        if active_heltec_ws:
            frame = build_send_self_advert_frame(flood=True)
            sent = await send_to_heltec(frame)
            if sent:
                await send_telegram("📢 <b>Beacon Advert trasmesso via radio (flood)!</b>\nI nodi vicini aggiorneranno la loro lista contatti.", chat_id=chat_id, reply_markup=build_settings_keyboard())
            else:
                await send_telegram("⚠️ Errore nell'invio del beacon.", chat_id=chat_id, reply_markup=build_settings_keyboard())
        else:
            await send_telegram("⚠️ Heltec non connessa.", chat_id=chat_id, reply_markup=build_settings_keyboard())

    # ── Sync orario ─────────────────────────────────────────────────────────
    elif data == "sync_time":
        await ack("Sincronizzazione orario...")
        if active_heltec_ws:
            frame = build_set_device_time_frame()
            sent = await send_to_heltec(frame)
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if sent:
                await send_telegram(f"🕐 <b>Orario sincronizzato!</b>\n<code>{ts}</code>", chat_id=chat_id, reply_markup=build_settings_keyboard())
            else:
                await send_telegram("⚠️ Errore nella sincronizzazione.", chat_id=chat_id, reply_markup=build_settings_keyboard())
        else:
            await send_telegram("⚠️ Heltec non connessa.", chat_id=chat_id, reply_markup=build_settings_keyboard())

    # ── Riavvio (conferma) ──────────────────────────────────────────────────
    elif data == "reboot_confirm":
        await ack()
        await send_telegram(
            "⚠️ <b>Conferma Riavvio Heltec V3</b>\n\nSei sicuro di voler riavviare la scheda?\nLa connessione sarà interrotta per circa 30 secondi.",
            chat_id=chat_id,
            reply_markup=build_reboot_confirm_keyboard()
        )

    # ── Riavvio (eseguito) ──────────────────────────────────────────────────
    elif data == "reboot_yes":
        await ack("Riavvio in corso...")
        if active_heltec_ws:
            frame = build_reboot_frame()
            sent = await send_to_heltec(frame)
            if sent:
                await send_telegram("🔁 <b>Comando di riavvio inviato alla Heltec V3!</b>\nLa scheda si riavvierà a breve e si riconnetterà automaticamente.", chat_id=chat_id, reply_markup=build_main_menu_keyboard())
            else:
                await send_telegram("⚠️ Errore nell'invio del comando.", chat_id=chat_id, reply_markup=build_settings_keyboard())
        else:
            await send_telegram("⚠️ Heltec non connessa.", chat_id=chat_id, reply_markup=build_settings_keyboard())

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

                    if text.startswith("/start") or text.startswith("/menu"):
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

                        greeting = f"👋 Ciao <b>{sender_name}</b>!\n\n" if text.startswith("/start") else ""
                        await send_telegram(
                            greeting + build_main_menu_text(chat_id),
                            chat_id=chat_id,
                            reply_markup=build_main_menu_keyboard(),
                            message_thread_id=thread_id
                        )

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
                        req_limit = 30
                        parts = text.split()
                        if len(parts) > 1 and parts[1].isdigit():
                            req_limit = min(int(parts[1]), 100)
                        nodes = get_recent_heard_nodes(req_limit)
                        if not nodes:
                            await send_telegram("ℹ️ Nessun nodo radio memorizzato di recente.", chat_id=chat_id, message_thread_id=thread_id)
                        else:
                            my_lat = node_info.get("lat")
                            my_lon = node_info.get("lon")
                            await send_telegram(
                                format_node_list_rich(nodes, my_lat, my_lon),
                                chat_id=chat_id,
                                message_thread_id=thread_id
                            )

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
                            reply_markup={"inline_keyboard": [[{"text": "🚀 Apri Web Client", "url": "https://meshcore-room-bot.onrender.com/app"}]]},
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
                                register_subscription(chat_id)
                                last_telegram_sender = {
                                    "chat_id": chat_id,
                                    "message_id": msg.get("message_id"),
                                    "thread_id": thread_id,
                                    "timestamp": time.time()
                                }
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
                                    "hops": 0,
                                    "sent_to_radio": sent
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

                    elif text.startswith("/annulla") or text.startswith("/cancel"):
                        # Annulla qualsiasi input pendente
                        chat_pending_input.pop(chat_id, None)
                        await send_telegram(
                            "❌ <b>Operazione annullata.</b>",
                            chat_id=chat_id,
                            reply_markup=build_main_menu_keyboard(),
                            message_thread_id=thread_id
                        )

                    else:
                        # ── Gestione input pendente (impostazioni) ──────────────────────
                        pending = chat_pending_input.get(chat_id)
                        if pending:
                            chat_pending_input.pop(chat_id, None)

                            if pending == "set_name":
                                new_name = text.strip()[:31]
                                if active_heltec_ws:
                                    frame = build_set_advert_name_frame(new_name)
                                    sent = await send_to_heltec(frame)
                                    if sent:
                                        node_info["name"] = new_name
                                        await send_telegram(f"✅ <b>Nome nodo aggiornato:</b> <b>{new_name}</b>", chat_id=chat_id, reply_markup=build_settings_keyboard(), message_thread_id=thread_id)
                                    else:
                                        await send_telegram("⚠️ Errore nell'aggiornamento del nome. Heltec disconnessa?", chat_id=chat_id, reply_markup=build_settings_keyboard())
                                else:
                                    await send_telegram("⚠️ Heltec non connessa.", chat_id=chat_id, reply_markup=build_settings_keyboard())

                            elif pending == "set_tx_power":
                                try:
                                    tx_pwr = int(text.strip())
                                    if not (2 <= tx_pwr <= 22):
                                        raise ValueError
                                    if active_heltec_ws:
                                        frame = build_set_tx_power_frame(tx_pwr)
                                        sent = await send_to_heltec(frame)
                                        if sent:
                                            node_info["tx_power"] = tx_pwr
                                            await send_telegram(f"✅ <b>TX Power aggiornato:</b> {tx_pwr} dBm", chat_id=chat_id, reply_markup=build_settings_keyboard(), message_thread_id=thread_id)
                                        else:
                                            await send_telegram("⚠️ Errore nell'aggiornamento. Heltec disconnessa?", chat_id=chat_id, reply_markup=build_settings_keyboard())
                                    else:
                                        await send_telegram("⚠️ Heltec non connessa.", chat_id=chat_id, reply_markup=build_settings_keyboard())
                                except ValueError:
                                    await send_telegram("⚠️ Valore non valido. Inserisci un numero intero tra 2 e 22.", chat_id=chat_id, reply_markup=build_settings_keyboard())

                            elif pending == "set_radio":
                                try:
                                    parts_r = text.strip().split()
                                    freq = float(parts_r[0])
                                    bw = float(parts_r[1])
                                    sf = int(parts_r[2])
                                    cr = int(parts_r[3])
                                    if active_heltec_ws:
                                        frame_r = build_set_radio_params_frame(freq, bw, sf, cr)
                                        sent = await send_to_heltec(frame_r)
                                        if sent:
                                            node_info["freq_mhz"] = freq
                                            node_info["bw_khz"] = bw
                                            node_info["sf"] = sf
                                            node_info["cr"] = cr
                                            await send_telegram(
                                                f"✅ <b>Parametri radio aggiornati!</b>\n{freq} MHz | BW {bw} kHz | SF{sf} CR4/{cr}",
                                                chat_id=chat_id, reply_markup=build_settings_keyboard(), message_thread_id=thread_id
                                            )
                                        else:
                                            await send_telegram("⚠️ Errore nell'aggiornamento. Heltec disconnessa?", chat_id=chat_id, reply_markup=build_settings_keyboard())
                                    else:
                                        await send_telegram("⚠️ Heltec non connessa.", chat_id=chat_id, reply_markup=build_settings_keyboard())
                                except (ValueError, IndexError):
                                    await send_telegram(
                                        "⚠️ Formato non valido. Usa:\n<code>FREQ BW SF CR</code>\nEsempio: <code>869.618 62.5 8 8</code>",
                                        chat_id=chat_id, reply_markup=build_settings_keyboard()
                                    )

                            elif pending == "set_gps":
                                try:
                                    parts_g = text.strip().split()
                                    lat_v = float(parts_g[0])
                                    lon_v = float(parts_g[1])
                                    if not (-90 <= lat_v <= 90 and -180 <= lon_v <= 180):
                                        raise ValueError
                                    if active_heltec_ws:
                                        frame_ll = build_set_advert_latlon_frame(lat_v, lon_v)
                                        sent = await send_to_heltec(frame_ll)
                                        if sent:
                                            node_info["lat"] = lat_v
                                            node_info["lon"] = lon_v
                                            await send_telegram(
                                                f"✅ <b>Posizione GPS aggiornata:</b>\n📍 ({lat_v:.6f}, {lon_v:.6f})",
                                                chat_id=chat_id, reply_markup=build_settings_keyboard(), message_thread_id=thread_id
                                            )
                                        else:
                                            await send_telegram("⚠️ Errore nell'aggiornamento. Heltec disconnessa?", chat_id=chat_id, reply_markup=build_settings_keyboard())
                                    else:
                                        await send_telegram("⚠️ Heltec non connessa.", chat_id=chat_id, reply_markup=build_settings_keyboard())
                                except (ValueError, IndexError):
                                    await send_telegram(
                                        "⚠️ Formato non valido. Usa:\n<code>LATITUDINE LONGITUDINE</code>\nEsempio: <code>45.5231 8.8742</code>",
                                        chat_id=chat_id, reply_markup=build_settings_keyboard()
                                    )

                            continue  # Skip radio forwarding when handling settings input

                        # ── Messaggio libero → invia alla radio ─────────────────────────
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

                        register_subscription(chat_id)
                        last_telegram_sender = {
                            "chat_id": chat_id,
                            "message_id": msg.get("message_id"),
                            "thread_id": thread_id,
                            "timestamp": time.time()
                        }
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
                            "hops": 0,
                            "sent_to_radio": sent
                        })
                        if sent:
                            await send_telegram(f"📡 <i>Trasmesso su [Canale {target_ch_idx}: {target_ch_name}] via LoRa:</i>\n\"{payload_text}\"", chat_id=chat_id, message_thread_id=thread_id)
                        else:
                            await send_telegram(f"💾 Salvato nella Room locale [{target_ch_name}]. (Heltec offline, non trasmesso via radio).", chat_id=chat_id, message_thread_id=thread_id)

        except Exception as e:
            print("Telegram polling error:", e)
            await asyncio.sleep(5)

        await asyncio.sleep(1)


