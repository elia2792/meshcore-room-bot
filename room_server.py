import os
import time
import json
import threading
import queue
import logging
from datetime import datetime
from typing import Optional, Dict, Any

logger = logging.getLogger("RoomServer")

class RoomServerBridge:
    def __init__(self, port: str = "/dev/ttyUSB0", baudrate: int = 115200):
        self.port = port
        self.baudrate = baudrate
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.ser = None
        self.cmd_queue = queue.Queue(maxsize=100)
        self.lock = threading.Lock()
        
        self.status: Dict[str, Any] = {
            "enabled": bool(port),
            "connected": False,
            "port": port,
            "role": "room_server",
            "name": "Buscate-Room",
            "freq_mhz": 869.618,
            "battery_mv": None,
            "uptime_secs": None,
            "noise_floor": None,
            "last_rssi": None,
            "last_snr": None,
            "recv_packets": 0,
            "sent_packets": 0,
            "errors": 0,
            "posts_count": 0,
            "last_post_text": None,
            "last_post_time": None,
            "last_stats_update": None
        }

    def start(self):
        if self.running or not self.port:
            return
        self.running = True
        self.thread = threading.Thread(target=self._worker_loop, daemon=True, name="RoomServerWorker")
        self.thread.start()
        logger.info(f"[RoomServer] Bridge avviato su {self.port}")

    def stop(self):
        self.running = False
        if self.ser:
            try:
                self.ser.close()
            except Exception:
                pass
        logger.info("[RoomServer] Bridge arrestato")

    def _open_serial(self) -> bool:
        if not os.path.exists(self.port):
            self.status["connected"] = False
            return False
        try:
            import serial
            self.ser = serial.Serial(self.port, self.baudrate, timeout=1.0)
            time.sleep(0.5)
            # Svuota buffer residui
            self.ser.read(self.ser.in_waiting or 1000)
            self.status["connected"] = True
            logger.info(f"[RoomServer] Seriale aperta con successo su {self.port}")
            # Sincronizza orologio iniziale
            self._send_cmd(f"time {int(time.time())}")
            self._update_stats()
            return True
        except Exception as e:
            self.status["connected"] = False
            self.ser = None
            logger.warning(f"[RoomServer] Errore apertura seriale {self.port}: {e}")
            return False

    def _send_cmd(self, cmd: str, wait_secs: float = 0.35) -> str:
        if not self.ser:
            return ""
        try:
            with self.lock:
                self.ser.write((cmd.strip() + "\r\n").encode("utf-8"))
                time.sleep(wait_secs)
                out = self.ser.read(self.ser.in_waiting or 2000).decode("utf-8", errors="ignore")
                return out.strip()
        except Exception as e:
            logger.error(f"[RoomServer] Errore invio comando '{cmd}': {e}")
            self.status["connected"] = False
            try:
                self.ser.close()
            except Exception:
                pass
            self.ser = None
            return ""

    def _update_stats(self):
        if not self.ser:
            return
        try:
            out_core = self._send_cmd("stats-core", wait_secs=0.3)
            out_radio = self._send_cmd("stats-radio", wait_secs=0.3)
            out_pkt = self._send_cmd("stats-packets", wait_secs=0.3)

            merged = {}
            for out in [out_core, out_radio, out_pkt]:
                for line in out.splitlines():
                    if "{" in line and "}" in line:
                        try:
                            s = line[line.find("{"):line.rfind("}") + 1]
                            merged.update(json.loads(s))
                        except Exception:
                            pass

            if merged:
                if "battery_mv" in merged:
                    self.status["battery_mv"] = merged["battery_mv"]
                if "uptime_secs" in merged:
                    self.status["uptime_secs"] = merged["uptime_secs"]
                if "errors" in merged:
                    self.status["errors"] = merged["errors"]
                if "noise_floor" in merged:
                    self.status["noise_floor"] = merged["noise_floor"]
                if "last_rssi" in merged:
                    self.status["last_rssi"] = merged["last_rssi"]
                if "last_snr" in merged:
                    self.status["last_snr"] = merged["last_snr"]
                if "recv" in merged:
                    self.status["recv_packets"] = merged["recv"]
                if "sent" in merged:
                    self.status["sent_packets"] = merged["sent"]
                self.status["last_stats_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logger.error(f"[RoomServer] Errore lettura statistiche: {e}")

    def _worker_loop(self):
        last_stats_time = 0
        last_clock_sync_time = time.time()

        while self.running:
            if not self.ser or not self.status["connected"]:
                if not self._open_serial():
                    time.sleep(10.0)
                    continue

            # Processa comandi o messaggi in coda
            try:
                try:
                    action, payload = self.cmd_queue.get(timeout=2.0)
                    if action == "post":
                        clean_text = payload.replace("\r", " ").replace("\n", " ").strip()
                        if len(clean_text) > 110:
                            clean_text = clean_text[:107] + "..."
                        resp = self._send_cmd(f"room.post {clean_text}", wait_secs=0.5)
                        if "OK" in resp:
                            self.status["posts_count"] += 1
                            self.status["last_post_text"] = clean_text
                            self.status["last_post_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            logger.info(f"[RoomServer] Messaggio archiviato in bacheca: {clean_text}")
                        else:
                            logger.warning(f"[RoomServer] Risposta anomala da room.post: {resp}")
                    elif action == "cmd":
                        self._send_cmd(payload, wait_secs=0.4)
                    elif action == "sync_time":
                        self._send_cmd(f"time {int(time.time())}")
                    elif action == "advert":
                        self._send_cmd("advert.zerohop")
                    self.cmd_queue.task_done()
                except queue.Empty:
                    pass

                now = time.time()
                # Sincronizza orologio ogni ora
                if now - last_clock_sync_time > 3600:
                    last_clock_sync_time = now
                    self._send_cmd(f"time {int(now)}")

                # Aggiorna statistiche ogni 45 secondi
                if now - last_stats_time > 45:
                    last_stats_time = now
                    self._update_stats()

            except Exception as e:
                logger.error(f"[RoomServer] Errore worker loop: {e}")
                time.sleep(2.0)

    def post_message(self, sender: str, channel: str, text: str):
        if not self.running or not self.port:
            return
        if not text or not text.strip():
            return
        
        # Filtra messaggi di servizio interni se già originati dal Room Server
        if sender and "Buscate-Room" in sender:
            return

        ch_tag = f" #{channel}" if channel and channel.lower() not in ("public", "radio", "direct", "mesh") else ""
        formatted = f"[{sender}{ch_tag}]: {text.strip()}"
        try:
            self.cmd_queue.put_nowait(("post", formatted))
        except queue.Full:
            logger.warning("[RoomServer] Coda messaggi piena, scarto post")

    def trigger_advert(self, zerohop: bool = True):
        cmd = "advert.zerohop" if zerohop else "advert"
        try:
            self.cmd_queue.put_nowait(("cmd", cmd))
        except Exception:
            pass

    def sync_time(self):
        try:
            self.cmd_queue.put_nowait(("sync_time", None))
        except Exception:
            pass

    def get_status(self) -> Dict[str, Any]:
        return dict(self.status)

# Istanza singleton globale
room_server_bridge = RoomServerBridge(
    port=os.getenv("ROOM_SERVER_SERIAL_PORT", "/dev/ttyUSB0")
)
