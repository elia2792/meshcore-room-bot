# 📡 MeshCore Web Station & Telegram Bot 🤖

Una suite completa e pronta all'uso per gestire una **Stazione Radio LoRa MeshCore**, dotata di:
* 🌐 **Web App reattiva (PWA)**: Chat LoRa multicanale in tempo reale, lista nodi ascoltati, mappa GPS, radar nodi diretti RF e statistiche di propagazione 24h.
* ✈️ **Bot Telegram bidirezionale**: Ricevi notifiche sull'etere e rispondi sui canali radio LoRa direttamente da Telegram.
* 🤖 **Auto-Responder intelligente**: Risposte automatiche a `ping`, `test`, `echo`, `snr`, `status`, `path` e previsioni `meteo`.
* 📬 **Mailbox LoRa (Store-and-Forward)**: Lascia messaggi differiti per nodi offline con consegna automatica appena vengono ascoltati via radio.
* 🏛️ **Ponte Room Server**: Integrazione opzionale per secondo Heltec dedicato come Room Server / bacheca messaggi persistente.

---

## 🛠️ Prerequisiti Hardware & Software

1. **Scheda Radio LoRa compatibile con MeshCore**:
   * Heltec WiFi LoRa 32 V3 (ESP32-S3), Heltec V2, Wireless Tracker, T-Beam, T-Echo, o qualsiasi board supportata da MeshCore.
   * Con a bordo il firmware **MeshCore Companion** (flashabile in un click da [flasher.meshcore.io](https://flasher.meshcore.io)).
2. **Computer o Single Board Computer**:
   * Raspberry Pi (qualsiasi modello), Mini PC, server Linux, macOS o PC Windows con una porta USB o connessione di rete locale.
3. **Python**: versione 3.9 o superiore.

---

## 🚀 Installazione Rapida in 4 Passaggi

### 1. Clona il repository
```bash
git clone https://github.com/elia2792/meshcore-room-bot.git
cd meshcore-room-bot
```

### 2. Crea un ambiente virtuale Python e installa le dipendenze
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configura le impostazioni (`.env`)
Copia il file di esempio e configuralo per la tua radio:
```bash
cp .env.example .env
nano .env
```

**Esempio di configurazione via cavo USB (Consigliata):**
```ini
# Porta USB assegnata alla tua radio (su Linux / Raspberry di solito /dev/ttyUSB0):
HELTEC_SERIAL_PORT=/dev/ttyUSB0
PORT=8000

# Facoltativo: Token Bot Telegram per ricevere e inviare messaggi LoRa da remoto
TELEGRAM_BOT_TOKEN=il_tuo_token_da_botfather
TELEGRAM_CHAT_ID=il_tuo_chat_id
```

*(Se invece colleghi la tua radio via WiFi/TCP, lascia `HELTEC_SERIAL_PORT=` vuoto e inserisci `HELTEC_TCP_HOST=192.168.x.x` e `HELTEC_TCP_PORT=5000`)*.

### 4. Avvia l'applicazione
```bash
python3 -m uvicorn app:app --host 0.0.0.0 --port 8000
```
Ora apri il browser all'indirizzo:
👉 **`http://localhost:8000/app`** (oppure l'IP locale del tuo Raspberry Pi, es. `http://192.168.1.50:8000/app`).

---

## 📻 Comandi Radio LoRa Riconosciuti dal Bot

Chiunque trasmetta via radio su un canale ascoltato dalla tua stazione può usare questi comandi:

| Comando | Descrizione | Risposta Bot |
| :--- | :--- | :--- |
| `ping`, `test`, `echo`, `prova` | Test di portata e ricezione | Risponde con SNR, salti RF e distanza chilometrica |
| `snr`, `!snr` | Verifica qualità del segnale | Restituisce il valore SNR esatto in dB |
| `path`, `!path`, `percorso` | Verifica instradamento pacchetto | Mostra se il segnale è diretto (0 salti) o tramite ripetitori mesh |
| `status`, `stazione` | Stato del nodo ricevente | Frequenza operativa, modello e parametri RF |
| `meteo [città]` | Bollettino meteo in tempo reale | Previsione meteo, temperatura, umidità e vento |
| `!msg @[Nodo] testo` | Salva messaggio differito (Mailbox) | Registra il messaggio e lo consegna appena il nodo trasmette |

### 🛡️ Comandi Amministratore via Radio (Richiedono nodo in `ADMIN_LORA_NODES` o PIN in `.env`)
| Comando | Descrizione |
| :--- | :--- |
| `!admin stats [pin]` o `!stats [pin]` | Report amministrativo: Uptime bot, RX/TX, nodi censiti nel DB e statistiche rate limit del canale |
| `!admin advert [pin]` o `!advert [pin]` | Forza l'invio immediato del pacchetto Flood Beacon Advert su tutta la rete mesh |
| `!reboot [pin]` | Riavvia via hardware la scheda Heltec Companion Radio |

### 🛑 Protezione Airtime & Anti-Loop (Best-practice MeshCore)
* **Silent Rejection**: Qualsiasi comando non autorizzato, non valido o rifiutato per cooldown/limiti **non genera alcuna trasmissione RF**, azzerando il consumo di banda radio.
* **Rate-Limiter a livello di Canale**: Massimo 5 risposte automatiche per finestra di 5 minuti per singolo canale (`CHANNEL_RATE_LIMIT_MAX` e `CHANNEL_RATE_LIMIT_WINDOW`), evitando il flood del canale.
* **Filtro Anti-Loop Bot/Repeater**: Rilevamento automatico di nodi bot, server e ripetitori e filtraggio delle risposte tipiche (`[bot]`, `pong!`, `ack da`, `snr:`, ecc.) per prevenire loop infiniti ping-pong radio.

---

## ✈️ Comandi Telegram

* `/status`: Stato della stazione, frequenza, canali attivi e pacchetti RX/TX.
* `/nodi`: Lista degli ultimi nodi ascoltati via radio con SNR e salti.
* `/diretti`: Scansione nodi RF diretti (0 salti) senza rimbalzi.
* `/path`: Storico dei percorsi degli ultimi messaggi ricevuti.
* `/room <testo>`: Pubblica un annuncio in bacheca sul Room Server.
* `/room_status`: Stato hardware, batteria e telemetria del Room Server.
* Invia qualsiasi testo per trasmetterlo direttamente sul canale radio attivo!

---

## 🔄 Avvio Automatico con Systemd (Linux / Raspberry Pi)

Per far partire il servizio automaticamente all'accensione del Raspberry Pi:
```bash
sudo nano /etc/systemd/system/meshcore-bot.service
```

Incolla la seguente configurazione:
```ini
[Unit]
Description=MeshCore Station & Telegram Bot Service
After=network.target

[Service]
User=pi
WorkingDirectory=/home/pi/meshcore-room-bot
ExecStart=/home/pi/meshcore-room-bot/venv/bin/python -m uvicorn app:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Attiva e avvia il servizio:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now meshcore-bot
```

---

## 📜 Licenza
Progetto open source rilasciato sotto licenza MIT.
