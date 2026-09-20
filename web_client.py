"""
MeshCore Web Client - Standalone HTML5 Single Page Application
Provides full real-time channel switching, message streaming, LoRa transmission,
heard nodes list, telemetry dashboard, and audio chimes via WebSockets and REST API.
"""

def get_web_client_html() -> str:
    return """<!DOCTYPE html>
<html lang="it">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>MeshCore Web Client</title>
    <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>📡</text></svg>">
    <style>
        :root {
            --bg: #0b0f19;
            --surface: #111827;
            --surface-hover: #1f293d;
            --border: #1f2937;
            --border-highlight: #374151;
            --text-main: #f9fafb;
            --text-muted: #9ca3af;
            --text-dim: #6b7280;
            --primary: #10b981;
            --primary-dark: #059669;
            --accent-blue: #38bdf8;
            --accent-purple: #a855f7;
            --accent-amber: #f59e0b;
            --danger: #ef4444;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
            -webkit-tap-highlight-color: transparent;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            background-color: var(--bg);
            color: var(--text-main);
            height: 100vh;
            height: 100dvh;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }

        /* Header */
        header {
            background: var(--surface);
            border-bottom: 1px solid var(--border);
            padding: 10px 16px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
            flex-shrink: 0;
            z-index: 20;
        }

        .header-brand {
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .logo-icon {
            font-size: 1.5rem;
            line-height: 1;
        }

        .brand-title {
            font-size: 1.05rem;
            font-weight: 700;
            letter-spacing: -0.02em;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .badge-status {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 3px 8px;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .badge-status.online {
            background: rgba(16, 185, 129, 0.15);
            color: #34d399;
            border: 1px solid rgba(16, 185, 129, 0.3);
        }

        .badge-status.offline {
            background: rgba(239, 68, 68, 0.15);
            color: #f87171;
            border: 1px solid rgba(239, 68, 68, 0.3);
        }

        .pulse-dot {
            width: 7px;
            height: 7px;
            border-radius: 50%;
            background: currentColor;
            display: inline-block;
        }

        .badge-status.online .pulse-dot {
            box-shadow: 0 0 8px #10b981;
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.4; transform: scale(0.85); }
        }

        .header-actions {
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .icon-btn {
            background: var(--surface-hover);
            border: 1px solid var(--border);
            color: var(--text-main);
            padding: 6px 10px;
            border-radius: 8px;
            font-size: 0.85rem;
            font-weight: 500;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            gap: 6px;
            transition: all 0.15s ease;
            text-decoration: none;
        }

        .icon-btn:hover {
            border-color: var(--border-highlight);
            background: #283548;
        }

        .icon-btn.active {
            background: rgba(56, 189, 248, 0.15);
            border-color: rgba(56, 189, 248, 0.4);
            color: var(--accent-blue);
        }

        /* Mobile View Switcher */
        .mobile-tabs {
            display: none;
            background: var(--surface);
            border-bottom: 1px solid var(--border);
            padding: 6px 12px;
            gap: 8px;
            flex-shrink: 0;
        }

        .mobile-tab-btn {
            flex: 1;
            padding: 6px;
            border-radius: 6px;
            background: transparent;
            border: none;
            color: var(--text-muted);
            font-size: 0.85rem;
            font-weight: 600;
            cursor: pointer;
            text-align: center;
        }

        .mobile-tab-btn.active {
            background: var(--surface-hover);
            color: var(--text-main);
        }

        /* Channels Bar */
        .channels-bar {
            background: #0d1322;
            border-bottom: 1px solid var(--border);
            padding: 8px 16px;
            display: flex;
            align-items: center;
            gap: 8px;
            overflow-x: auto;
            scrollbar-width: none;
            flex-shrink: 0;
        }

        .channels-bar::-webkit-scrollbar {
            display: none;
        }

        .ch-label {
            font-size: 0.75rem;
            color: var(--text-dim);
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-right: 4px;
            white-space: nowrap;
        }

        .ch-chip {
            background: var(--surface);
            border: 1px solid var(--border);
            color: var(--text-muted);
            padding: 5px 12px;
            border-radius: 9999px;
            font-size: 0.82rem;
            font-weight: 500;
            cursor: pointer;
            white-space: nowrap;
            display: inline-flex;
            align-items: center;
            gap: 6px;
            transition: all 0.15s ease;
        }

        .ch-chip:hover {
            border-color: var(--border-highlight);
            color: var(--text-main);
        }

        .ch-chip.active {
            background: rgba(16, 185, 129, 0.2);
            border-color: var(--primary);
            color: #34d399;
            font-weight: 600;
        }

        .ch-count-badge {
            background: rgba(255, 255, 255, 0.1);
            font-size: 0.7rem;
            padding: 1px 5px;
            border-radius: 10px;
        }

        /* Main Container */
        .main-container {
            display: flex;
            flex: 1;
            overflow: hidden;
            position: relative;
        }

        /* Chat Section */
        .chat-section {
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
            background: var(--bg);
            border-right: 1px solid var(--border);
        }

        .messages-container {
            flex: 1;
            overflow-y: auto;
            padding: 16px;
            display: flex;
            flex-direction: column;
            gap: 12px;
            scroll-behavior: smooth;
        }

        .message-bubble {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 12px 14px;
            max-width: 90%;
            align-self: flex-start;
            position: relative;
            box-shadow: 0 2px 6px rgba(0, 0, 0, 0.25);
            transition: border-color 0.2s;
        }

        .message-bubble.from-lora {
            border-left: 4px solid var(--primary);
        }

        .message-bubble.from-telegram {
            border-left: 4px solid var(--accent-blue);
        }

        .message-bubble.from-web {
            border-left: 4px solid var(--accent-purple);
            align-self: flex-end;
            background: #141c2e;
        }

        .msg-header {
            display: flex;
            align-items: center;
            gap: 8px;
            margin-bottom: 6px;
            flex-wrap: wrap;
        }

        .msg-sender {
            font-weight: 700;
            font-size: 0.9rem;
            color: var(--text-main);
        }

        .msg-ch-tag {
            font-size: 0.72rem;
            padding: 2px 7px;
            border-radius: 4px;
            font-weight: 600;
            background: #1f293d;
            color: var(--accent-blue);
        }

        .msg-source-tag {
            font-size: 0.7rem;
            color: var(--text-dim);
        }

        .msg-time {
            font-size: 0.72rem;
            color: var(--text-dim);
            margin-left: auto;
        }

        .msg-body {
            font-size: 0.95rem;
            line-height: 1.45;
            color: #e5e7eb;
            word-break: break-word;
        }

        .msg-footer {
            margin-top: 8px;
            padding-top: 6px;
            border-top: 1px solid rgba(255, 255, 255, 0.06);
            display: flex;
            align-items: center;
            gap: 10px;
            font-size: 0.75rem;
            color: var(--text-muted);
            flex-wrap: wrap;
        }

        .signal-pill {
            display: inline-flex;
            align-items: center;
            gap: 4px;
        }

        .signal-pill.good { color: #34d399; }
        .signal-pill.fair { color: #fbbf24; }
        .signal-pill.poor { color: #fb923c; }

        .map-btn {
            color: var(--accent-blue);
            text-decoration: none;
            display: inline-flex;
            align-items: center;
            gap: 4px;
            font-weight: 500;
        }

        .map-btn:hover {
            text-decoration: underline;
        }

        /* Composer */
        .composer {
            background: var(--surface);
            border-top: 1px solid var(--border);
            padding: 12px 16px;
            display: flex;
            flex-direction: column;
            gap: 10px;
            flex-shrink: 0;
        }

        .composer-top {
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .input-callsign {
            background: var(--bg);
            border: 1px solid var(--border);
            color: var(--text-main);
            padding: 6px 10px;
            border-radius: 8px;
            font-size: 0.82rem;
            font-weight: 600;
            width: 140px;
        }

        .select-channel {
            background: var(--bg);
            border: 1px solid var(--border);
            color: var(--accent-blue);
            padding: 6px 10px;
            border-radius: 8px;
            font-size: 0.82rem;
            font-weight: 600;
            flex: 1;
            cursor: pointer;
        }

        .composer-bottom {
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .input-message {
            flex: 1;
            background: var(--bg);
            border: 1px solid var(--border);
            color: var(--text-main);
            padding: 10px 14px;
            border-radius: 10px;
            font-size: 0.95rem;
            outline: none;
            transition: border-color 0.15s;
        }

        .input-message:focus {
            border-color: var(--primary);
        }

        .btn-send {
            background: var(--primary);
            color: #052e16;
            border: none;
            padding: 10px 18px;
            border-radius: 10px;
            font-size: 0.95rem;
            font-weight: 700;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            gap: 6px;
            transition: all 0.15s;
            white-space: nowrap;
        }

        .btn-send:hover {
            background: var(--primary-dark);
            color: #fff;
        }

        .btn-send:active {
            transform: scale(0.97);
        }

        /* Sidebar */
        .sidebar {
            width: 340px;
            background: var(--surface);
            display: flex;
            flex-direction: column;
            overflow-y: auto;
            flex-shrink: 0;
        }

        .sidebar-panel {
            padding: 16px;
            border-bottom: 1px solid var(--border);
        }

        .sidebar-title {
            font-size: 0.82rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            color: var(--text-dim);
            font-weight: 700;
            margin-bottom: 12px;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .node-list {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }

        .node-card {
            background: var(--bg);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 8px 10px;
            display: flex;
            flex-direction: column;
            gap: 4px;
        }

        .node-card-top {
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .node-name {
            font-weight: 700;
            font-size: 0.88rem;
            color: var(--text-main);
        }

        .node-packets {
            font-size: 0.7rem;
            background: rgba(255, 255, 255, 0.08);
            padding: 1px 6px;
            border-radius: 10px;
            color: var(--text-muted);
        }

        .node-card-meta {
            font-size: 0.74rem;
            color: var(--text-muted);
            display: flex;
            align-items: center;
            gap: 8px;
            flex-wrap: wrap;
        }

        .station-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px;
        }

        .station-stat {
            background: var(--bg);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 8px 10px;
        }

        .stat-label {
            font-size: 0.7rem;
            color: var(--text-dim);
            text-transform: uppercase;
        }

        .stat-value {
            font-size: 1.05rem;
            font-weight: 700;
            color: var(--text-main);
            margin-top: 2px;
        }

        .empty-state {
            text-align: center;
            color: var(--text-dim);
            font-size: 0.85rem;
            padding: 20px 10px;
        }

        /* Responsive */
        @media (max-width: 768px) {
            .mobile-tabs {
                display: flex;
            }

            .sidebar {
                display: none;
                width: 100%;
            }

            .sidebar.mobile-visible {
                display: flex;
            }

            .chat-section.mobile-hidden {
                display: none;
            }

            .input-callsign {
                width: 110px;
            }
        }
    </style>
</head>
<body>

    <!-- Header -->
    <header>
        <div class="header-brand">
            <span class="logo-icon">📡</span>
            <div>
                <div class="brand-title">
                    MeshCore Web
                    <span id="boardStatusBadge" class="badge-status offline">
                        <span class="pulse-dot"></span>
                        <span id="boardStatusText">Heltec Offline</span>
                    </span>
                </div>
            </div>
        </div>

        <div class="header-actions">
            <button id="soundToggleBtn" class="icon-btn" title="Notifiche Audio">
                <span id="soundIcon">🔊</span>
                <span style="display:none;" id="soundLabel">Audio</span>
            </button>
            <button id="refreshChannelsBtn" class="icon-btn" title="Aggiorna Canali Radio">
                🔄
            </button>
            <a href="https://t.me/Meshcoreeliaxs_bot" target="_blank" class="icon-btn" title="Apri Bot Telegram">
                ✈️ Bot
            </a>
        </div>
    </header>

    <!-- Mobile Navigation Tabs -->
    <div class="mobile-tabs">
        <button id="tabChatBtn" class="mobile-tab-btn active">💬 Canali & Chat</button>
        <button id="tabNodesBtn" class="mobile-tab-btn">👥 Nodi & Telemetria</button>
    </div>

    <!-- Channels Selector Bar -->
    <div class="channels-bar" id="channelsBar">
        <span class="ch-label">Canale:</span>
        <button class="ch-chip active" data-ch="all">🌟 Tutti</button>
    </div>

    <!-- Main Container -->
    <div class="main-container">
        
        <!-- Chat Section -->
        <section class="chat-section" id="chatSection">
            <div class="messages-container" id="messagesContainer">
                <div class="empty-state" id="loadingState">Connessione in corso al server MeshCore...</div>
            </div>

            <!-- Composer -->
            <div class="composer">
                <div class="composer-top">
                    <input type="text" id="callsignInput" class="input-callsign" placeholder="Tuo Nome" value="Web-Operatore" title="Tuo nominativo o nome operatore">
                    <select id="channelSelect" class="select-channel" title="Seleziona il canale su cui trasmettere">
                        <option value="0">[0] Public</option>
                    </select>
                </div>
                <div class="composer-bottom">
                    <input type="text" id="messageInput" class="input-message" placeholder="Scrivi un messaggio da inviare via radio..." autocomplete="off">
                    <button id="sendBtn" class="btn-send">
                        <span>📡</span>
                        <span>Invia</span>
                    </button>
                </div>
            </div>
        </section>

        <!-- Sidebar / Telemetry -->
        <aside class="sidebar" id="sidebarSection">
            <!-- Station Telemetry -->
            <div class="sidebar-panel">
                <div class="sidebar-title">
                    <span>📻 Stazione Heltec V3</span>
                    <span id="nodeNameTag" style="color:var(--primary); font-weight:bold;">Buscate</span>
                </div>
                <div class="station-grid">
                    <div class="station-stat">
                        <div class="stat-label">Frequenza</div>
                        <div class="stat-value" id="statFreq">869.618 MHz</div>
                    </div>
                    <div class="station-stat">
                        <div class="stat-label">Modulazione</div>
                        <div class="stat-value" id="statMod">SF8 / BW62.5</div>
                    </div>
                    <div class="station-stat">
                        <div class="stat-label">Pacchetti RX</div>
                        <div class="stat-value" id="statRx" style="color:#34d399;">0</div>
                    </div>
                    <div class="station-stat">
                        <div class="stat-label">Pacchetti TX</div>
                        <div class="stat-value" id="statTx" style="color:#38bdf8;">0</div>
                    </div>
                </div>
            </div>

            <!-- Heard Nodes -->
            <div class="sidebar-panel" style="flex: 1;">
                <div class="sidebar-title">
                    <span>👥 Nodi Ascoltati via Radio</span>
                    <span id="nodesCountBadge" style="background:#1f293d; padding:2px 7px; border-radius:10px; font-size:0.75rem;">0</span>
                </div>
                <div class="node-list" id="nodesList">
                    <div class="empty-state">In attesa di traffico radio...</div>
                </div>
            </div>
        </aside>

    </div>

    <!-- Client-side Application Script -->
    <script>
        // State
        let socket = null;
        let channels = {0: "Public", 1: "Piemonte", 2: "Italia", 3: "Lombardia", 4: "Veneto", 6: "#it-pi"};
        let currentFilter = "all";
        let activeSendChannel = 0;
        let messages = [];
        let heardNodes = [];
        let soundEnabled = true;
        let isHeltecConnected = false;

        // Restore callsign and sound preferences
        const savedCallsign = localStorage.getItem("meshcore_callsign");
        if (savedCallsign) {
            document.getElementById("callsignInput").value = savedCallsign;
        }
        document.getElementById("callsignInput").addEventListener("change", (e) => {
            localStorage.setItem("meshcore_callsign", e.target.value.trim() || "Web-Operatore");
        });

        const savedSound = localStorage.getItem("meshcore_sound");
        if (savedSound !== null) {
            soundEnabled = savedSound === "true";
            updateSoundIcon();
        }

        // Web Audio Synthesizer for LoRa packet chime
        function playChime() {
            if (!soundEnabled) return;
            try {
                const AudioCtx = window.AudioContext || window.webkitAudioContext;
                if (!AudioCtx) return;
                const ctx = new AudioCtx();
                const now = ctx.currentTime;
                
                // Tone 1
                const osc1 = ctx.createOscillator();
                const gain1 = ctx.createGain();
                osc1.type = "sine";
                osc1.frequency.setValueAtTime(880, now);
                osc1.frequency.exponentialRampToValueAtTime(1320, now + 0.08);
                gain1.gain.setValueAtTime(0.08, now);
                gain1.gain.exponentialRampToValueAtTime(0.001, now + 0.12);
                osc1.connect(gain1);
                gain1.connect(ctx.destination);
                osc1.start(now);
                osc1.stop(now + 0.12);

                // Tone 2
                const osc2 = ctx.createOscillator();
                const gain2 = ctx.createGain();
                osc2.type = "sine";
                osc2.frequency.setValueAtTime(1760, now + 0.08);
                gain2.gain.setValueAtTime(0.06, now + 0.08);
                gain2.gain.exponentialRampToValueAtTime(0.001, now + 0.22);
                osc2.connect(gain2);
                gain2.connect(ctx.destination);
                osc2.start(now + 0.08);
                osc2.stop(now + 0.22);
            } catch(e) {
                console.log("Audio play error:", e);
            }
        }

        function updateSoundIcon() {
            document.getElementById("soundIcon").innerText = soundEnabled ? "🔊" : "🔇";
        }

        document.getElementById("soundToggleBtn").addEventListener("click", () => {
            soundEnabled = !soundEnabled;
            localStorage.setItem("meshcore_sound", soundEnabled);
            updateSoundIcon();
            if (soundEnabled) playChime();
        });

        // Mobile Tabs Switcher
        const tabChatBtn = document.getElementById("tabChatBtn");
        const tabNodesBtn = document.getElementById("tabNodesBtn");
        const chatSection = document.getElementById("chatSection");
        const sidebarSection = document.getElementById("sidebarSection");

        tabChatBtn.addEventListener("click", () => {
            tabChatBtn.classList.add("active");
            tabNodesBtn.classList.remove("active");
            chatSection.classList.remove("mobile-hidden");
            sidebarSection.classList.remove("mobile-visible");
        });

        tabNodesBtn.addEventListener("click", () => {
            tabNodesBtn.classList.add("active");
            tabChatBtn.classList.remove("active");
            chatSection.classList.add("mobile-hidden");
            sidebarSection.classList.add("mobile-visible");
        });

        // Channel tabs rendering
        function renderChannelsBar() {
            const bar = document.getElementById("channelsBar");
            const select = document.getElementById("channelSelect");
            
            // Retain 'all' chip
            bar.innerHTML = '<span class="ch-label">Canale:</span>';
            
            const allBtn = document.createElement("button");
            allBtn.className = "ch-chip" + (currentFilter === "all" ? " active" : "");
            allBtn.innerText = "🌟 Tutti";
            allBtn.addEventListener("click", () => selectFilter("all"));
            bar.appendChild(allBtn);

            select.innerHTML = "";

            const sortedKeys = Object.keys(channels).map(Number).sort((a, b) => a - b);
            sortedKeys.forEach(idx => {
                const name = channels[idx];
                
                // Chip in top bar
                const chip = document.createElement("button");
                chip.className = "ch-chip" + (currentFilter === String(idx) ? " active" : "");
                chip.innerText = `[${idx}] ${name}`;
                chip.addEventListener("click", () => selectFilter(String(idx)));
                bar.appendChild(chip);

                // Option in composer select
                const opt = document.createElement("option");
                opt.value = idx;
                opt.innerText = `[${idx}] ${name}`;
                if (idx === activeSendChannel) opt.selected = true;
                select.appendChild(opt);
            });
        }

        function selectFilter(chKey) {
            currentFilter = chKey;
            renderChannelsBar();
            renderMessages();
            if (chKey !== "all") {
                activeSendChannel = parseInt(chKey);
                document.getElementById("channelSelect").value = activeSendChannel;
            }
        }

        document.getElementById("channelSelect").addEventListener("change", (e) => {
            activeSendChannel = parseInt(e.target.value);
        });

        // Messages rendering
        function renderMessages() {
            const container = document.getElementById("messagesContainer");
            const filtered = currentFilter === "all" 
                ? messages 
                : messages.filter(m => {
                    const targetName = channels[parseInt(currentFilter)];
                    return m.channel === targetName || String(m.channel_idx) === String(currentFilter);
                });

            if (filtered.length === 0) {
                container.innerHTML = '<div class="empty-state">Nessun messaggio in questo canale al momento.</div>';
                return;
            }

            container.innerHTML = "";
            filtered.forEach(m => {
                const bubble = document.createElement("div");
                let sourceClass = "from-lora";
                let sourceIcon = "📡 LoRa";
                if (m.source === "Telegram") {
                    sourceClass = "from-telegram";
                    sourceIcon = "✈️ Telegram";
                } else if (m.source === "Web Client" || m.source === "Web API") {
                    sourceClass = "from-web";
                    sourceIcon = "🌐 Web";
                }

                bubble.className = `message-bubble ${sourceClass}`;

                // Header
                const timeStr = m.timestamp ? (m.timestamp.length > 16 ? m.timestamp.substring(11, 19) : m.timestamp) : "";
                let headerHtml = `
                    <div class="msg-header">
                        <span class="msg-sender">${escapeHtml(m.sender || "Nodo Radio")}</span>
                        <span class="msg-ch-tag">[${escapeHtml(m.channel || "Public")}]</span>
                        <span class="msg-source-tag">${sourceIcon}</span>
                        <span class="msg-time">${timeStr}</span>
                    </div>
                `;

                // Content
                const contentHtml = `<div class="msg-body">${escapeHtml(m.content || "")}</div>`;

                // Footer (metrics & GPS)
                let footerHtml = "";
                let metricsParts = [];
                if (m.snr !== undefined && m.snr !== null) {
                    const snrVal = parseFloat(m.snr);
                    let snrClass = "poor";
                    let snrRating = "Debole";
                    if (snrVal >= 5) { snrClass = "good"; snrRating = "Ottimo"; }
                    else if (snrVal >= 0) { snrClass = "fair"; snrRating = "Buono"; }
                    metricsParts.push(`<span class="signal-pill ${snrClass}">📡 SNR: ${snrVal >= 0 ? "+" : ""}${snrVal.toFixed(1)} dB (${snrRating})</span>`);
                }
                if (m.hops !== undefined && m.hops !== null) {
                    const hopsVal = parseInt(m.hops);
                    const hopsStr = (hopsVal === 0 || hopsVal === 255) ? "Diretto (0 salti)" : `${hopsVal} ${hopsVal === 1 ? "salto" : "salti"}`;
                    metricsParts.push(`<span>🔗 ${hopsStr}</span>`);
                }
                if (m.lat && m.lon) {
                    metricsParts.push(`<a href="https://www.openstreetmap.org/?mlat=${m.lat}&mlon=${m.lon}#map=14/${m.lat}/${m.lon}" target="_blank" class="map-btn">📍 Mappa (${m.lat.toFixed(4)}, ${m.lon.toFixed(4)})</a>`);
                }
                if (m.sent_to_radio !== undefined && m.sent_to_radio !== null) {
                    if (m.sent_to_radio === true) {
                        metricsParts.push(`<span class="signal-pill good" title="Confermato: Trasmesso via radio LoRa dall'antenna">✓✓ Trasmesso via LoRa</span>`);
                    } else {
                        metricsParts.push(`<span class="signal-pill poor" title="Heltec offline: salvato solo nella room">⚠️ Non irradiato (Heltec offline)</span>`);
                    }
                }

                if (metricsParts.length > 0) {
                    footerHtml = `<div class="msg-footer">${metricsParts.join(" &nbsp;•&nbsp; ")}</div>`;
                }

                bubble.innerHTML = headerHtml + contentHtml + footerHtml;
                container.appendChild(bubble);
            });

            // Scroll to bottom
            container.scrollTop = container.scrollHeight;
        }

        // Heard Nodes rendering
        function renderHeardNodes() {
            const list = document.getElementById("nodesList");
            const badge = document.getElementById("nodesCountBadge");
            badge.innerText = heardNodes.length;

            if (heardNodes.length === 0) {
                list.innerHTML = '<div class="empty-state">In attesa di traffico radio...</div>';
                return;
            }

            list.innerHTML = "";
            heardNodes.forEach(n => {
                const card = document.createElement("div");
                card.className = "node-card";
                
                const timeAgo = formatTimeAgo(n.last_seen);
                let snrBadge = "";
                if (n.last_snr !== null && n.last_snr !== undefined) {
                    const s = parseFloat(n.last_snr);
                    snrBadge = `SNR ${s >= 0 ? "+" : ""}${s.toFixed(1)}dB`;
                }

                let mapLink = "";
                if (n.lat && n.lon) {
                    mapLink = `• <a href="https://www.openstreetmap.org/?mlat=${n.lat}&mlon=${n.lon}#map=14/${n.lat}/${n.lon}" target="_blank" class="map-btn" style="font-size:0.72rem;">📍 Mappa</a>`;
                }

                card.innerHTML = `
                    <div class="node-card-top">
                        <span class="node-name">${escapeHtml(n.node_name)}</span>
                        <span class="node-packets">${n.packets_count || 1} pkt</span>
                    </div>
                    <div class="node-card-meta">
                        <span>⏱️ ${timeAgo}</span>
                        <span>📻 ${escapeHtml(n.last_channel || "LoRa")}</span>
                        ${snrBadge ? `<span>${snrBadge}</span>` : ""}
                        ${mapLink}
                    </div>
                `;
                list.appendChild(card);
            });
        }

        function updateStationInfo(info, stats, heltecConnected) {
            const badge = document.getElementById("boardStatusBadge");
            const text = document.getElementById("boardStatusText");

            if (heltecConnected !== undefined && heltecConnected !== null) {
                isHeltecConnected = !!heltecConnected;
            }

            if (heltecConnected) {
                badge.className = "badge-status online";
                text.innerText = "Heltec Online";
            } else {
                badge.className = "badge-status offline";
                text.innerText = "Heltec Offline";
            }

            if (info) {
                if (info.name) document.getElementById("nodeNameTag").innerText = info.name;
                if (info.freq_mhz) document.getElementById("statFreq").innerText = `${info.freq_mhz.toFixed(3)} MHz`;
                if (info.sf && info.bw_khz) {
                    document.getElementById("statMod").innerText = `SF${info.sf} / BW${info.bw_khz}`;
                }
            }

            if (stats) {
                document.getElementById("statRx").innerText = stats.packets_rx || 0;
                document.getElementById("statTx").innerText = stats.packets_tx || 0;
            }
        }

        // WebSocket Connection
        function connectWebSocket() {
            const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
            const wsUrl = `${protocol}//${window.location.host}/ws/client`;
            console.log("Connecting WebSocket:", wsUrl);

            socket = new WebSocket(wsUrl);

            socket.onopen = () => {
                console.log("WebSocket connected to MeshCore bridge.");
                document.getElementById("loadingState")?.remove();
            };

            socket.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    handleIncomingEvent(data);
                } catch(e) {
                    console.error("Error parsing WebSocket JSON:", e);
                }
            };

            socket.onclose = () => {
                console.log("WebSocket closed. Reconnecting in 3s...");
                setTimeout(connectWebSocket, 3000);
            };

            socket.onerror = (err) => {
                console.error("WebSocket error:", err);
                socket.close();
            };
        }

        function handleIncomingEvent(data) {
            console.log("Event received:", data.type);

            if (data.type === "init") {
                if (data.channels) channels = data.channels;
                if (data.recent_messages) messages = data.recent_messages;
                if (data.recent_nodes) heardNodes = data.recent_nodes;
                renderChannelsBar();
                renderMessages();
                renderHeardNodes();
                updateStationInfo(data.node_info, data.stats, data.heltec_connected);
            } 
            else if (data.type === "new_message") {
                messages.push(data);
                renderMessages();
                if (data.source === "LoRa Mesh") {
                    playChime();
                }
            }
            else if (data.type === "channels") {
                channels = data.channels;
                renderChannelsBar();
            }
            else if (data.type === "nodes") {
                heardNodes = data.nodes;
                renderHeardNodes();
            }
            else if (data.type === "status") {
                if (data.channels) channels = data.channels;
                updateStationInfo(data.node_info, data.stats, data.heltec_connected);
            }
            else if (data.type === "node_info") {
                updateStationInfo(data.node_info, null, true);
            }
        }

        function showToast(msg, isSuccess) {
            let toast = document.getElementById("toastNotification");
            if (!toast) {
                toast = document.createElement("div");
                toast.id = "toastNotification";
                toast.style.position = "fixed";
                toast.style.bottom = "84px";
                toast.style.left = "50%";
                toast.style.transform = "translateX(-50%)";
                toast.style.padding = "10px 18px";
                toast.style.borderRadius = "10px";
                toast.style.fontSize = "0.86rem";
                toast.style.fontWeight = "600";
                toast.style.zIndex = "999";
                toast.style.transition = "all 0.25s ease";
                toast.style.boxShadow = "0 6px 18px rgba(0,0,0,0.5)";
                toast.style.maxWidth = "90%";
                toast.style.textAlign = "center";
                document.body.appendChild(toast);
            }
            toast.style.background = isSuccess ? "#059669" : "#d97706";
            toast.style.color = "#ffffff";
            toast.innerText = msg;
            toast.style.opacity = "1";
            toast.style.pointerEvents = "auto";
            setTimeout(() => { 
                if (toast) {
                    toast.style.opacity = "0"; 
                    toast.style.pointerEvents = "none";
                }
            }, 3800);
        }

        // Send Message
        async function sendMessage() {
            const input = document.getElementById("messageInput");
            const callsignInput = document.getElementById("callsignInput");
            const text = input.value.trim();
            const sender = callsignInput.value.trim() || "Web-Operatore";
            const channelIdx = parseInt(document.getElementById("channelSelect").value || 0);

            if (!text) return;

            const payload = {
                action: "send_message",
                channel_idx: channelIdx,
                text: text,
                sender: sender
            };

            const sendBtn = document.getElementById("sendBtn");
            sendBtn.disabled = true;

            let sentSuccess = false;

            // Try via WebSocket first
            if (socket && socket.readyState === WebSocket.OPEN) {
                socket.send(JSON.stringify(payload));
                sentSuccess = true;
            } else {
                // Fallback to REST API
                try {
                    const resp = await fetch("/api/send", {
                        method: "POST",
                        headers: {"Content-Type": "application/json"},
                        body: JSON.stringify({
                            channel_idx: channelIdx,
                            text: text,
                            sender: sender
                        })
                    });
                    if (resp.ok) sentSuccess = true;
                } catch(e) {
                    console.error("REST send error:", e);
                }
            }

            sendBtn.disabled = false;
            if (sentSuccess) {
                input.value = "";
                input.focus();
                if (isHeltecConnected) {
                    showToast("📡 Inviato! La Heltec sta trasmettendo il pacchetto via LoRa.", true);
                } else {
                    showToast("💾 Salvato nel server. (Heltec offline, non irradiato via radio).", false);
                }
            } else {
                showToast("❌ Errore invio: Impossibile contattare il server MeshCore.", false);
            }
        }

        document.getElementById("sendBtn").addEventListener("click", sendMessage);
        document.getElementById("messageInput").addEventListener("keydown", (e) => {
            if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                sendMessage();
            }
        });

        document.getElementById("refreshChannelsBtn").addEventListener("click", () => {
            if (socket && socket.readyState === WebSocket.OPEN) {
                socket.send(JSON.stringify({action: "refresh_channels"}));
            }
            fetch("/api/status").then(r => r.json()).then(d => {
                if (d.discovered_channels) {
                    channels = d.discovered_channels;
                    renderChannelsBar();
                }
            });
        });

        // Helpers
        function escapeHtml(str) {
            if (!str) return "";
            return str
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#039;");
        }

        function formatTimeAgo(timeStr) {
            if (!timeStr) return "N/A";
            try {
                if (timeStr.length >= 16) {
                    return timeStr.substring(11, 16);
                }
                return timeStr;
            } catch(e) {
                return timeStr;
            }
        }

        // Keep-alive WebSocket Ping
        setInterval(() => {
            if (socket && socket.readyState === WebSocket.OPEN) {
                socket.send(JSON.stringify({action: "ping"}));
            }
        }, 25000);

        // Start
        renderChannelsBar();
        connectWebSocket();
    </script>
</body>
</html>"""
