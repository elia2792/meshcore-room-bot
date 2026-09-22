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
    <link rel="icon" href="/api/icon.svg">
    <link rel="manifest" href="/manifest.json">
    <meta name="theme-color" content="#10b981">
    <meta name="apple-mobile-web-app-capable" content="yes">
    <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
    <meta name="apple-mobile-web-app-title" content="MeshCore">
    <link rel="apple-touch-icon" href="/api/icon.svg">
    <!-- Leaflet CSS & JS for Interactive LoRa Map -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin=""/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
    <style>
        :root {
            --bg: #090d16;
            --surface: #111827;
            --surface-card: #162032;
            --surface-hover: #1e293b;
            --border: #1f2d42;
            --border-highlight: #334155;
            --text-main: #f8fafc;
            --text-muted: #94a3b8;
            --text-dim: #64748b;
            --primary: #10b981;
            --primary-dark: #059669;
            --primary-glow: rgba(16, 185, 129, 0.25);
            --accent-blue: #38bdf8;
            --accent-purple: #a855f7;
            --accent-amber: #f59e0b;
            --danger: #ef4444;
            --ack-sent: #94a3b8;
            --ack-air: #38bdf8;
            --ack-ok: #10b981;
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
            user-select: none;
        }

        /* Top Header Bar */
        header {
            background: rgba(17, 24, 39, 0.94);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            border-bottom: 1px solid var(--border);
            padding: 10px 16px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
            flex-shrink: 0;
            z-index: 30;
        }

        .header-brand {
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .logo-box {
            width: 34px;
            height: 34px;
            border-radius: 9px;
            background: linear-gradient(135deg, #10b981, #0284c7);
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.15rem;
            box-shadow: 0 2px 10px rgba(16, 185, 129, 0.35);
        }

        .brand-meta {
            display: flex;
            flex-direction: column;
        }

        .brand-title {
            font-size: 1.02rem;
            font-weight: 700;
            letter-spacing: -0.01em;
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .brand-node {
            font-size: 0.76rem;
            color: var(--text-muted);
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 5px;
            padding: 3px 8px;
            border-radius: 9999px;
            font-size: 0.72rem;
            font-weight: 600;
            letter-spacing: 0.03em;
        }

        .status-badge.online {
            background: rgba(16, 185, 129, 0.15);
            color: #34d399;
            border: 1px solid rgba(16, 185, 129, 0.3);
        }

        .status-badge.offline {
            background: rgba(239, 68, 68, 0.15);
            color: #f87171;
            border: 1px solid rgba(239, 68, 68, 0.3);
        }

        .pulse-dot {
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: currentColor;
            display: inline-block;
        }

        .status-badge.online .pulse-dot {
            box-shadow: 0 0 6px #10b981;
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.35; transform: scale(0.8); }
        }

        .header-actions {
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .btn-icon {
            background: var(--surface-card);
            border: 1px solid var(--border);
            color: var(--text-main);
            padding: 7px 10px;
            border-radius: 8px;
            font-size: 0.82rem;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            gap: 5px;
            transition: all 0.15s ease;
            text-decoration: none;
        }

        .btn-icon:hover {
            border-color: var(--border-highlight);
            background: var(--surface-hover);
        }

        /* Main View Container */
        .app-view-container {
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
            position: relative;
        }

        .tab-screen {
            display: none;
            flex-direction: column;
            width: 100%;
            height: 100%;
            overflow-y: auto;
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
        }

        .tab-screen.active {
            display: flex;
        }

        /* TAB 1: MESSAGES */
        .channels-nav-bar {
            background: #0b111e;
            border-bottom: 1px solid var(--border);
            padding: 8px 12px;
            display: flex;
            align-items: center;
            gap: 8px;
            overflow-x: auto;
            scrollbar-width: none;
            flex-shrink: 0;
        }

        .channels-nav-bar::-webkit-scrollbar {
            display: none;
        }

        .channel-chip {
            background: var(--surface-card);
            border: 1px solid var(--border);
            color: var(--text-muted);
            padding: 6px 13px;
            border-radius: 20px;
            font-size: 0.82rem;
            font-weight: 500;
            cursor: pointer;
            white-space: nowrap;
            display: inline-flex;
            align-items: center;
            gap: 6px;
            transition: all 0.15s ease;
        }

        .channel-chip:hover {
            border-color: var(--border-highlight);
            color: var(--text-main);
        }

        .channel-chip.active {
            background: var(--primary);
            border-color: var(--primary);
            color: #022c22;
            font-weight: 700;
            box-shadow: 0 2px 8px var(--primary-glow);
        }

        .chip-badge {
            background: rgba(0, 0, 0, 0.2);
            padding: 1px 6px;
            border-radius: 10px;
            font-size: 0.72rem;
            font-weight: 700;
        }

        .channel-chip.active .chip-badge {
            background: rgba(0, 0, 0, 0.35);
            color: #fff;
        }

        .chat-messages-scroll {
            flex: 1;
            padding: 14px 16px;
            overflow-y: auto;
            display: flex;
            flex-direction: column;
            gap: 12px;
            scroll-behavior: smooth;
        }

        .chat-date-separator {
            text-align: center;
            margin: 6px 0;
            position: relative;
        }

        .chat-date-separator span {
            background: var(--surface-card);
            border: 1px solid var(--border);
            padding: 3px 12px;
            border-radius: 12px;
            font-size: 0.72rem;
            color: var(--text-dim);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .msg-bubble-wrap {
            display: flex;
            flex-direction: column;
            max-width: 82%;
            animation: fadeIn 0.2s cubic-bezier(0.16, 1, 0.3, 1);
        }

        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(6px); }
            to { opacity: 1; transform: translateY(0); }
        }

        .msg-bubble-wrap.outgoing {
            align-self: flex-end;
            align-items: flex-end;
        }

        .msg-bubble-wrap.incoming {
            align-self: flex-start;
            align-items: flex-start;
        }

        .msg-sender-name {
            font-size: 0.73rem;
            font-weight: 600;
            color: var(--text-muted);
            margin-bottom: 3px;
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .msg-bubble {
            padding: 10px 14px;
            border-radius: 14px;
            font-size: 0.92rem;
            line-height: 1.42;
            word-break: break-word;
            position: relative;
            user-select: text;
        }

        .msg-bubble-wrap.outgoing .msg-bubble {
            background: linear-gradient(135deg, #059669, #047857);
            color: white;
            border-bottom-right-radius: 3px;
            box-shadow: 0 2px 8px rgba(5, 150, 105, 0.25);
        }

        .msg-bubble-wrap.incoming .msg-bubble {
            background: var(--surface-card);
            border: 1px solid var(--border);
            color: var(--text-main);
            border-bottom-left-radius: 3px;
        }

        .msg-footer {
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 0.71rem;
            margin-top: 3px;
            color: var(--text-dim);
        }

        .ack-indicator {
            display: inline-flex;
            align-items: center;
            gap: 4px;
            font-size: 0.74rem;
            padding: 2px 7px;
            border-radius: 6px;
            font-weight: 600;
            letter-spacing: 0.2px;
        }

        .ack-indicator.pending {
            color: #94a3b8;
            background: rgba(148, 163, 184, 0.1);
        }

        .ack-indicator.air {
            color: #38bdf8;
            background: rgba(56, 189, 248, 0.15);
            border: 1px solid rgba(56, 189, 248, 0.3);
        }

        .ack-indicator.confirmed {
            color: #10b981;
            background: rgba(16, 185, 129, 0.18);
            border: 1px solid rgba(16, 185, 129, 0.4);
            box-shadow: 0 0 10px rgba(16, 185, 129, 0.25);
            animation: pulseRecapito 1.2s ease-out;
        }

        @keyframes pulseRecapito {
            0% { transform: scale(1.18); box-shadow: 0 0 16px rgba(16, 185, 129, 0.7); }
            100% { transform: scale(1.0); box-shadow: 0 0 10px rgba(16, 185, 129, 0.25); }
        }

        .reply-bar {
            display: flex;
            align-items: center;
            justify-content: space-between;
            background: rgba(30, 41, 59, 0.95);
            border-left: 3px solid var(--primary);
            border-radius: 8px;
            padding: 7px 12px;
            margin-bottom: 2px;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.35);
            animation: fadeInReply 0.15s ease-out;
        }

        @keyframes fadeInReply {
            from { opacity: 0; transform: translateY(4px); }
            to { opacity: 1; transform: translateY(0); }
        }

        .reply-bar-left {
            display: flex;
            flex-direction: column;
            gap: 2px;
            overflow: hidden;
            flex: 1;
        }

        .reply-bar-title {
            font-size: 0.76rem;
            font-weight: 700;
            color: var(--primary);
            display: flex;
            align-items: center;
            gap: 4px;
        }

        .reply-bar-snippet {
            font-size: 0.74rem;
            color: var(--text-dim);
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            max-width: 270px;
        }

        .reply-bar-close {
            background: transparent;
            border: none;
            color: var(--text-dim);
            font-size: 1.15rem;
            cursor: pointer;
            padding: 4px 8px;
            line-height: 1;
            border-radius: 4px;
            transition: color 0.15s;
        }

        .reply-bar-close:hover {
            color: #ef4444;
        }

        .msg-quote {
            background: rgba(0, 0, 0, 0.28);
            border-left: 3px solid var(--primary);
            border-radius: 4px;
            padding: 4px 8px;
            margin-bottom: 6px;
            font-size: 0.76rem;
            line-height: 1.3;
        }

        .msg-quote-sender {
            font-weight: 700;
            color: var(--primary);
            font-size: 0.72rem;
        }

        .msg-quote-text {
            color: #cbd5e1;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .msg-bubble-wrap {
            cursor: pointer;
            -webkit-tap-highlight-color: transparent;
            user-select: text;
        }

        .chat-input-container {
            background: var(--surface);
            border-top: 1px solid var(--border);
            padding: 10px 14px;
            display: flex;
            flex-direction: column;
            gap: 8px;
            flex-shrink: 0;
        }

        .chat-input-row {
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .chat-input-row input[type="text"] {
            flex: 1;
            background: var(--bg);
            border: 1px solid var(--border);
            border-radius: 24px;
            padding: 10px 16px;
            font-size: 0.92rem;
            color: var(--text-main);
            outline: none;
            transition: border-color 0.15s ease;
        }

        .chat-input-row input[type="text"]:focus {
            border-color: var(--primary);
            box-shadow: 0 0 0 3px var(--primary-glow);
        }

        .btn-send {
            width: 42px;
            height: 42px;
            border-radius: 50%;
            background: var(--primary);
            color: #022c22;
            border: none;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.15rem;
            cursor: pointer;
            transition: all 0.15s ease;
            flex-shrink: 0;
            box-shadow: 0 2px 8px var(--primary-glow);
        }

        .btn-send:hover {
            background: #34d399;
            transform: scale(1.05);
        }

        .btn-send:active {
            transform: scale(0.95);
        }

        .chat-tools-row {
            display: flex;
            align-items: center;
            justify-content: space-between;
            font-size: 0.75rem;
            color: var(--text-dim);
            padding: 0 4px;
        }

        /* TAB 2: NODI & MAPPA */
        .content-scroll {
            padding: 16px;
            overflow-y: auto;
            flex: 1;
            display: flex;
            flex-direction: column;
            gap: 12px;
        }

        .section-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 4px;
        }

        .section-title {
            font-size: 1rem;
            font-weight: 700;
            color: var(--text-main);
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .nodes-filter-bar {
            display: flex;
            gap: 8px;
            margin: 4px 0 8px 0;
            flex-wrap: wrap;
        }

        .filter-chip {
            padding: 6px 14px;
            border-radius: 20px;
            border: 1px solid var(--border);
            background: var(--surface-card);
            color: var(--text-muted);
            font-size: 0.8rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.15s ease;
            display: inline-flex;
            align-items: center;
            gap: 6px;
        }

        .filter-chip:hover {
            border-color: var(--border-highlight);
            color: var(--text-main);
        }

        .filter-chip.active {
            background: rgba(16, 185, 129, 0.15);
            border-color: var(--primary);
            color: var(--primary);
        }

        .node-card {
            background: var(--surface-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 12px 14px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
            transition: all 0.15s ease;
        }

        .node-card:hover {
            border-color: var(--border-highlight);
            background: var(--surface-hover);
        }

        .node-avatar {
            width: 40px;
            height: 40px;
            border-radius: 10px;
            background: linear-gradient(135deg, #1e293b, #334155);
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.2rem;
            color: var(--text-main);
            flex-shrink: 0;
            border: 1px solid rgba(255,255,255,0.06);
        }

        .node-info-col {
            flex: 1;
            min-width: 0;
        }

        .node-name-text {
            font-size: 0.92rem;
            font-weight: 700;
            color: var(--text-main);
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .node-sub-text {
            font-size: 0.74rem;
            color: var(--text-muted);
            margin-top: 2px;
            display: flex;
            align-items: center;
            gap: 8px;
            flex-wrap: wrap;
        }

        .node-badge {
            background: rgba(56, 189, 248, 0.12);
            color: var(--accent-blue);
            padding: 2px 7px;
            border-radius: 6px;
            font-size: 0.7rem;
            font-weight: 600;
        }

        .node-actions-col {
            display: flex;
            flex-direction: column;
            gap: 4px;
            align-items: flex-end;
        }

        /* TAB 3: CANALI */
        .channel-card {
            background: var(--surface-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 14px;
            display: flex;
            flex-direction: column;
            gap: 10px;
        }

        .channel-card-top {
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .channel-slot-badge {
            background: rgba(16, 185, 129, 0.15);
            color: #34d399;
            border: 1px solid rgba(16, 185, 129, 0.3);
            padding: 3px 8px;
            border-radius: 6px;
            font-weight: 700;
            font-size: 0.75rem;
        }

        /* TAB 4: IMPOSTAZIONI */
        .settings-card {
            background: var(--surface-card);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 16px;
            display: flex;
            flex-direction: column;
            gap: 14px;
            margin-bottom: 12px;
        }

        .settings-card-title {
            font-size: 0.92rem;
            font-weight: 700;
            color: var(--text-main);
            display: flex;
            align-items: center;
            gap: 8px;
            border-bottom: 1px solid var(--border);
            padding-bottom: 8px;
        }

        .form-row {
            display: flex;
            flex-direction: column;
            gap: 6px;
        }

        .form-row label {
            font-size: 0.78rem;
            font-weight: 600;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.04em;
        }

        .form-control {
            background: var(--bg);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 8px 12px;
            color: var(--text-main);
            font-size: 0.9rem;
            outline: none;
            transition: border-color 0.15s ease;
        }

        .form-control:focus {
            border-color: var(--primary);
        }

        .form-grid-2 {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
        }

        .btn-action {
            background: var(--primary);
            color: #022c22;
            border: none;
            padding: 10px 16px;
            border-radius: 8px;
            font-size: 0.88rem;
            font-weight: 700;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 6px;
            transition: all 0.15s ease;
        }

        .btn-action:hover {
            background: #34d399;
        }

        .btn-action.btn-danger {
            background: var(--danger);
            color: white;
        }

        .btn-action.btn-danger:hover {
            background: #dc2626;
        }

        .btn-action.btn-secondary {
            background: var(--surface-hover);
            color: var(--text-main);
            border: 1px solid var(--border-highlight);
        }

        .btn-action.btn-secondary:hover {
            background: #334155;
        }

        /* Map styling */
        .map-container {
            width: 100%;
            height: 380px;
            border-radius: 12px;
            border: 1px solid var(--border);
            overflow: hidden;
            background: #111;
            margin-top: 8px;
            position: relative;
            z-index: 10;
        }

        .map-iframe-container {
            width: 100%;
            height: 480px;
            border-radius: 12px;
            border: 1px solid var(--border);
            overflow: hidden;
            background: #0f172a;
            margin-top: 8px;
            position: relative;
            z-index: 10;
        }

        .map-mode-tabs {
            display: flex;
            background: var(--bg);
            padding: 3px;
            border-radius: 8px;
            border: 1px solid var(--border);
            gap: 4px;
            margin-top: 8px;
        }

        .map-mode-tab {
            flex: 1;
            padding: 6px 10px;
            font-size: 0.78rem;
            font-weight: 600;
            border-radius: 6px;
            border: none;
            background: transparent;
            color: var(--text-muted);
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 6px;
            transition: all 0.15s ease;
        }

        .map-mode-tab.active {
            background: var(--surface-card);
            color: var(--primary);
            box-shadow: 0 1px 3px rgba(0,0,0,0.2);
            font-weight: 700;
        }

        .map-stats-bar {
            display: flex;
            align-items: center;
            justify-content: space-between;
            font-size: 0.78rem;
            color: var(--text-muted);
            margin-top: 6px;
            padding: 0 4px;
        }

        .leaflet-popup-content-wrapper {
            background: #111827 !important;
            color: #f8fafc !important;
            border: 1px solid #334155 !important;
            border-radius: 8px !important;
            font-family: inherit !important;
        }

        .leaflet-popup-tip {
            background: #111827 !important;
        }

        /* Bottom Tab Navigation Bar */
        .bottom-nav {
            background: rgba(17, 24, 39, 0.96);
            backdrop-filter: blur(14px);
            -webkit-backdrop-filter: blur(14px);
            border-top: 1px solid var(--border);
            padding: 6px 12px 10px 12px;
            display: flex;
            align-items: center;
            justify-content: space-around;
            flex-shrink: 0;
            z-index: 40;
        }

        .nav-item {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            background: transparent;
            border: none;
            color: var(--text-muted);
            cursor: pointer;
            padding: 6px 14px;
            border-radius: 10px;
            transition: all 0.15s ease;
            flex: 1;
            max-width: 90px;
        }

        .nav-item:hover {
            color: var(--text-main);
        }

        .nav-item.active {
            color: var(--primary);
        }

        .nav-icon {
            font-size: 1.3rem;
            line-height: 1;
            margin-bottom: 3px;
        }

        .nav-label {
            font-size: 0.7rem;
            font-weight: 600;
            letter-spacing: 0.01em;
        }

        /* Toast Notifications */
        .toast-container {
            position: fixed;
            bottom: 70px;
            left: 50%;
            transform: translateX(-50%);
            display: flex;
            flex-direction: column;
            gap: 8px;
            z-index: 100;
            pointer-events: none;
            width: 90%;
            max-width: 420px;
        }

        .toast-bubble {
            background: var(--surface-card);
            border: 1px solid var(--border-highlight);
            color: var(--text-main);
            padding: 10px 16px;
            border-radius: 10px;
            font-size: 0.84rem;
            box-shadow: 0 8px 24px rgba(0,0,0,0.5);
            animation: slideUp 0.25s cubic-bezier(0.16, 1, 0.3, 1);
            display: flex;
            align-items: center;
            gap: 10px;
            pointer-events: auto;
        }

        @keyframes slideUp {
            from { opacity: 0; transform: translateY(12px); }
            to { opacity: 1; transform: translateY(0); }
        }

        /* Modal Dialog for Scan Results */
        .modal-overlay {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(4, 7, 13, 0.82);
            backdrop-filter: blur(8px);
            -webkit-backdrop-filter: blur(8px);
            display: none;
            align-items: center;
            justify-content: center;
            z-index: 99999 !important;
            opacity: 0;
            pointer-events: none;
            transition: opacity 0.2s ease;
            padding: 16px;
        }

        .modal-overlay.active {
            display: flex !important;
            opacity: 1 !important;
            pointer-events: auto !important;
        }

        .modal-dialog {
            background: var(--surface);
            border: 1px solid var(--border-highlight);
            border-radius: 16px;
            width: 100%;
            max-width: 560px;
            max-height: 85vh;
            display: flex;
            flex-direction: column;
            box-shadow: 0 20px 50px rgba(0, 0, 0, 0.85), 0 0 30px rgba(16, 185, 129, 0.2);
            transform: scale(0.95);
            transition: transform 0.2s cubic-bezier(0.16, 1, 0.3, 1);
            overflow: hidden;
            position: relative;
            z-index: 100000 !important;
        }

        .modal-overlay.active .modal-dialog {
            transform: scale(1);
        }

        .modal-header {
            padding: 16px 20px;
            border-bottom: 1px solid var(--border);
            display: flex;
            align-items: center;
            justify-content: space-between;
            background: var(--surface-card);
        }

        .modal-title {
            font-size: 1.05rem;
            font-weight: 700;
            display: flex;
            align-items: center;
            gap: 8px;
            color: var(--text-main);
        }

        .modal-close-btn {
            background: rgba(255, 255, 255, 0.08);
            border: none;
            color: var(--text-muted);
            width: 32px;
            height: 32px;
            border-radius: 50%;
            font-size: 1.1rem;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: all 0.15s ease;
        }

        .modal-close-btn:hover {
            background: rgba(239, 68, 68, 0.2);
            color: var(--danger);
        }

        .modal-body {
            padding: 16px 20px;
            overflow-y: auto;
            flex: 1;
            display: flex;
            flex-direction: column;
            gap: 12px;
        }

        .scan-node-card {
            background: var(--surface-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 12px 14px;
            display: flex;
            flex-direction: column;
            gap: 8px;
            transition: border-color 0.15s ease;
        }

        .scan-node-card:hover {
            border-color: var(--primary);
        }

        .scan-node-top {
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .scan-node-name {
            font-size: 0.95rem;
            font-weight: 700;
            color: var(--text-main);
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .scan-telemetry-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
            gap: 6px 10px;
            font-size: 0.77rem;
            background: rgba(0, 0, 0, 0.25);
            padding: 8px 10px;
            border-radius: 8px;
            border: 1px solid rgba(255, 255, 255, 0.04);
        }

        .telemetry-item {
            display: flex;
            flex-direction: column;
        }

        .telemetry-label {
            color: var(--text-dim);
            font-size: 0.7rem;
            text-transform: uppercase;
            letter-spacing: 0.03em;
        }

        .telemetry-val {
            color: var(--text-main);
            font-weight: 600;
            margin-top: 1px;
        }

        .modal-footer {
            padding: 12px 20px;
            border-top: 1px solid var(--border);
            background: var(--surface-card);
            display: flex;
            align-items: center;
            justify-content: space-between;
            font-size: 0.8rem;
            color: var(--text-muted);
        }

        /* Search Bar in Chat */
        .chat-search-bar {
            display: flex;
            align-items: center;
            gap: 8px;
            background: #0b111e;
            border-bottom: 1px solid var(--border);
            padding: 6px 14px;
            flex-shrink: 0;
        }
        .chat-search-bar input {
            flex: 1;
            background: transparent;
            border: none;
            color: var(--text-main);
            font-size: 0.82rem;
            outline: none;
        }
        .chat-search-bar input::placeholder {
            color: var(--text-dim);
        }
        .clear-search-btn {
            background: transparent;
            border: none;
            color: var(--text-muted);
            cursor: pointer;
            font-size: 0.82rem;
            padding: 2px 6px;
        }

        /* Radar Proximity Alert Banner */
        .radar-alert-banner {
            background: linear-gradient(135deg, rgba(239, 68, 68, 0.22), rgba(245, 158, 11, 0.18));
            border: 1px solid #ef4444;
            border-radius: 10px;
            padding: 10px 14px;
            margin: 10px 14px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            animation: radarPulse 1.8s infinite;
        }
        @keyframes radarPulse {
            0% { box-shadow: 0 0 0 0 rgba(239, 68, 68, 0.4); }
            70% { box-shadow: 0 0 0 10px rgba(239, 68, 68, 0); }
            100% { box-shadow: 0 0 0 0 rgba(239, 68, 68, 0); }
        }

        /* 24h Traffic Chart */
        .traffic-chart-container {
            width: 100%;
            height: 150px;
            background: #090d16;
            border-radius: 10px;
            border: 1px solid var(--border);
            padding: 12px;
            box-sizing: border-box;
            display: flex;
            flex-direction: column;
            justify-content: flex-end;
            position: relative;
            margin-top: 8px;
        }
        .traffic-bars-row {
            display: flex;
            align-items: flex-end;
            gap: 3px;
            height: 105px;
            width: 100%;
        }
        .traffic-bar-col {
            flex: 1;
            display: flex;
            flex-direction: column;
            align-items: center;
            height: 100%;
            justify-content: flex-end;
            cursor: pointer;
            position: relative;
        }
        .traffic-bar-fill {
            width: 100%;
            background: linear-gradient(to top, var(--primary), #38bdf8);
            border-radius: 3px 3px 0 0;
            min-height: 2px;
            transition: height 0.3s ease;
        }
        .traffic-bar-col:hover .traffic-bar-fill {
            background: #f59e0b;
        }
        .traffic-bar-hour {
            font-size: 0.58rem;
            color: var(--text-dim);
            margin-top: 4px;
            text-align: center;
        }
    </style>
</head>
<body>
    <!-- Top Header -->
    <header>
        <div class="header-brand">
            <div class="logo-box">📡</div>
            <div class="brand-meta">
                <div class="brand-title">
                    <span>MeshCore</span>
                    <span id="connBadge" class="status-badge offline">
                        <span class="pulse-dot"></span>
                        <span id="connBadgeText">DISCONNESSO</span>
                    </span>
                </div>
                <div class="brand-node">
                    <span id="nodeNameDisplay">Buscate</span> • <span id="radioFreqDisplay">869.618 MHz</span>
                </div>
            </div>
        </div>
        <div class="header-actions">
            <button id="notifToggleBtn" class="btn-icon" title="Attiva Notifiche Push Browser" onclick="togglePushNotifications()">📲 App</button>
            <button id="audioToggleBtn" class="btn-icon" title="Attiva/Disattiva Suoni">🔔</button>
            <a href="https://t.me/Meshcoreeliaxs_bot" target="_blank" class="btn-icon" title="Apri Telegram">✈️ Telegram</a>
        </div>
    </header>

    <!-- Main Screens Container -->
    <div class="app-view-container">

        <!-- TAB 1: MESSAGGI (Chat Screen) -->
        <div id="tabMessages" class="tab-screen active">
            <!-- Channel Selection Chips -->
            <div id="channelsNavBar" class="channels-nav-bar">
                <!-- Dynamically filled -->
            </div>

            <!-- Live Message Search Bar -->
            <div class="chat-search-bar" id="chatSearchBar">
                <span style="font-size:0.85rem; color:var(--text-dim);">🔍</span>
                <input type="text" id="chatSearchInput" placeholder="Cerca parole o nominativi nei messaggi..." oninput="handleChatSearch()" autocomplete="off"/>
                <button id="clearSearchBtn" class="clear-search-btn" onclick="clearChatSearch()" style="display:none;" title="Azzera ricerca">✕</button>
            </div>

            <!-- Messages Log -->
            <div id="chatMessagesScroll" class="chat-messages-scroll">
                <div class="chat-date-separator">
                    <span>Oggi</span>
                </div>
                <!-- Chat bubbles dynamically filled -->
            </div>

            <!-- Input Bar -->
            <div class="chat-input-container">
                <!-- Reply Bar -->
                <div id="replyBar" class="reply-bar" style="display: none;">
                    <div class="reply-bar-left">
                        <div class="reply-bar-title">↩️ Rispondi a <span id="replyToSender"></span></div>
                        <div id="replyToSnippet" class="reply-bar-snippet"></div>
                    </div>
                    <button id="cancelReplyBtn" class="reply-bar-close" title="Annulla risposta">✕</button>
                </div>

                <div class="chat-input-row">
                    <input type="text" id="messageInput" placeholder="Scrivi un messaggio LoRa..." autocomplete="off" />
                    <button id="sendBtn" class="btn-send" title="Trasmetti messaggio via radio">➤</button>
                </div>
                <div class="chat-tools-row">
                    <span>Canale: <b id="currentChannelName" style="color: var(--primary);">Public [0]</b></span>
                    <div style="display:flex; align-items:center; gap:8px;">
                        <button id="shareLocationBtn" class="btn-icon" style="padding:2px 8px; font-size:0.75rem; color:var(--accent-blue);" onclick="shareGpsLocationInChat()" title="Invia coordinate GPS correnti nel canale LoRa">📍 Invia GPS</button>
                        <span>Operatore: <input type="text" id="senderNameInput" value="Web-Operatore" style="background:transparent; border:none; color:var(--accent-blue); font-weight:600; width:110px; text-align:right;" /></span>
                    </div>
                </div>
            </div>
        </div>

        <!-- TAB 2: NODI & MAPPA -->
        <div id="tabNodes" class="tab-screen">
            <div class="content-scroll">
                <div class="section-header">
                    <div class="section-title">👥 Nodi Radio Ascoltati (<span id="nodesCount">0</span>)</div>
                    <button id="refreshNodesBtn" class="btn-icon">🔄 Aggiorna</button>
                </div>
                <div class="nodes-filter-bar">
                    <button id="filterNodesAll" class="filter-chip active" onclick="setNodesFilter('all')">
                        👥 Tutti (<span id="nodesCountAll">0</span>)
                    </button>
                    <button id="filterNodesDirect" class="filter-chip" onclick="setNodesFilter('direct')">
                        🎯 Solo Diretti RF (<span id="nodesCountDirect">0</span>)
                    </button>
                </div>
                <div id="nodesListContainer" style="display:flex; flex-direction:column; gap:10px;">
                    <!-- Filled dynamically -->
                </div>
            </div>
        </div>

        <!-- TAB 3: CANALI -->
        <div id="tabChannels" class="tab-screen">
            <div class="content-scroll">
                <div class="section-header">
                    <div class="section-title">📻 Configurazione Canali Heltec</div>
                    <button id="refreshChannelsBtn" class="btn-icon">🔄 Interroga Scheda</button>
                </div>

                <!-- Form to Add/Edit Channel -->
                <div class="settings-card">
                    <div class="settings-card-title">➕ Salva / Modifica Canale</div>
                    <div class="form-grid-2">
                        <div class="form-row">
                            <label>Slot Canale (0 - 39)</label>
                            <input type="number" id="newChannelIdx" class="form-control" value="0" min="0" max="39" />
                        </div>
                        <div class="form-row">
                            <label>Nome Canale</label>
                            <input type="text" id="newChannelName" class="form-control" placeholder="es. Public, Italia, Emergenza..." />
                        </div>
                    </div>
                    <div class="form-row">
                        <label>Chiave Segreta PSK (Hex 16-byte o vuoto per Default)</label>
                        <input type="text" id="newChannelPsk" class="form-control" placeholder="Default Public PSK (lascia vuoto)" />
                    </div>
                    <button id="saveChannelBtn" class="btn-action">💾 Salva Canale nella Heltec</button>
                </div>

                <!-- List of channels -->
                <div id="channelsListContainer" style="display:flex; flex-direction:column; gap:10px;">
                    <!-- Filled dynamically -->
                </div>
            </div>
        </div>

        <!-- TAB 4: IMPOSTAZIONI RADIO & DISPOSITIVO -->
        <div id="tabSettings" class="tab-screen">
            <div class="content-scroll">
                <!-- Radio Settings -->
                <div class="settings-card">
                    <div class="settings-card-title">📻 Parametri Radio LoRa (SX1262)</div>
                    <div class="form-grid-2">
                        <div class="form-row">
                            <label>Frequenza (MHz)</label>
                            <input type="number" step="0.001" id="cfgFreq" class="form-control" value="869.618" />
                        </div>
                        <div class="form-row">
                            <label>Bandwidth (kHz)</label>
                            <select id="cfgBw" class="form-control">
                                <option value="62.5">62.5 kHz (Default EU)</option>
                                <option value="125">125 kHz</option>
                                <option value="250">250 kHz</option>
                                <option value="500">500 kHz</option>
                            </select>
                        </div>
                    </div>
                    <div class="form-grid-2">
                        <div class="form-row">
                            <label>Spreading Factor (SF)</label>
                            <select id="cfgSf" class="form-control">
                                <option value="7">SF7 (Più veloce)</option>
                                <option value="8">SF8 (Default)</option>
                                <option value="9">SF9</option>
                                <option value="10">SF10</option>
                                <option value="11">SF11</option>
                                <option value="12">SF12 (Lungo raggio)</option>
                            </select>
                        </div>
                        <div class="form-row">
                            <label>Coding Rate (CR)</label>
                            <select id="cfgCr" class="form-control">
                                <option value="5">4/5</option>
                                <option value="6">4/6</option>
                                <option value="7">4/7</option>
                                <option value="8">4/8 (Massima robustezza)</option>
                            </select>
                        </div>
                    </div>
                    <div class="form-row">
                        <label>Potenza di Trasmissione (TX Power dBm)</label>
                        <select id="cfgTxPower" class="form-control">
                            <option value="14">14 dBm (25 mW)</option>
                            <option value="17">17 dBm (50 mW)</option>
                            <option value="20">20 dBm (100 mW - Normale)</option>
                            <option value="22">22 dBm (160 mW - Massima)</option>
                        </select>
                    </div>
                    <button id="saveRadioBtn" class="btn-action">💾 Applica Parametri Radio</button>
                </div>

                <!-- Node Identity -->
                <div class="settings-card">
                    <div class="settings-card-title">🏷️ Identità Nodo & Beacon Advert</div>
                    <div class="form-row">
                        <label>Nome del Nodo</label>
                        <input type="text" id="cfgNodeName" class="form-control" value="Buscate" />
                    </div>
                    <div class="form-grid-2">
                        <div class="form-row">
                            <label>Latitudine GPS</label>
                            <input type="number" step="0.00001" id="cfgLat" class="form-control" placeholder="es. 45.526" />
                        </div>
                        <div class="form-row">
                            <label>Longitudine GPS</label>
                            <input type="number" step="0.00001" id="cfgLon" class="form-control" placeholder="es. 8.814" />
                        </div>
                    </div>
                    <div style="display:flex; gap:10px; flex-wrap:wrap;">
                        <button id="saveNodeBtn" class="btn-action" style="flex:1;">💾 Salva Nome & GPS</button>
                        <button id="sendAdvertBtn" class="btn-action btn-secondary" style="flex:1;">📢 Invia Beacon Ora</button>
                    </div>
                </div>

                <!-- Mappa Nodi & Copertura LoRa (Richiesta Utente) -->
                <div class="settings-card">
                    <div style="display:flex; align-items:center; justify-content:space-between; border-bottom:1px solid var(--border); padding-bottom:8px;">
                        <div class="settings-card-title" style="border-bottom:none; padding-bottom:0; margin:0;">
                            🗺️ Mappa Nodi LoRa
                        </div>
                        <a href="https://livemapnew.meshcoreitalia.it/" target="_blank" class="btn-icon" style="font-size:0.75rem; padding:4px 8px;" title="Apri in nuova scheda">
                            🔗 Apri MeshCore Italia ↗
                        </a>
                    </div>

                    <!-- Map Mode Selector -->
                    <div class="map-mode-tabs">
                        <button id="tabMapLiveBtn" class="map-mode-tab active" type="button">
                            🇮🇹 Live Map MeshCore Italia
                        </button>
                        <button id="tabMapLocalBtn" class="map-mode-tab" type="button">
                            📡 Nodi Locali Stazione
                        </button>
                    </div>

                    <!-- Mode 1: Live Map MeshCore Italia iframe -->
                    <div id="viewMapLive" style="display:block;">
                        <div class="map-iframe-container">
                            <iframe 
                                id="meshCoreLiveMapIframe" 
                                src="https://livemapnew.meshcoreitalia.it/" 
                                style="width:100%; height:100%; border:none;" 
                                allow="geolocation" 
                                loading="lazy">
                            </iframe>
                        </div>
                        <div class="map-stats-bar">
                            <span>Mappa live globale dei nodi MeshCore Italia con percorsi e pacchetti in tempo reale.</span>
                            <button id="reloadLiveMapBtn" class="btn-icon" style="font-size:0.75rem; padding:3px 8px;">🔄 Ricarica</button>
                        </div>
                    </div>

                    <!-- Mode 2: Local Stored Leaflet Map -->
                    <div id="viewMapLocal" style="display:none;">
                        <div id="meshMap" class="map-container"></div>
                        <div class="map-stats-bar">
                            <span id="mapNodesCount">Nodi con coordinate: 0</span>
                            <button id="mapCenterBtn" class="btn-icon" style="font-size:0.75rem; padding:3px 8px;">🎯 Centra su di me</button>
                        </div>
                    </div>
                </div>

                <!-- Trova Nodi Vicini (Discovery) -->
                <div class="settings-card">
                    <div class="settings-card-title">🔍 Trova Nodi Vicini & Scansione Mesh</div>
                    <p style="font-size:0.8rem; color:var(--text-muted);">
                        Invia un annuncio radio beacon (zero-hop) e interroga la tabella dei nodi per identificare e sincronizzare tutti i dispositivi LoRa nel raggio d'ascolto.
                    </p>
                    <div style="display:flex; gap:10px; flex-wrap:wrap; margin-top:2px;">
                        <button id="findNearbyNodesBtn" class="btn-action" style="flex:1;">📡 Trova Nodi Vicini</button>
                        <button id="getGpsLocationBtn" class="btn-action btn-secondary" style="flex:1;">📍 Usa GPS Telefono / Browser</button>
                    </div>
                </div>

                <!-- Auto-Responder / Echo Test Card -->
                <div class="settings-card">
                    <div style="display:flex; align-items:center; justify-content:space-between; border-bottom:1px solid var(--border); padding-bottom:8px;">
                        <div class="settings-card-title" style="border-bottom:none; padding-bottom:0; margin:0;">
                            🤖 Auto-Responder Radio / Echo Test
                        </div>
                        <span id="autoRespStatusBadge" class="status-badge online" style="font-size:0.75rem;">ATTIVO</span>
                    </div>
                    <p style="font-size:0.8rem; color:var(--text-muted); margin-top:8px;">
                        Risponde automaticamente a comandi radio (<code>!ping</code>, <code>!test</code>, <code>!snr</code>) calcolando SNR locale ed hop RF.
                        Include protezione anti-loop con cooldown di 60 secondi per ciascun nodo.
                    </p>
                    <div style="display:flex; gap:10px; margin-top:10px; align-items:center;">
                        <button id="toggleAutoRespBtn" class="btn-action btn-secondary" style="flex:1;" onclick="toggleAutoResponder()">
                            🔄 Attiva / Disattiva Auto-Responder
                        </button>
                    </div>
                </div>

                <!-- 24h Radio Traffic Card -->
                <div class="settings-card">
                    <div style="display:flex; align-items:center; justify-content:space-between; border-bottom:1px solid var(--border); padding-bottom:8px;">
                        <div class="settings-card-title" style="border-bottom:none; padding-bottom:0; margin:0;">
                            📈 Traffico Radio 24 Ore (Attività Mesh)
                        </div>
                        <button class="btn-icon" style="font-size:0.75rem; padding:3px 8px;" onclick="fetchHourlyTraffic()">🔄 Aggiorna</button>
                    </div>
                    <div id="trafficChartBox" class="traffic-chart-container">
                        <div id="trafficBarsRow" class="traffic-bars-row"></div>
                    </div>
                    <div style="display:flex; justify-content:space-between; font-size:0.75rem; color:var(--text-dim); margin-top:6px;">
                        <span>24h fa</span>
                        <span id="trafficTotalSummary">Caricamento traffico...</span>
                        <span>Adesso</span>
                    </div>
                </div>

                <!-- Device Maintenance -->
                <div class="settings-card">
                    <div class="settings-card-title">🛠️ Strumenti Dispositivo</div>
                    <p style="font-size:0.8rem; color:var(--text-muted);">
                        Dispositivo: <b id="cfgDevModel">Heltec V3 (ESP32-S3)</b><br>
                        Firmware: <b id="cfgDevFw">MeshCore</b>
                    </p>
                    <div style="display:flex; gap:10px; flex-wrap:wrap; margin-top:4px;">
                        <button id="syncTimeBtn" class="btn-action btn-secondary" style="flex:1;">⏱️ Sincronizza Ora</button>
                        <button id="rebootBtn" class="btn-action btn-danger" style="flex:1;">⚠️ Riavvia Heltec</button>
                    </div>
                </div>
            </div>
        </div>

    </div>

    <!-- Bottom Navigation Bar (4 Native Tabs) -->
    <nav class="bottom-nav">
        <button class="nav-item active" data-tab="tabMessages">
            <span class="nav-icon">💬</span>
            <span class="nav-label">Messaggi</span>
        </button>
        <button class="nav-item" data-tab="tabNodes">
            <span class="nav-icon">👥</span>
            <span class="nav-label">Nodi</span>
        </button>
        <button class="nav-item" data-tab="tabChannels">
            <span class="nav-icon">📻</span>
            <span class="nav-label">Canali</span>
        </button>
        <button class="nav-item" data-tab="tabSettings">
            <span class="nav-icon">⚙️</span>
            <span class="nav-label">Impostazioni</span>
        </button>
    </nav>

    <!-- Toast Notifications -->
    <div id="toastContainer" class="toast-container"></div>

    <!-- Modal Popup for Scan Results (Rich Telemetry & Distance) -->
    <div id="scanModalOverlay" class="modal-overlay">
        <div class="modal-dialog">
            <div class="modal-header">
                <div class="modal-title">
                    <span>📡</span>
                    <span>Nodi Rilevati dalla Scansione</span>
                </div>
                <button id="closeScanModalBtn" class="modal-close-btn" title="Chiudi popup">✕</button>
            </div>
            <div id="scanModalBody" class="modal-body">
                <!-- Dynamically populated with telemetry cards -->
            </div>
            <div class="modal-footer">
                <span id="scanModalSummary">Scansione completata.</span>
                <button id="closeScanModalFooterBtn" class="btn-action btn-secondary" style="padding: 6px 14px; font-size: 0.8rem;">Chiudi</button>
            </div>
        </div>
    </div>

    <script>
        // State
        let activeTab = "tabMessages";
        let activeChannelIdx = 0;
        let channels = { 0: "Public" };
        let nodeInfo = { name: "Buscate", freq_mhz: 869.618, bw_khz: 62.5, sf: 8, cr: 8, tx_power: 20 };
        let heardNodes = [];
        
        const STORAGE_KEY = "meshcore_room_messages_v3";

        function getMsgKey(m) {
            if (m.client_id) return `cid_${m.client_id}`;
            return `${m.timestamp || ''}_${m.sender || ''}_${m.content || ''}`;
        }

        function loadMessagesFromStorage() {
            let combined = [];
            const existingKeys = new Set();

            function importFromKey(key) {
                try {
                    const raw = localStorage.getItem(key);
                    if (raw) {
                        const parsed = JSON.parse(raw);
                        if (Array.isArray(parsed)) {
                            parsed.forEach(m => {
                                const k = getMsgKey(m);
                                if (!existingKeys.has(k)) {
                                    combined.push(m);
                                    existingKeys.add(k);
                                }
                            });
                        }
                    }
                } catch(e) {}
            }

            importFromKey(STORAGE_KEY);
            importFromKey("meshcore_room_messages");
            importFromKey("meshcore_chat_messages_v1");

            combined.sort((a,b) => (a.timestamp || "").localeCompare(b.timestamp || ""));
            try {
                localStorage.setItem(STORAGE_KEY, JSON.stringify(combined.slice(-500)));
            } catch(e) {}
            return combined;
        }

        function saveMessagesToStorage() {
            try {
                localStorage.setItem(STORAGE_KEY, JSON.stringify(messages.slice(-500)));
            } catch(e) {}
        }

        let messages = loadMessagesFromStorage();
        let socket = null;
        let audioEnabled = true;
        let audioCtx = null;
        let isHeltecConnected = false;
        let leafletMap = null;
        let mapMarkers = [];
        let mapTracks = [];
        let stationCoverageCircle = null;
        let localNodeMarker = null;
        let currentReplyTarget = null;
        let nodesFilter = 'all';

        function decodeHops(raw) {
            if (raw === undefined || raw === null || raw === 255 || raw === -1) return 0;
            return raw & 0x3F;
        }

        window.setNodesFilter = function(f) {
            nodesFilter = f;
            const btnAll = document.getElementById("filterNodesAll");
            const btnDirect = document.getElementById("filterNodesDirect");
            if (btnAll && btnDirect) {
                if (f === 'direct') {
                    btnAll.classList.remove("active");
                    btnDirect.classList.add("active");
                } else {
                    btnDirect.classList.remove("active");
                    btnAll.classList.add("active");
                }
            }
            renderNodes();
        };

        function setReplyTarget(m) {
            currentReplyTarget = m;
            const replyBar = document.getElementById("replyBar");
            const senderSpan = document.getElementById("replyToSender");
            const snippetSpan = document.getElementById("replyToSnippet");
            if (replyBar && senderSpan && snippetSpan) {
                const sName = m.sender || (m.source === "Web Client" ? "Tu" : "Nodo Radio");
                senderSpan.textContent = sName;
                snippetSpan.textContent = (m.content || "").substring(0, 60);
                replyBar.style.display = "flex";
                if (navigator.vibrate) {
                    try { navigator.vibrate(50); } catch(e) {}
                }
                const input = document.getElementById("messageInput");
                input.focus();
                showToast(`↩️ Rispondi a ${sName}`, true);
            }
        }

        function clearReplyTarget() {
            currentReplyTarget = null;
            const replyBar = document.getElementById("replyBar");
            if (replyBar) replyBar.style.display = "none";
        }

        // Sound Synthesizer
        function playChime(type = "recv") {
            if (!audioEnabled) return;
            try {
                if (!audioCtx) {
                    audioCtx = new (window.AudioContext || window.webkitAudioContext)();
                }
                if (audioCtx.state === "suspended") {
                    audioCtx.resume();
                }
                const osc = audioCtx.createOscillator();
                const gain = audioCtx.createGain();
                osc.connect(gain);
                gain.connect(audioCtx.destination);

                const now = audioCtx.currentTime;
                if (type === "recv") {
                    // Two-tone bell for received message
                    osc.type = "sine";
                    osc.frequency.setValueAtTime(587.33, now); // D5
                    osc.frequency.setValueAtTime(880, now + 0.08); // A5
                    gain.gain.setValueAtTime(0.12, now);
                    gain.gain.exponentialRampToValueAtTime(0.001, now + 0.35);
                    osc.start(now);
                    osc.stop(now + 0.35);
                } else if (type === "ack") {
                    // Triple rising chime for delivery ACK confirmation
                    osc.type = "sine";
                    osc.frequency.setValueAtTime(659.25, now); // E5
                    osc.frequency.setValueAtTime(880.00, now + 0.07); // A5
                    osc.frequency.setValueAtTime(1318.51, now + 0.15); // E6
                    gain.gain.setValueAtTime(0.18, now);
                    gain.gain.exponentialRampToValueAtTime(0.001, now + 0.45);
                    osc.start(now);
                    osc.stop(now + 0.45);
                    if (navigator.vibrate) {
                        try { navigator.vibrate([70, 30, 70]); } catch(e) {}
                    }
                } else if (type === "radar") {
                    // Sonar / Radar high ping for direct RF detection
                    osc.type = "sine";
                    osc.frequency.setValueAtTime(987.77, now); // B5
                    osc.frequency.exponentialRampToValueAtTime(1479.98, now + 0.12); // F#6
                    gain.gain.setValueAtTime(0.22, now);
                    gain.gain.exponentialRampToValueAtTime(0.001, now + 0.55);
                    osc.start(now);
                    osc.stop(now + 0.55);
                    if (navigator.vibrate) {
                        try { navigator.vibrate([120, 60, 120, 60, 200]); } catch(e) {}
                    }
                }
            } catch(e) {
                console.warn("Audio chime error:", e);
            }
        }

        // PWA Service Worker & Push Notifications
        if ('serviceWorker' in navigator) {
            window.addEventListener('load', () => {
                navigator.serviceWorker.register('/sw.js').catch(err => console.log('SW reg error:', err));
            });
        }

        function togglePushNotifications() {
            if (!("Notification" in window)) {
                showToast("Notifiche non supportate dal tuo browser.", false);
                return;
            }
            if (Notification.permission === "granted") {
                showToast("🔔 Notifiche push browser già attive!", true);
                new Notification("MeshCore Web App", {
                    body: "Le notifiche LoRa in tempo reale sono attive sul tuo dispositivo.",
                    icon: "/api/icon.svg"
                });
            } else if (Notification.permission !== "denied") {
                Notification.requestPermission().then(permission => {
                    if (permission === "granted") {
                        showToast("🔔 Notifiche push attivate con successo!", true);
                        new Notification("MeshCore Web App", {
                            body: "Notifiche attivate! Riceverai avvisi quando arrivano messaggi o nodi vicini.",
                            icon: "/api/icon.svg"
                        });
                    } else {
                        showToast("Notifiche rifiutate dal browser.", false);
                    }
                });
            } else {
                showToast("Notifiche bloccate nelle impostazioni del browser.", false);
            }
        }

        // Chat Live Search Filter
        let chatSearchQuery = "";

        function handleChatSearch() {
            const input = document.getElementById("chatSearchInput");
            const clearBtn = document.getElementById("clearSearchBtn");
            if (!input) return;
            chatSearchQuery = input.value.trim().toLowerCase();
            if (clearBtn) {
                clearBtn.style.display = chatSearchQuery ? "inline-block" : "none";
            }
            renderMessages();
        }

        function clearChatSearch() {
            const input = document.getElementById("chatSearchInput");
            const clearBtn = document.getElementById("clearSearchBtn");
            if (input) input.value = "";
            if (clearBtn) clearBtn.style.display = "none";
            chatSearchQuery = "";
            renderMessages();
        }

        // Share Current GPS in Chat
        function shareGpsLocationInChat() {
            if (!navigator.geolocation) {
                showToast("Geolocalizzazione non supportata.", false);
                return;
            }
            showToast("Rilevamento posizione GPS...", true);
            navigator.geolocation.getCurrentPosition(
                (pos) => {
                    const lat = pos.coords.latitude.toFixed(5);
                    const lon = pos.coords.longitude.toFixed(5);
                    const mapLink = `https://maps.google.com/?q=${lat},${lon}`;
                    const input = document.getElementById("messageInput");
                    input.value = `📍 GPS: ${lat}, ${lon} (${mapLink})`;
                    input.focus();
                    showToast("Coordinate inserite nella barra chat! Premi Invia per trasmettere.", true);
                },
                (err) => {
                    showToast(`Errore GPS: ${err.message}`, false);
                },
                { enableHighAccuracy: true, timeout: 8000 }
            );
        }

        // Direct Message helper
        function openDirectMessage(nodeName) {
            const input = document.getElementById("messageInput");
            if (!input) return;
            input.value = `@${nodeName} `;
            document.querySelector('[data-tab="tabMessages"]').click();
            input.focus();
            showToast(`Modalità messaggio diretto per ${nodeName}`, true);
        }

        function escapeJsString(str) {
            if (!str) return "";
            return String(str).replace(/'/g, "\\'").replace(/"/g, "&quot;");
        }

        // 24h Radio Traffic & Auto-Responder Status
        async function fetchHourlyTraffic() {
            try {
                const resp = await fetch("/api/stats/hourly");
                if (!resp.ok) return;
                const data = await resp.json();
                const hourly = Array.isArray(data) ? data : (data.hourly || []);
                const container = document.getElementById("trafficBarsRow");
                if (!container) return;
                container.innerHTML = "";

                if (hourly.length === 0) {
                    container.innerHTML = '<div style="margin:auto; font-size:0.75rem; color:var(--text-dim); text-align:center;">Nessun pacchetto registrato nelle ultime 24 ore.</div>';
                    const summaryEl = document.getElementById("trafficTotalSummary");
                    if (summaryEl) summaryEl.textContent = "0 pacchetti (24h)";
                    return;
                }

                const maxCount = Math.max(...hourly.map(h => h.count), 1);
                let totalPackets = data.total !== undefined ? data.total : 0;

                hourly.forEach(item => {
                    if (data.total === undefined) totalPackets += item.count;
                    const barCol = document.createElement("div");
                    barCol.className = "traffic-bar-col";
                    const heightPercent = Math.max(Math.round((item.count / maxCount) * 100), item.count > 0 ? 8 : 2);
                    barCol.title = `Ore ${item.hour}:00 - ${item.count} pacchetti`;

                    barCol.innerHTML = `
                        <div class="traffic-bar-fill" style="height:${heightPercent}%;"></div>
                        <div class="traffic-bar-hour">${item.hour}</div>
                    `;
                    container.appendChild(barCol);
                });

                const summaryEl = document.getElementById("trafficTotalSummary");
                if (summaryEl) summaryEl.textContent = `Totale: ${totalPackets} pacchetti (24h)`;
            } catch(e) {
                console.warn("Traffic fetch error:", e);
            }
        }

        async function fetchAutoResponderStatus() {
            try {
                const resp = await fetch("/api/autoresponder");
                if (!resp.ok) return;
                const data = await resp.json();
                updateAutoResponderBadge(data.auto_responder_enabled !== undefined ? data.auto_responder_enabled : data.enabled);
            } catch(e) {}
        }

        function updateAutoResponderBadge(enabled) {
            const badge = document.getElementById("autoRespStatusBadge");
            const btn = document.getElementById("toggleAutoRespBtn");
            if (!badge || !btn) return;
            if (enabled) {
                badge.className = "status-badge online";
                badge.textContent = "ATTIVO";
                btn.textContent = "⏹️ Disattiva Auto-Responder";
            } else {
                badge.className = "status-badge offline";
                badge.textContent = "DISATTIVATO";
                btn.textContent = "▶️ Attiva Auto-Responder";
            }
        }

        async function toggleAutoResponder() {
            try {
                const currResp = await fetch("/api/autoresponder");
                const currData = await currResp.json();
                const newState = !currData.auto_responder_enabled;
                const postResp = await fetch("/api/autoresponder", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ enabled: newState })
                });
                const postData = await postResp.json();
                updateAutoResponderBadge(postData.auto_responder_enabled);
                showToast(`Auto-responder radio ${postData.auto_responder_enabled ? 'attivato' : 'disattivato'}!`, true);
            } catch(e) {
                showToast("Errore aggiornamento auto-responder", false);
            }
        }

        // Toasts
        function showToast(text, isGood = true) {
            const container = document.getElementById("toastContainer");
            const bubble = document.createElement("div");
            bubble.className = "toast-bubble";
            bubble.innerHTML = `<span>${isGood ? "✅" : "ℹ️"}</span><span>${escapeHtml(text)}</span>`;
            container.appendChild(bubble);
            setTimeout(() => {
                bubble.style.opacity = "0";
                bubble.style.transition = "opacity 0.3s ease";
                setTimeout(() => bubble.remove(), 300);
            }, 3500);
        }

        // Tab Switching
        document.querySelectorAll(".nav-item").forEach(btn => {
            btn.addEventListener("click", () => {
                const targetTab = btn.getAttribute("data-tab");
                document.querySelectorAll(".nav-item").forEach(b => b.classList.remove("active"));
                btn.classList.add("active");

                document.querySelectorAll(".tab-screen").forEach(s => s.classList.remove("active"));
                const activeScreen = document.getElementById(targetTab);
                if (activeScreen) activeScreen.classList.add("active");
                activeTab = targetTab;

                if (targetTab === "tabMessages") {
                    scrollChatToBottom();
                } else if (targetTab === "tabSettings") {
                    setTimeout(() => {
                        initOrUpdateMap();
                    }, 100);
                }
            });
        });

        // Interactive Map Logic
        function initOrUpdateMap() {
            const mapContainer = document.getElementById("meshMap");
            if (!mapContainer || typeof L === "undefined") return;

            // Default fallback center: Italy / Milan area (Buscate: 45.54, 8.81)
            let centerLat = (nodeInfo && nodeInfo.lat) ? Number(nodeInfo.lat) : 45.543;
            let centerLon = (nodeInfo && nodeInfo.lon) ? Number(nodeInfo.lon) : 8.815;

            if (!leafletMap) {
                leafletMap = L.map("meshMap", {
                    center: [centerLat, centerLon],
                    zoom: 12,
                    zoomControl: true
                });

                // Dark / CartoDB dark tile layer
                L.tileLayer("https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png", {
                    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
                    subdomains: "abcd",
                    maxZoom: 19
                }).addTo(leafletMap);
            } else {
                leafletMap.invalidateSize();
            }

            renderMapMarkers();
        }

        function renderMapMarkers() {
            if (!leafletMap || typeof L === "undefined") return;

            // Clear old markers, tracks and circles
            mapMarkers.forEach(m => leafletMap.removeLayer(m));
            mapMarkers = [];
            mapTracks.forEach(t => leafletMap.removeLayer(t));
            mapTracks = [];
            if (stationCoverageCircle) {
                leafletMap.removeLayer(stationCoverageCircle);
                stationCoverageCircle = null;
            }
            if (localNodeMarker) {
                leafletMap.removeLayer(localNodeMarker);
                localNodeMarker = null;
            }

            let validCoordsCount = 0;
            const bounds = [];

            // Add local node marker
            const myLat = (nodeInfo && nodeInfo.lat) ? Number(nodeInfo.lat) : null;
            const myLon = (nodeInfo && nodeInfo.lon) ? Number(nodeInfo.lon) : null;
            const myName = (nodeInfo && nodeInfo.name) ? nodeInfo.name : "Stazione Heltec";

            if (myLat && myLon) {
                const homeIcon = L.divIcon({
                    className: "local-pin",
                    html: `<div style="background:#10b981; color:#022c22; font-size:1.1rem; width:30px; height:30px; border-radius:50%; display:flex; align-items:center; justify-content:center; border:2px solid white; box-shadow:0 0 12px rgba(16,185,129,0.8);">📡</div>`,
                    iconSize: [30, 30],
                    iconAnchor: [15, 15]
                });
                localNodeMarker = L.marker([myLat, myLon], { icon: homeIcon }).addTo(leafletMap);
                localNodeMarker.bindPopup(`
                    <div style="font-size:0.88rem; line-height:1.4;">
                        <b style="color:#10b981; font-size:0.95rem;">📡 ${escapeHtml(myName)} (Tu)</b><br>
                        <span>Frequenza: <b>${nodeInfo.freq_mhz || 869.618} MHz</b></span><br>
                        <span>Potenza TX: <b>${nodeInfo.tx_power || 20} dBm</b></span><br>
                        <small style="color:#94a3b8;">Coordinate: ${myLat.toFixed(5)}, ${myLon.toFixed(5)}</small>
                    </div>
                `);
                bounds.push([myLat, myLon]);

                // Station Estimated RF Coverage Circle (~25 km)
                stationCoverageCircle = L.circle([myLat, myLon], {
                    radius: 25000,
                    color: '#10b981',
                    fillColor: '#10b981',
                    fillOpacity: 0.05,
                    weight: 1.5,
                    dashArray: '6, 6'
                }).addTo(leafletMap);
                stationCoverageCircle.bindPopup(`
                    <div style="font-size:0.85rem; line-height:1.4;">
                        <b style="color:#10b981;">📡 Raggio di Copertura RF Stimato (~25 km)</b><br>
                        <span>Stazione: <b>${escapeHtml(myName)}</b></span><br>
                        <small style="color:#94a3b8;">Portata ottica/diretta tipica per antenna stazione Buscate</small>
                    </div>
                `);
            }

            // Add heard nodes markers
            heardNodes.forEach(n => {
                const nLat = n.lat ? Number(n.lat) : null;
                const nLon = n.lon ? Number(n.lon) : null;
                if (nLat && nLon) {
                    validCoordsCount++;
                    const nodeIcon = L.divIcon({
                        className: "node-pin",
                        html: `<div style="background:#0284c7; color:white; font-size:0.95rem; width:26px; height:26px; border-radius:50%; display:flex; align-items:center; justify-content:center; border:2px solid white; box-shadow:0 0 8px rgba(2,132,199,0.7);">📍</div>`,
                        iconSize: [26, 26],
                        iconAnchor: [13, 13]
                    });
                    const marker = L.marker([nLat, nLon], { icon: nodeIcon }).addTo(leafletMap);
                    const snrStr = n.last_snr !== null && n.last_snr !== undefined ? ` • SNR: ${n.last_snr > 0 ? '+' : ''}${Number(n.last_snr).toFixed(1)}dB` : '';
                    const hops = decodeHops(n.last_hops);
                    const hopsStr = hops === 0 ? "🎯 Diretto RF" : `🔀 ${hops} salti`;
                    marker.bindPopup(`
                        <div style="font-size:0.86rem; line-height:1.4;">
                            <b style="color:#38bdf8; font-size:0.92rem;">👤 ${escapeHtml(n.node_name)}</b><br>
                            <span>Canale: <b>${escapeHtml(n.last_channel || 'Radio')}</b></span><br>
                            <span>Percorso: <b>${hopsStr}${snrStr}</b></span><br>
                            <small style="color:#94a3b8;">Visto: ${n.last_seen || 'N/A'}</small>
                        </div>
                    `);
                    mapMarkers.push(marker);
                    bounds.push([nLat, nLon]);
                }
            });

            // Fetch and render GPS historical tracks
            fetch("/api/nodes/tracks")
                .then(r => r.json())
                .then(data => {
                    if (!data || !data.tracks || !leafletMap) return;
                    const colors = ["#38bdf8", "#a855f7", "#f59e0b", "#ec4899", "#10b981", "#06b6d4"];
                    let cIdx = 0;
                    for (const [nodeName, pts] of Object.entries(data.tracks)) {
                        if (pts && pts.length > 1) {
                            const latlngs = pts.map(p => [p.lat, p.lon]);
                            const trackColor = colors[cIdx % colors.length];
                            cIdx++;
                            const poly = L.polyline(latlngs, {
                                color: trackColor,
                                weight: 3,
                                opacity: 0.8,
                                dashArray: '4, 4'
                            }).addTo(leafletMap);
                            poly.bindTooltip(`📍 Traccia GPS: <b>${escapeHtml(nodeName)}</b> (${pts.length} rilevamenti)`, { sticky: true });
                            mapTracks.push(poly);
                        }
                    }
                })
                .catch(() => {});

            document.getElementById("mapNodesCount").textContent = `Nodi con coordinate GPS: ${validCoordsCount}`;

            if (bounds.length > 0) {
                try {
                    leafletMap.fitBounds(bounds, { padding: [40, 40], maxZoom: 14 });
                } catch(e) {}
            }
        }

        // Haversine Distance Calculation between two GPS coordinates in Kilometers
        function calculateDistanceKm(lat1, lon1, lat2, lon2) {
            if (lat1 === null || lon1 === null || lat2 === null || lon2 === null) return null;
            const R = 6371; // Radius of earth in km
            const dLat = (lat2 - lat1) * Math.PI / 180;
            const dLon = (lon2 - lon1) * Math.PI / 180;
            const a = 
                Math.sin(dLat/2) * Math.sin(dLat/2) +
                Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * 
                Math.sin(dLon/2) * Math.sin(dLon/2);
            const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
            return R * c;
        }

        // Show Scan Results Modal Dialog
        function showScanResultsModal(nodesList, isScanning = false) {
            const overlay = document.getElementById("scanModalOverlay");
            const body = document.getElementById("scanModalBody");
            const summary = document.getElementById("scanModalSummary");
            if (!overlay || !body) return;

            // Make sure overlay is visible immediately
            overlay.style.display = "flex";
            // Trigger animation frame so CSS transition fires
            requestAnimationFrame(() => {
                overlay.classList.add("active");
            });

            if (isScanning) {
                summary.textContent = "Scansione radio LoRa in corso...";
                body.innerHTML = `
                    <div style="text-align: center; color: var(--text-muted); padding: 36px 12px;">
                        <div style="font-size: 2.6rem; animation: pulseGlow 1.5s infinite ease-in-out; margin-bottom: 12px;">📡</div>
                        <b style="color: var(--text-main); font-size: 1.05rem;">Scansione radio LoRa in corso...</b>
                        <div style="font-size: 0.82rem; margin-top: 8px; color: var(--primary);">
                            Invio beacon zero-hop e sincronizzazione tabella contatti...
                        </div>
                        <div style="margin-top: 16px; display: flex; justify-content: center;">
                            <div class="pulse-dot" style="background: var(--primary); width: 14px; height: 14px; border-radius: 50%;"></div>
                        </div>
                    </div>
                `;
                return;
            }

            body.innerHTML = "";
            const list = (nodesList && nodesList.length > 0) ? nodesList : heardNodes;

            if (!list || list.length === 0) {
                body.innerHTML = `
                    <div style="text-align: center; color: var(--text-muted); padding: 30px 10px;">
                        <div style="font-size: 2.2rem; margin-bottom: 8px;">📡</div>
                        <b style="color: var(--text-main);">Nessun nodo rilevato nelle immediate vicinanze.</b>
                        <div style="font-size: 0.8rem; margin-top: 6px; color: var(--text-dim);">
                            I beacon radio zero-hop sono stati inviati. Assicurati che altri nodi LoRa siano accesi sullo stesso canale e frequenza (${nodeInfo.freq_mhz || 869.618} MHz).
                        </div>
                    </div>
                `;
                summary.textContent = "Scansione terminata (0 nodi).";
                return;
            }

            const myLat = (nodeInfo && nodeInfo.lat) ? Number(nodeInfo.lat) : null;
            const myLon = (nodeInfo && nodeInfo.lon) ? Number(nodeInfo.lon) : null;

            list.forEach(n => {
                const card = document.createElement("div");
                card.className = "scan-node-card";

                const nLat = (n.lat !== null && n.lat !== undefined) ? Number(n.lat) : null;
                const nLon = (n.lon !== null && n.lon !== undefined) ? Number(n.lon) : null;

                // Compute distance
                let distStr = "Non disp. (senza GPS)";
                let distNum = null;
                if (myLat && myLon && nLat && nLon) {
                    const d = calculateDistanceKm(myLat, myLon, nLat, nLon);
                    if (d !== null) {
                        distNum = d;
                        distStr = d < 1 ? `${Math.round(d * 1000)} metri` : `${d.toFixed(2)} km`;
                    }
                } else if (!myLat || !myLon) {
                    distStr = "Imposta GPS stazione per calcolare";
                }

                const snrVal = (n.last_snr !== null && n.last_snr !== undefined) ? `${n.last_snr > 0 ? '+' : ''}${Number(n.last_snr).toFixed(1)} dB` : "N/A";
                const hops = decodeHops(n.last_hops);
                const hopsVal = hops === 0 ? "🎯 0 (Diretto RF)" : `🔀 ${hops} ${hops === 1 ? 'salto' : 'salti'}`;
                const packetsVal = n.packets_count || 1;
                const lastSeenVal = n.last_seen ? n.last_seen : "Adesso";
                const channelVal = n.last_channel || "Radio LoRa";

                card.innerHTML = `
                    <div class="scan-node-top">
                        <div class="scan-node-name">
                            <span>📻</span>
                            <span>${escapeHtml(n.node_name)}</span>
                        </div>
                        <span class="node-badge" style="font-size:0.75rem;">${escapeHtml(channelVal)}</span>
                    </div>
                    <div class="scan-telemetry-grid">
                        <div class="telemetry-item">
                            <span class="telemetry-label">📏 Distanza</span>
                            <span class="telemetry-val" style="color:${distNum ? '#10b981' : 'var(--text-muted)'}; font-weight:700;">${distStr}</span>
                        </div>
                        <div class="telemetry-item">
                            <span class="telemetry-label">📶 Segnale SNR</span>
                            <span class="telemetry-val">${snrVal}</span>
                        </div>
                        <div class="telemetry-item">
                            <span class="telemetry-label">🔀 Salti / Hops</span>
                            <span class="telemetry-val">${hopsVal}</span>
                        </div>
                        <div class="telemetry-item">
                            <span class="telemetry-label">📦 Pacchetti RX</span>
                            <span class="telemetry-val">${packetsVal}</span>
                        </div>
                        <div class="telemetry-item">
                            <span class="telemetry-label">📍 Coordinate GPS</span>
                            <span class="telemetry-val">${nLat && nLon ? `${nLat.toFixed(4)}, ${nLon.toFixed(4)}` : 'Non fornite'}</span>
                        </div>
                        <div class="telemetry-item">
                            <span class="telemetry-label">⏱️ Ultimo Segnale</span>
                            <span class="telemetry-val">${escapeHtml(lastSeenVal)}</span>
                        </div>
                    </div>
                `;
                body.appendChild(card);
            });

            summary.textContent = `Trovati ${list.length} nodi con telemetria.`;
        }

        function closeScanResultsModal() {
            const overlay = document.getElementById("scanModalOverlay");
            if (overlay) {
                overlay.classList.remove("active");
                setTimeout(() => {
                    if (!overlay.classList.contains("active")) {
                        overlay.style.display = "none";
                    }
                }, 200);
            }
        }

        // Audio Toggle
        const audioBtn = document.getElementById("audioToggleBtn");
        audioBtn.addEventListener("click", () => {
            audioEnabled = !audioEnabled;
            audioBtn.textContent = audioEnabled ? "🔔" : "🔕";
            showToast(audioEnabled ? "Notifiche audio attivate" : "Notifiche audio disattivate", audioEnabled);
        });

        // Update Top Header Display
        function updateHeader() {
            const badge = document.getElementById("connBadge");
            const badgeText = document.getElementById("connBadgeText");
            if (isHeltecConnected) {
                badge.className = "status-badge online";
                badgeText.textContent = "CONNESSA";
            } else {
                badge.className = "status-badge offline";
                badgeText.textContent = "OFFLINE";
            }
            document.getElementById("nodeNameDisplay").textContent = nodeInfo.name || "Buscate";
            document.getElementById("radioFreqDisplay").textContent = (nodeInfo.freq_mhz || 869.618) + " MHz";
        }

        // Render Channel Chips
        async function fetchHistoryForChannel(channelName, channelIdx) {
            try {
                const res = await fetch(`/api/messages?limit=150&channel=${encodeURIComponent(channelName)}`);
                if (res.ok) {
                    const serverMsgs = await res.json();
                    if (serverMsgs && serverMsgs.length > 0) {
                        const existingKeys = new Set(messages.map(getMsgKey));
                        let added = 0;
                        serverMsgs.forEach(m => {
                            const key = getMsgKey(m);
                            if (!existingKeys.has(key)) {
                                if (m.channel_idx === undefined || m.channel_idx === null) m.channel_idx = channelIdx;
                                messages.push(m);
                                existingKeys.add(key);
                                added++;
                            }
                        });
                        if (added > 0) {
                            messages.sort((a,b) => (a.timestamp || "").localeCompare(b.timestamp || ""));
                            saveMessagesToStorage();
                            renderMessages();
                        }
                    }
                }
            } catch(e) {
                console.warn("fetchHistory error:", e);
            }
        }

        function renderChannelsBar() {
            const bar = document.getElementById("channelsNavBar");
            bar.innerHTML = "";

            // "Tutti i Canali" Chip
            const allChip = document.createElement("button");
            allChip.className = `channel-chip ${activeChannelIdx === -1 ? "active" : ""}`;
            allChip.innerHTML = `<span>🌐 Tutti</span> <span class="chip-badge">${messages.length}</span>`;
            allChip.addEventListener("click", () => {
                activeChannelIdx = -1;
                document.getElementById("currentChannelName").textContent = `🌐 Tutti i Canali`;
                renderChannelsBar();
                renderMessages();
            });
            bar.appendChild(allChip);

            const keys = Object.keys(channels).map(Number).sort((a,b) => a - b);
            keys.forEach(idx => {
                const name = channels[idx];
                const count = messages.filter(m => {
                    if (m.channel_idx !== undefined && m.channel_idx !== null) {
                        return Number(m.channel_idx) === Number(idx);
                    }
                    return m.channel === name || m.channel === `Canale ${idx}` || m.channel === `Canale #${idx}`;
                }).length;

                const chip = document.createElement("button");
                chip.className = `channel-chip ${idx === activeChannelIdx ? "active" : ""}`;
                chip.innerHTML = `<span>[${idx}] ${name}</span> <span class="chip-badge">${count}</span>`;
                chip.addEventListener("click", () => {
                    activeChannelIdx = idx;
                    document.getElementById("currentChannelName").textContent = `${name} [${idx}]`;
                    renderChannelsBar();
                    renderMessages();
                    fetchHistoryForChannel(name, idx);
                });
                bar.appendChild(chip);
            });

            if (activeChannelIdx === -1) {
                document.getElementById("currentChannelName").textContent = `🌐 Tutti i Canali`;
            } else {
                document.getElementById("currentChannelName").textContent = `${channels[activeChannelIdx] || "Canale"} [${activeChannelIdx}]`;
            }
        }

        // Render Messages List
        function renderMessages() {
            const container = document.getElementById("chatMessagesScroll");
            const chTitle = activeChannelIdx === -1 ? "🌐 Tutti i Canali" : `Canale [${activeChannelIdx}] ${channels[activeChannelIdx] || ""}`;
            container.innerHTML = `<div class="chat-date-separator"><span>Oggi • ${chTitle}</span></div>`;

            let filtered = activeChannelIdx === -1 ? messages : messages.filter(m => {
                if (m.channel_idx !== undefined && m.channel_idx !== null) {
                    return Number(m.channel_idx) === Number(activeChannelIdx);
                }
                const chName = channels[activeChannelIdx];
                return m.channel === chName || m.channel === `Canale ${activeChannelIdx}` || m.channel === `Canale #${activeChannelIdx}`;
            });

            if (chatSearchQuery) {
                filtered = filtered.filter(m => {
                    const c = (m.content || "").toLowerCase();
                    const s = (m.sender || "").toLowerCase();
                    return c.includes(chatSearchQuery) || s.includes(chatSearchQuery);
                });
                container.innerHTML = `<div class="chat-date-separator"><span>🔍 Risultati per "${escapeHtml(chatSearchQuery)}" (${filtered.length} trovati)</span></div>`;
            }

            if (filtered.length === 0) {
                const empty = document.createElement("div");
                empty.style.cssText = "text-align: center; color: var(--text-dim); margin-top: 40px; font-size: 0.85rem;";
                empty.innerHTML = chatSearchQuery
                    ? `Nessun messaggio trovato per "${escapeHtml(chatSearchQuery)}".<br><button onclick="clearChatSearch()" class="btn-icon" style="margin-top:8px;">Azzera Ricerca</button>`
                    : `Nessun messaggio su questo canale.<br>Invia un messaggio per trasmettere via radio LoRa.`;
                container.appendChild(empty);
                return;
            }

            filtered.forEach(m => {
                const isOut = m.source === "Web Client" || m.source === "Web API" || m.source === "Telegram";
                const wrap = document.createElement("div");
                wrap.className = `msg-bubble-wrap ${isOut ? "outgoing" : "incoming"}`;

                // Sender Header
                const senderDiv = document.createElement("div");
                senderDiv.className = "msg-sender-name";
                let senderText = m.sender || (isOut ? "Tu" : "Nodo Radio");
                if (activeChannelIdx === -1 && m.channel) {
                    senderDiv.innerHTML = `${escapeHtml(senderText)} <span class="node-badge" style="font-size:0.7rem; font-weight:normal; margin-left:6px; padding:1px 6px;">${escapeHtml(m.channel)}</span>`;
                } else {
                    senderDiv.textContent = senderText;
                }
                wrap.appendChild(senderDiv);

                // Bubble Body
                const bubble = document.createElement("div");
                bubble.className = "msg-bubble";

                // Render quoted reply if present
                if (m.reply_to_sender) {
                    const quoteDiv = document.createElement("div");
                    quoteDiv.className = "msg-quote";
                    quoteDiv.innerHTML = `<div class="msg-quote-sender">↩️ ${escapeHtml(m.reply_to_sender)}</div><div class="msg-quote-text">${escapeHtml(m.reply_to_text || '')}</div>`;
                    bubble.appendChild(quoteDiv);
                }

                const contentSpan = document.createElement("span");
                contentSpan.textContent = m.content;
                bubble.appendChild(contentSpan);
                wrap.appendChild(bubble);

                // Long-press and click listeners to reply directly
                let pressTimer = null;
                let touchMoved = false;

                wrap.addEventListener("touchstart", (e) => {
                    touchMoved = false;
                    pressTimer = setTimeout(() => {
                        if (!touchMoved) {
                            setReplyTarget(m);
                        }
                    }, 400);
                }, { passive: true });

                wrap.addEventListener("touchmove", () => {
                    touchMoved = true;
                    if (pressTimer) clearTimeout(pressTimer);
                }, { passive: true });

                wrap.addEventListener("touchend", () => {
                    if (pressTimer) clearTimeout(pressTimer);
                });

                wrap.addEventListener("touchcancel", () => {
                    if (pressTimer) clearTimeout(pressTimer);
                });

                wrap.addEventListener("contextmenu", (e) => {
                    e.preventDefault();
                    setReplyTarget(m);
                });

                wrap.addEventListener("dblclick", () => {
                    setReplyTarget(m);
                });

                // Footer (Time, Signal/Hops, ACK)
                const footer = document.createElement("div");
                footer.className = "msg-footer";

                const timeStr = m.timestamp ? m.timestamp.substring(11, 16) : "";
                let details = `<span>${timeStr}</span>`;

                if (m.snr !== undefined && m.snr !== null) {
                    details += `<span>• SNR: ${m.snr > 0 ? '+' : ''}${Number(m.snr).toFixed(1)}dB</span>`;
                }
                if (m.hops !== undefined && m.hops !== null && !isOut) {
                    const hops = decodeHops(m.hops);
                    const hopsLabel = hops === 0 ? "🎯 Diretto RF" : `🔀 ${hops} ${hops === 1 ? 'salto' : 'salti'}`;
                    details += `<span>• ${hopsLabel}</span>`;
                }

                if (isOut) {
                    const status = m.ack_status || (m.sent_to_radio ? "sent_to_radio" : "pending");
                    if (status === "confirmed") {
                        const rtt = m.rtt_ms ? ` • ${m.rtt_ms}ms` : "";
                        let hopInfo = "";
                        if (m.hops !== undefined && m.hops !== null) {
                            const hops = decodeHops(m.hops);
                            hopInfo = hops === 0 ? " • 🎯 Diretto RF" : ` • 🔀 ${hops} ${hops === 1 ? 'salto' : 'salti'}`;
                        }
                        details += `<span class="ack-indicator confirmed">✓✓ RECAPITATO${hopInfo}${rtt}</span>`;
                    } else if (status === "air" || status === "transmitted") {
                        details += `<span class="ack-indicator air">✓ Trasmesso in etere LoRa</span>`;
                    } else if (status === "sent_to_radio") {
                        details += `<span class="ack-indicator pending">✓ Inviato alla radio</span>`;
                    } else {
                        details += `<span class="ack-indicator pending">⏳ In trasmissione...</span>`;
                    }
                }

                footer.innerHTML = details;
                wrap.appendChild(footer);
                container.appendChild(wrap);
            });

            scrollChatToBottom();
        }

        function scrollChatToBottom() {
            const container = document.getElementById("chatMessagesScroll");
            container.scrollTop = container.scrollHeight;
        }

        // Render Nodes Tab
        function renderNodes() {
            const container = document.getElementById("nodesListContainer");
            container.innerHTML = "";

            const totalCount = heardNodes.length;
            const directNodes = heardNodes.filter(n => decodeHops(n.last_hops) === 0);
            const directCount = directNodes.length;

            const countEl = document.getElementById("nodesCount");
            if (countEl) countEl.textContent = totalCount;
            const countAllEl = document.getElementById("nodesCountAll");
            if (countAllEl) countAllEl.textContent = totalCount;
            const countDirectEl = document.getElementById("nodesCountDirect");
            if (countDirectEl) countDirectEl.textContent = directCount;

            const listToDisplay = (nodesFilter === 'direct') ? directNodes : heardNodes;

            if (listToDisplay.length === 0) {
                const emptyMsg = (nodesFilter === 'direct')
                    ? "Nessun nodo ascoltato direttamente via RF (0 salti) finora.<br>Tutti i nodi memorizzati sono stati ricevuti tramite ripetitori o percorsi mesh."
                    : "Nessun nodo radio ascoltato finora.<br>I nodi compariranno automaticamente non appena trasmetteranno pacchetti o beacon.";
                container.innerHTML = `<div style="text-align:center; color:var(--text-dim); padding:30px;">${emptyMsg}</div>`;
                return;
            }

            listToDisplay.forEach(n => {
                const card = document.createElement("div");
                card.className = "node-card";

                const icon = n.lat && n.lon ? "📍" : "📻";
                const snrText = n.last_snr !== null && n.last_snr !== undefined ? `SNR: ${n.last_snr > 0 ? '+' : ''}${Number(n.last_snr).toFixed(1)}dB` : "";
                const hops = decodeHops(n.last_hops);
                const isDirect = (hops === 0);
                const hopsBadge = isDirect
                    ? `<span style="background:rgba(16, 185, 129, 0.15); color:#10b981; padding:2px 8px; border-radius:12px; font-weight:700; font-size:0.72rem;">🎯 Diretto RF (0 salti)</span>`
                    : `<span style="background:rgba(245, 158, 11, 0.15); color:#f59e0b; padding:2px 8px; border-radius:12px; font-weight:600; font-size:0.72rem;">🔀 ${hops} ${hops === 1 ? 'salto' : 'salti'}</span>`;
                const timeText = n.last_seen ? n.last_seen.substring(11, 16) : "";

                card.innerHTML = `
                    <div class="node-avatar">${icon}</div>
                    <div class="node-info-col">
                        <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
                            <span class="node-name-text">${escapeHtml(n.node_name)}</span>
                            ${hopsBadge}
                        </div>
                        <div class="node-sub-text">
                            <span>⏱️ ${timeText}</span>
                            ${snrText ? `<span>• ${snrText}</span>` : ''}
                            <span>• ${n.packets_count || 1} pkt RX</span>
                        </div>
                    </div>
                    <div class="node-actions-col">
                        <span class="node-badge">${escapeHtml(n.last_channel || 'Radio')}</span>
                        <button class="btn-icon" style="padding:3px 8px; font-size:0.7rem; color:var(--accent-blue);" onclick="openDirectMessage('${escapeJsString(n.node_name)}')">💬 DM</button>
                        ${n.lat && n.lon ? `<a href="https://www.openstreetmap.org/?mlat=${n.lat}&mlon=${n.lon}#map=14/${n.lat}/${n.lon}" target="_blank" class="btn-icon" style="padding:3px 8px; font-size:0.7rem;">Mappa</a>` : ''}
                    </div>
                `;
                container.appendChild(card);
            });
        }

        // Render Channels Tab
        function renderChannelsSettings() {
            const container = document.getElementById("channelsListContainer");
            container.innerHTML = "";
            const keys = Object.keys(channels).map(Number).sort((a,b) => a - b);
            keys.forEach(idx => {
                const name = channels[idx];
                const card = document.createElement("div");
                card.className = "channel-card";
                card.innerHTML = `
                    <div class="channel-card-top">
                        <div style="display:flex; align-items:center; gap:8px;">
                            <span class="channel-slot-badge">Slot #${idx}</span>
                            <b style="font-size:0.95rem;">${escapeHtml(name)}</b>
                        </div>
                        <button class="btn-icon" onclick="selectAndChat(${idx})">💬 Apri Chat</button>
                    </div>
                `;
                container.appendChild(card);
            });
        }

        window.selectAndChat = function(idx) {
            activeChannelIdx = idx;
            renderChannelsBar();
            renderMessages();
            document.querySelector('[data-tab="tabMessages"]').click();
        };

        // Populate Settings Inputs
        function populateSettingsInputs() {
            if (nodeInfo.freq_mhz) document.getElementById("cfgFreq").value = nodeInfo.freq_mhz;
            if (nodeInfo.bw_khz) document.getElementById("cfgBw").value = String(nodeInfo.bw_khz);
            if (nodeInfo.sf) document.getElementById("cfgSf").value = String(nodeInfo.sf);
            if (nodeInfo.cr) document.getElementById("cfgCr").value = String(nodeInfo.cr);
            if (nodeInfo.tx_power) document.getElementById("cfgTxPower").value = String(nodeInfo.tx_power);
            if (nodeInfo.name) document.getElementById("cfgNodeName").value = nodeInfo.name;
            if (nodeInfo.lat) document.getElementById("cfgLat").value = nodeInfo.lat;
            if (nodeInfo.lon) document.getElementById("cfgLon").value = nodeInfo.lon;
            if (nodeInfo.model) document.getElementById("cfgDevModel").textContent = nodeInfo.model;
            if (nodeInfo.firmware) document.getElementById("cfgDevFw").textContent = nodeInfo.firmware;
        }

        // WebSocket Connection
        function connectWebSocket() {
            const protocol = location.protocol === "https:" ? "wss:" : "ws:";
            const wsUrl = `${protocol}//${location.host}/ws/client`;
            socket = new WebSocket(wsUrl);

            socket.onopen = () => {
                console.log("WebSocket client connected");
            };

            socket.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    handleSocketMessage(data);
                } catch(e) {
                    console.error("Socket parse error:", e);
                }
            };

            socket.onclose = () => {
                console.warn("WebSocket client disconnected, reconnecting in 3s...");
                isHeltecConnected = false;
                updateHeader();
                setTimeout(connectWebSocket, 3000);
            };

            socket.onerror = (err) => {
                console.error("WebSocket error:", err);
            };
        }

        function handleSocketMessage(data) {
            if (data.type === "init") {
                isHeltecConnected = data.heltec_connected;
                if (data.channels) channels = data.channels;
                if (data.node_info) nodeInfo = Object.assign(nodeInfo, data.node_info);
                if (data.recent_messages && data.recent_messages.length > 0) {
                    const existingKeys = new Set(messages.map(getMsgKey));
                    data.recent_messages.forEach(m => {
                        const key = getMsgKey(m);
                        if (!existingKeys.has(key)) {
                            messages.push(m);
                            existingKeys.add(key);
                        }
                    });
                    messages.sort((a,b) => (a.timestamp || "").localeCompare(b.timestamp || ""));
                    saveMessagesToStorage();
                }
                if (data.recent_nodes) heardNodes = data.recent_nodes;
                updateHeader();
                renderChannelsBar();
                renderMessages();
                renderNodes();
                renderChannelsSettings();
                populateSettingsInputs();
            } else if (data.type === "status") {
                isHeltecConnected = data.heltec_connected;
                if (data.channels) channels = data.channels;
                if (data.node_info) nodeInfo = Object.assign(nodeInfo, data.node_info);
                updateHeader();
                populateSettingsInputs();
            } else if (data.type === "channels") {
                channels = data.channels;
                renderChannelsBar();
                renderChannelsSettings();
            } else if (data.type === "node_info") {
                nodeInfo = Object.assign(nodeInfo, data.node_info);
                updateHeader();
                populateSettingsInputs();
            } else if (data.type === "new_message") {
                let matched = false;
                if (data.client_id) {
                    for (let i = messages.length - 1; i >= 0; i--) {
                        if (messages[i].client_id === data.client_id) {
                            Object.assign(messages[i], data);
                            matched = true;
                            break;
                        }
                    }
                }
                if (!matched) {
                    const key = getMsgKey(data);
                    const exists = messages.some(m => getMsgKey(m) === key);
                    if (!exists) {
                        messages.push(data);
                        if (messages.length > 500) messages.shift();
                    }
                }
                saveMessagesToStorage();
                renderMessages();
                if (data.source === "LoRa Mesh") {
                    playChime("recv");
                    showToast(`Nuovo messaggio LoRa su [${data.channel}]: ${data.content.substring(0, 40)}`, true);
                    if (window.Notification && Notification.permission === "granted") {
                        try {
                            new Notification(`LoRa [${data.channel || 'Mesh'}]: ${data.sender || 'Radio'}`, {
                                body: data.content || '',
                                icon: "/api/icon.svg"
                            });
                        } catch(e) {}
                    }
                }
            } else if (data.type === "message_in_flight") {
                // Sent to radio, in the air
                for (let i = messages.length - 1; i >= 0; i--) {
                    if ((messages[i].source === "Web Client" || messages[i].source === "Telegram") && (!messages[i].ack_status || messages[i].ack_status === "sent_to_radio" || messages[i].ack_status === "pending")) {
                        messages[i].ack_status = data.status || "transmitted";
                        break;
                    }
                }
                saveMessagesToStorage();
                renderMessages();
            } else if (data.type === "message_ack") {
                // Confirmed delivery ACK by remote node
                for (let i = messages.length - 1; i >= 0; i--) {
                    if (messages[i].source === "Web Client" || messages[i].source === "Web API" || messages[i].source === "Telegram") {
                        messages[i].ack_status = "confirmed";
                        messages[i].rtt_ms = data.round_trip_ms;
                        if (data.hops !== undefined && data.hops !== null) {
                            messages[i].hops = data.hops;
                        }
                        break;
                    }
                }
                saveMessagesToStorage();
                renderMessages();
                playChime("ack");
                const hops = (data.hops !== undefined && data.hops !== null) ? decodeHops(data.hops) : null;
                const hopsStr = data.hops_str ? ` (${data.hops_str})` : (hops !== null ? (hops === 0 ? " (🎯 Diretto RF)" : ` (🔀 ${hops} salti)`) : "");
                const rttStr = data.round_trip_ms ? ` (${data.round_trip_ms} ms)` : "";
                showToast(`🟢 RECAPITATO! Confermato da nodo mesh${hopsStr}${rttStr}`, true);
            } else if (data.type === "radar_alert") {
                playChime("radar");
                const nName = data.node_name || "Nodo Diretto";
                const snrVal = (data.snr !== undefined && data.snr !== null) ? `${data.snr > 0 ? '+' : ''}${Number(data.snr).toFixed(1)} dB` : "N/A";
                showToast(`🚨 RADAR RF: Rilevato NUOVO NODO DIRETTO (0 salti): ${nName} (SNR: ${snrVal})!`, true);
                if (window.Notification && Notification.permission === "granted") {
                    try {
                        new Notification("🚨 RADAR RF - Nuovo Nodo Diretto!", {
                            body: `Rilevato ${nName} a 0 salti RF (SNR: ${snrVal})!`,
                            icon: "/api/icon.svg"
                        });
                    } catch(e) {}
                }
            } else if (data.type === "nodes") {
                heardNodes = data.nodes;
                renderNodes();
                renderMapMarkers();
            } else if (data.type === "action_result") {
                if (data.action === "find_nearby_nodes") {
                    scanInProgress = false;
                    if (data.nodes) {
                        heardNodes = data.nodes;
                        renderNodes();
                        renderMapMarkers();
                    }
                    showToast("Scansione nodi completata!", true);
                    showScanResultsModal(heardNodes, false);
                } else if (data.success) {
                    showToast(`Operazione '${data.action}' completata con successo!`, true);
                } else {
                    showToast(`Operazione '${data.action}' non riuscita.`, false);
                }
            }
        }

        // Send Message
        async function sendMessage() {
            const input = document.getElementById("messageInput");
            const text = input.value.trim();
            if (!text) return;

            const sender = document.getElementById("senderNameInput").value.trim() || "Web-Operatore";
            const btn = document.getElementById("sendBtn");
            btn.disabled = true;

            let finalLoRaText = text;
            let replySender = null;
            let replyText = null;

            if (currentReplyTarget) {
                replySender = currentReplyTarget.sender || (currentReplyTarget.source === "Web Client" ? "Tu" : "Nodo Radio");
                replyText = currentReplyTarget.content || "";
                finalLoRaText = `@[${replySender}] ${text}`;
                clearReplyTarget();
            }

            const clientMsgId = "msg_" + Date.now();
            const targetChannelIdx = activeChannelIdx === -1 ? 0 : activeChannelIdx;
            const chName = channels[targetChannelIdx] || `Canale ${targetChannelIdx}`;
            const optimisticMsg = {
                client_id: clientMsgId,
                source: "Web Client",
                sender: sender,
                channel: chName,
                channel_idx: targetChannelIdx,
                content: text,
                reply_to_sender: replySender,
                reply_to_text: replyText,
                timestamp: new Date().toISOString().replace('T', ' ').substring(0, 19),
                snr: null,
                hops: 0,
                sent_to_radio: false,
                ack_status: "pending"
            };
            messages.push(optimisticMsg);
            renderMessages();

            if (socket && socket.readyState === WebSocket.OPEN) {
                socket.send(JSON.stringify({
                    action: "send_message",
                    channel_idx: targetChannelIdx,
                    text: finalLoRaText,
                    sender: sender,
                    client_id: clientMsgId,
                    reply_to_sender: replySender,
                    reply_to_text: replyText
                }));
                input.value = "";
                input.focus();
                btn.disabled = false;
                showToast("📡 Inviato! La Heltec sta trasmettendo il pacchetto via LoRa.", true);
            } else {
                // Fallback to REST API
                try {
                    const resp = await fetch("/api/send", {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({
                            channel_idx: targetChannelIdx,
                            text: finalLoRaText,
                            sender: sender,
                            reply_to_sender: replySender,
                            reply_to_text: replyText
                        })
                    });
                    if (resp.ok) {
                        input.value = "";
                        input.focus();
                        showToast("📡 Inviato tramite REST API.", true);
                    } else {
                        showToast("❌ Errore invio messaggio.", false);
                    }
                } catch(e) {
                    showToast("❌ Errore di connessione.", false);
                }
                btn.disabled = false;
            }
        }

        document.getElementById("sendBtn").addEventListener("click", sendMessage);
        document.getElementById("cancelReplyBtn").addEventListener("click", (e) => {
            e.stopPropagation();
            clearReplyTarget();
        });
        document.getElementById("messageInput").addEventListener("keydown", (e) => {
            if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                sendMessage();
            }
        });

        // Settings Buttons
        document.getElementById("saveRadioBtn").addEventListener("click", () => {
            const freq = parseFloat(document.getElementById("cfgFreq").value);
            const bw = parseFloat(document.getElementById("cfgBw").value);
            const sf = parseInt(document.getElementById("cfgSf").value);
            const cr = parseInt(document.getElementById("cfgCr").value);
            const tx = parseInt(document.getElementById("cfgTxPower").value);

            if (socket && socket.readyState === WebSocket.OPEN) {
                socket.send(JSON.stringify({
                    action: "set_radio_params",
                    freq_mhz: freq,
                    bw_khz: bw,
                    sf: sf,
                    cr: cr
                }));
                socket.send(JSON.stringify({
                    action: "set_radio_tx_power",
                    tx_power: tx
                }));
            } else {
                fetch("/api/settings/radio", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ freq_mhz: freq, bw_khz: bw, sf: sf, cr: cr, tx_power: tx })
                });
            }
            showToast("Parametri inviati alla scheda Heltec...", true);
        });

        document.getElementById("saveNodeBtn").addEventListener("click", () => {
            const name = document.getElementById("cfgNodeName").value.trim();
            const lat = parseFloat(document.getElementById("cfgLat").value) || null;
            const lon = parseFloat(document.getElementById("cfgLon").value) || null;

            if (socket && socket.readyState === WebSocket.OPEN) {
                if (name) socket.send(JSON.stringify({ action: "set_advert_name", name: name }));
                if (lat && lon) socket.send(JSON.stringify({ action: "set_advert_latlon", lat: lat, lon: lon }));
            } else {
                fetch("/api/settings/node", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ name: name, lat: lat, lon: lon })
                });
            }
            showToast("Nome e posizione inviati alla scheda...", true);
        });

        document.getElementById("sendAdvertBtn").addEventListener("click", () => {
            if (socket && socket.readyState === WebSocket.OPEN) {
                socket.send(JSON.stringify({ action: "send_self_advert" }));
            } else {
                fetch("/api/advert/send", { method: "POST" });
            }
            showToast("Beacon Advert trasmesso in flood!", true);
        });

        document.getElementById("syncTimeBtn").addEventListener("click", () => {
            if (socket && socket.readyState === WebSocket.OPEN) {
                socket.send(JSON.stringify({ action: "sync_time" }));
                showToast("Orologio sincronizzato con successo.", true);
            }
        });

        document.getElementById("rebootBtn").addEventListener("click", () => {
            if (confirm("Vuoi davvero riavviare la scheda Heltec V3 da remoto?")) {
                if (socket && socket.readyState === WebSocket.OPEN) {
                    socket.send(JSON.stringify({ action: "reboot" }));
                } else {
                    fetch("/api/reboot", { method: "POST" });
                }
                showToast("Comando di riavvio inviato!", false);
            }
        });

        // Save Channel Button
        document.getElementById("saveChannelBtn").addEventListener("click", () => {
            const idx = parseInt(document.getElementById("newChannelIdx").value);
            const name = document.getElementById("newChannelName").value.trim();
            const psk = document.getElementById("newChannelPsk").value.trim();
            if (!name) {
                showToast("Inserisci un nome valido per il canale.", false);
                return;
            }
            if (socket && socket.readyState === WebSocket.OPEN) {
                socket.send(JSON.stringify({
                    action: "set_channel",
                    channel_idx: idx,
                    name: name,
                    psk: psk
                }));
            }
            showToast(`Canale [${idx}] ${name} salvato!`, true);
        });

        document.getElementById("refreshChannelsBtn").addEventListener("click", () => {
            if (socket && socket.readyState === WebSocket.OPEN) {
                socket.send(JSON.stringify({ action: "refresh_channels" }));
                showToast("Interrogazione canali in corso...", true);
            }
        });

        document.getElementById("refreshNodesBtn").addEventListener("click", () => {
            fetch("/api/nodes").then(r => r.json()).then(nodes => {
                heardNodes = nodes;
                renderNodes();
                renderMapMarkers();
                showToast("Lista nodi aggiornata.", true);
            });
        });

        // Trova Nodi Vicini (Discovery)
        let scanInProgress = false;
        document.getElementById("findNearbyNodesBtn").addEventListener("click", () => {
            showToast("🔍 Scansione nodi vicini avviata...", true);
            // 1. Immediately open modal with active scanning animation
            showScanResultsModal(null, true);
            scanInProgress = true;

            const triggerScanSuccess = (nodes) => {
                if (!scanInProgress) return;
                scanInProgress = false;
                if (nodes && Array.isArray(nodes)) {
                    heardNodes = nodes;
                    renderNodes();
                    renderMapMarkers();
                }
                showScanResultsModal(heardNodes, false);
            };

            // 2. Transmit discovery command via WebSocket
            if (socket && socket.readyState === WebSocket.OPEN) {
                socket.send(JSON.stringify({ action: "find_nearby_nodes" }));
            }

            // Fallback REST call if WebSocket is closed or if it takes > 2.5s
            setTimeout(() => {
                if (scanInProgress) {
                    fetch("/api/nodes/scan", { method: "POST" })
                        .then(r => r.json())
                        .then(res => {
                            triggerScanSuccess(res.nodes || heardNodes);
                        })
                        .catch(() => {
                            triggerScanSuccess(heardNodes);
                        });
                }
            }, 2500);
        });

        // Scan Modal Close Listeners
        document.getElementById("closeScanModalBtn").addEventListener("click", closeScanResultsModal);
        document.getElementById("closeScanModalFooterBtn").addEventListener("click", closeScanResultsModal);
        document.getElementById("scanModalOverlay").addEventListener("click", (e) => {
            if (e.target === document.getElementById("scanModalOverlay")) {
                closeScanResultsModal();
            }
        });

        // GPS Geolocation from Browser / Phone
        document.getElementById("getGpsLocationBtn").addEventListener("click", () => {
            if (!navigator.geolocation) {
                showToast("Geolocalizzazione non supportata dal tuo browser.", false);
                return;
            }
            showToast("Acquisizione coordinate GPS in corso...", true);
            navigator.geolocation.getCurrentPosition(
                (pos) => {
                    const lat = parseFloat(pos.coords.latitude.toFixed(5));
                    const lon = parseFloat(pos.coords.longitude.toFixed(5));
                    document.getElementById("cfgLat").value = lat;
                    document.getElementById("cfgLon").value = lon;
                    nodeInfo.lat = lat;
                    nodeInfo.lon = lon;

                    // Automatically save to Heltec
                    if (socket && socket.readyState === WebSocket.OPEN) {
                        socket.send(JSON.stringify({ action: "set_advert_latlon", lat: lat, lon: lon }));
                    } else {
                        fetch("/api/settings/node", {
                            method: "POST",
                            headers: { "Content-Type": "application/json" },
                            body: JSON.stringify({ lat: lat, lon: lon })
                        });
                    }
                    showToast(`📍 Posizione GPS rilevata e salvata: (${lat}, ${lon})`, true);
                    renderMapMarkers();
                },
                (err) => {
                    showToast(`Errore rilevamento GPS: ${err.message}`, false);
                },
                { enableHighAccuracy: true, timeout: 10000, maximumAge: 0 }
            );
        });

        // Map Centering Button
        document.getElementById("mapCenterBtn").addEventListener("click", () => {
            if (!leafletMap) return;
            const myLat = (nodeInfo && nodeInfo.lat) ? Number(nodeInfo.lat) : null;
            const myLon = (nodeInfo && nodeInfo.lon) ? Number(nodeInfo.lon) : null;
            if (myLat && myLon) {
                leafletMap.setView([myLat, myLon], 13);
                if (localNodeMarker) localNodeMarker.openPopup();
            } else {
                showToast("Nessuna coordinata GPS salvata per la tua stazione.", false);
            }
        });

        // Map Mode Switcher (MeshCore Italia Live Map vs Local Map)
        const tabMapLiveBtn = document.getElementById("tabMapLiveBtn");
        const tabMapLocalBtn = document.getElementById("tabMapLocalBtn");
        const viewMapLive = document.getElementById("viewMapLive");
        const viewMapLocal = document.getElementById("viewMapLocal");

        tabMapLiveBtn.addEventListener("click", () => {
            tabMapLiveBtn.classList.add("active");
            tabMapLocalBtn.classList.remove("active");
            viewMapLive.style.display = "block";
            viewMapLocal.style.display = "none";
        });

        tabMapLocalBtn.addEventListener("click", () => {
            tabMapLocalBtn.classList.add("active");
            tabMapLiveBtn.classList.remove("active");
            viewMapLocal.style.display = "block";
            viewMapLive.style.display = "none";
            setTimeout(() => {
                initOrUpdateMap();
            }, 100);
        });

        document.getElementById("reloadLiveMapBtn").addEventListener("click", () => {
            const iframe = document.getElementById("meshCoreLiveMapIframe");
            if (iframe) {
                iframe.src = iframe.src;
                showToast("Mappa MeshCore Italia ricaricata.", true);
            }
        });

        // Helpers
        function escapeHtml(str) {
            if (!str) return "";
            return String(str)
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#039;");
        }

        // Keepalive
        setInterval(() => {
            if (socket && socket.readyState === WebSocket.OPEN) {
                socket.send(JSON.stringify({ action: "ping" }));
            }
        }, 25000);

        // Init
        renderChannelsBar();
        connectWebSocket();
        fetchHourlyTraffic();
        fetchAutoResponderStatus();
    </script>
</body>
</html>"""
