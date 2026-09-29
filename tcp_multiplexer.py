import os
import asyncio
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

HELTEC_HOST = os.getenv("HELTEC_TCP_HOST", "192.168.1.100")
HELTEC_PORT = int(os.getenv("HELTEC_TCP_PORT", "5000"))
LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 5000

connected_clients = set()
heltec_writer = None
heltec_lock = asyncio.Lock()

async def broadcast_to_clients(data: bytes):
    dead = set()
    for writer in list(connected_clients):
        try:
            writer.write(data)
            await writer.drain()
        except Exception:
            dead.add(writer)
    for w in dead:
        connected_clients.discard(w)

async def heltec_connection_loop():
    global heltec_writer
    while True:
        try:
            logging.info(f"Connecting to Heltec V3 at {HELTEC_HOST}:{HELTEC_PORT}...")
            reader, writer = await asyncio.open_connection(HELTEC_HOST, HELTEC_PORT)
            async with heltec_lock:
                heltec_writer = writer
            logging.info("Connected to Heltec V3 TCP port 5000!")

            while True:
                data = await reader.read(4096)
                if not data:
                    logging.warning("Heltec connection closed by peer.")
                    break
                await broadcast_to_clients(data)

        except Exception as e:
            logging.error(f"Heltec error: {e}")
        finally:
            async with heltec_lock:
                heltec_writer = None
            await asyncio.sleep(2)

async def handle_client(reader, writer):
    peer = writer.get_extra_info("peername")
    logging.info(f"New companion client connected from {peer}")
    connected_clients.add(writer)

    try:
        while True:
            data = await reader.read(4096)
            if not data:
                break
            async with heltec_lock:
                if heltec_writer:
                    heltec_writer.write(data)
                    await heltec_writer.drain()
    except Exception as e:
        logging.warning(f"Client {peer} error: {e}")
    finally:
        logging.info(f"Client {peer} disconnected")
        connected_clients.discard(writer)
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

async def main():
    asyncio.create_task(heltec_connection_loop())
    server = await asyncio.start_server(handle_client, LISTEN_HOST, LISTEN_PORT)
    logging.info(f"Multiplexer listening on {LISTEN_HOST}:{LISTEN_PORT}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
