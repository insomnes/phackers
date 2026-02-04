import asyncio
import logging
from pathlib import Path

from phackers.cli import Config, eprint_verbose
from phackers.server import ConnHandler

logger = logging.getLogger(Path(__name__).stem)


def create_echo_handler(cfg: Config, stop: asyncio.Event) -> ConnHandler:
    eprint_verbose("Creating echo handler", cfg, level=2)

    async def handle_echo_conn(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        addr = writer.get_extra_info("peername")
        while not stop.is_set():
            data = await reader.read(cfg.buffer)
            if not data:
                logger.debug(f"Connection from {addr} closed")
                break
            logger.debug(f"Received {len(data)} bytes from {addr}")
            eprint_verbose(f"Echoing back data: {data}", cfg, level=3)
            writer.write(data)
            await writer.drain()

        writer.close()
        await writer.wait_closed()

    return handle_echo_conn
