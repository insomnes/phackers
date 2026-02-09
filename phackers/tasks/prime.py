import asyncio
import logging
from pathlib import Path

import msgspec

from phackers.cli import Config, eprint_verbose
from phackers.server import ConnHandler

logger = logging.getLogger(Path(__name__).stem)


class PrimeRequest(msgspec.Struct):
    method: str
    number: int | float

    def validate(self) -> None:
        if self.method != "isPrime":
            raise ValueError(f"Unsupported method: {self.method}")


TRUE_RESPONSE = msgspec.json.encode({"method": "isPrime", "prime": True})
FALSE_RESPONSE = msgspec.json.encode({"method": "isPrime", "prime": False})
MALFORMED_RESPONSE = msgspec.json.encode({"error": "bad request"})


def _is_prime(n: int) -> bool:
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True


async def is_prime(n: int | float) -> bool:
    if isinstance(n, float) or n != int(n):
        return False
    if 1 < n <= 3:
        return True
    if n <= 1 or n % 2 == 0 or n % 3 == 0:
        return False
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _is_prime, n)


def create_prime_handler(cfg: Config, stop: asyncio.Event) -> ConnHandler:
    eprint_verbose("Creating prime handler", cfg, level=2)

    async def handle_prime_conn(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        addr = writer.get_extra_info("peername")

        async def write_response(response: bytes) -> None:
            writer.write(response)
            writer.write(b"\n")
            await writer.drain()

        while not stop.is_set():
            data = await reader.readline()
            if not data:
                logger.debug(f"Connection from {addr} closed")
                break
            logger.debug(f"Received data from {addr}: {data!r}")
            try:
                req = msgspec.json.decode(data, type=PrimeRequest)
                req.validate()
                prime = await is_prime(req.number)
                response = TRUE_RESPONSE if prime else FALSE_RESPONSE
                await write_response(response)
            except Exception as e:
                logger.error(f"Error processing request from {addr}: {e}")
                await write_response(MALFORMED_RESPONSE)
                break

        writer.close()
        await writer.wait_closed()

    return handle_prime_conn
