import asyncio
import bisect
import logging
import struct
from pathlib import Path
from typing import NamedTuple

from phackers.cli import Config, eprint_verbose
from phackers.server import ConnHandler

logger = logging.getLogger(Path(__name__).stem)


# 1 byte: 'I' for insert, 'Q' for query
# Insert request, 9 bytes, big-endian, 2 int32 fields: timestamp and price
# Byte:  |  0  |  1     2     3     4  |  5     6     7     8  |
# Type:  |char |         int32         |         int32         |
# Value: | 'I' |       timestamp       |         price         |

# Query request, 9 bytes, big-endian 2 int32 fields: mintime and maxtime
# Byte:  |  0  |  1     2     3     4  |  5     6     7     8  |
# Type:  |char |         int32         |         int32         |
# Value: | 'Q' |        mintime        |        maxtime        |
RawRequest = struct.Struct(">cii")


class ParsedInsert(NamedTuple):
    method: str
    ts: int
    price: int


class ParsedQuery(NamedTuple):
    method: str
    mintime: int
    maxtime: int

    def is_valid(self) -> bool:
        return self.mintime <= self.maxtime


def parse_request(data: memoryview) -> ParsedInsert | ParsedQuery:
    if len(data) != RawRequest.size:
        raise ValueError(f"Invalid request length: expected {RawRequest.size}, got {len(data)}")
    method_byte, field1, field2 = RawRequest.unpack_from(data)
    method = method_byte.decode()
    if method == "I":
        return ParsedInsert(method, field1, field2)
    elif method == "Q":
        return ParsedQuery(method, field1, field2)
    else:
        raise ValueError(f"Invalid method byte: expected 'I' or 'Q', got {method_byte!r}")


# response encoding:
# Hexadecimal: 00 00 13 f3
# Decoded:            5107
# 4 bytes, big-endian int32, the mean price for the query
Response = struct.Struct(">i")


class PriceRegistry:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._prices: list[tuple[int, int]] = []
        self._loop = loop

    # Because we have single client inserts
    # we can avoid locking because we are doing all the Insert / Query operations
    # sequentially (we can't query while inserting, and we can't insert while querying)
    def insert(self, ts: int, price: int) -> None:
        bisect.insort_left(self._prices, (ts, price))

    def _query(self, mintime: int, maxtime: int) -> int:

        left_idx = bisect.bisect_left(self._prices, (mintime, -float("inf")))
        right_idx = bisect.bisect_right(self._prices, (maxtime, float("inf")))
        if left_idx >= right_idx:
            return 0
        total_price = sum(price for _, price in self._prices[left_idx:right_idx])
        count = right_idx - left_idx
        return total_price // count

    async def query(self, mintime: int, maxtime: int) -> int:
        if mintime > maxtime:
            return 0
        return await self._loop.run_in_executor(None, self._query, mintime, maxtime)


def create_mean_handler(cfg: Config, stop: asyncio.Event) -> ConnHandler:
    eprint_verbose("Creating mean handler", cfg, level=2)

    async def handle_mean_conn(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        addr = writer.get_extra_info("peername")
        req_buffer = memoryview(bytearray(RawRequest.size))
        resp_buffer = memoryview(bytearray(Response.size))
        registry = PriceRegistry(asyncio.get_running_loop())

        async def read_request() -> bool:
            nonlocal req_buffer

            cnt = 0
            while cnt < RawRequest.size:
                logger.debug(f"Reading request from {addr}, bytes read so far: {cnt}")
                chunk = await reader.read(RawRequest.size - cnt)
                logger.debug(f"Read chunk from {addr}: {chunk!r}")
                if len(chunk) == 0:
                    return False
                cnt += len(chunk)
                req_buffer[cnt - len(chunk) : cnt] = chunk
            return True

        async def write_response(response: memoryview) -> None:
            writer.write(response)
            await writer.drain()

        while not stop.is_set():
            try:
                ok = await read_request()
                if not ok:
                    logger.debug(f"Connection closed by {addr}")
                    break

                logger.debug(f"Received data from {addr}: {req_buffer!r}")
                req = parse_request(req_buffer)
                match req:
                    case ParsedInsert(method="I", ts=ts, price=price):
                        registry.insert(ts, price)
                    case ParsedQuery(method="Q", mintime=mintime, maxtime=maxtime):
                        mean_price = await registry.query(mintime, maxtime)
                        Response.pack_into(resp_buffer, 0, mean_price)
                        await write_response(resp_buffer)
                    case _:
                        raise ValueError(f"Invalid request: {req}")
            except Exception as e:
                logger.error(f"Error processing request from {addr}: {e}")
                break

        writer.close()
        await writer.wait_closed()

    return handle_mean_conn
