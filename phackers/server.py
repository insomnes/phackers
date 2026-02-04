import asyncio
import logging
import signal
from functools import wraps
from typing import Callable, Coroutine, TypeAlias

from phackers.cli import Config

logger = logging.getLogger(__name__)

ConnHandler: TypeAlias = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Coroutine]


CLIENT_HANDLERS: set[asyncio.Task] = set()


def make_signal_handler(
    config: Config,
    stop_event: asyncio.Event,
    finished_event: asyncio.Event,
    server: asyncio.Server,
) -> Callable[[], Coroutine[None, None, None]]:

    async def signal_handler() -> None:
        logger.info("Received stop signal, shutting down...")
        stop_event.set()
        if not config.with_uvloop:
            server.close_clients()
        server.close()
        await server.wait_closed()
        for task in CLIENT_HANDLERS:
            task.cancel()

        logger.debug("Waiting for tasks to finish...")
        await asyncio.sleep(0.1)  # Allow cancelled tasks to propagate
        await asyncio.gather(*CLIENT_HANDLERS, return_exceptions=True)
        finished_event.set()

    return signal_handler


def handler_wrapper(
    handler: ConnHandler,
) -> ConnHandler:

    @wraps(handler)
    async def wrapped_handler(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        addr = writer.get_extra_info("peername")
        logger.info(f"Accepted connection from {addr}")
        task = None
        try:
            handler_task = asyncio.create_task(handler(reader, writer))
            CLIENT_HANDLERS.add(handler_task)
            await handler_task
        except asyncio.CancelledError:
            logger.debug(f"Connection handler for {addr} was cancelled")
        except Exception as e:
            logger.error(f"Error in handler for {addr}: {e}", exc_info=True)
        finally:
            if task:
                CLIENT_HANDLERS.remove(task)
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logger.info(f"Handler for {addr} finished")

    return wrapped_handler


# Coro gets asyncio.StreamReader, asyncio.StreamWriter
async def _run_server(
    config: Config, handler_factory: Callable[[Config, asyncio.Event], ConnHandler]
) -> None:
    loop = asyncio.get_running_loop()
    loop.set_debug(config.verbose >= 2)
    stop_event, finished_event = asyncio.Event(), asyncio.Event()

    handler = handler_factory(config, stop_event)
    server = await asyncio.start_server(
        handler_wrapper(handler),
        config.address,
        config.port,
    )
    cur_task = asyncio.current_task()
    assert cur_task is not None
    addr = server.sockets[0].getsockname()
    logger.info(f"Serving on {addr}")

    serve_task = loop.create_task(server.wait_closed())

    signals = (signal.SIGINT, signal.SIGTERM)
    for s in signals:
        loop.add_signal_handler(
            s,
            lambda lp: lp.create_task(
                make_signal_handler(config, stop_event, finished_event, server)()
            ),
            loop,
        )

    try:
        async with server:
            await serve_task
    finally:
        await finished_event.wait()
        logger.info("Server has shut down.")


def run_server(
    config: Config, handler_factory: Callable[[Config, asyncio.Event], ConnHandler]
) -> None:
    if config.with_uvloop:
        import uvloop

        uvloop.run(_run_server(config, handler_factory))
    else:
        asyncio.run(_run_server(config, handler_factory))
