import asyncio
import contextlib
import logging
import signal
from functools import wraps
from typing import Callable, Coroutine, TypeAlias

from phackers.cli import Config

logger = logging.getLogger(__name__)

ConnHandler: TypeAlias = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Coroutine]


CLIENT_HANDLERS: set[asyncio.Task] = set()

SHUTTING_DOWN = False


async def _shutdown_server(
    config: Config,
    stop_event: asyncio.Event,
    finished_event: asyncio.Event,
    server: asyncio.Server,
) -> None:
    global SHUTTING_DOWN
    if SHUTTING_DOWN:
        logger.warning("Shutdown already in progress, ignoring additional signal")
        return
    SHUTTING_DOWN = True
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
    c_results = await asyncio.gather(*CLIENT_HANDLERS, return_exceptions=True)
    for r in c_results:
        if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError):
            logger.debug(f"Client handler ended with exception: {r}", exc_info=True)
    finished_event.set()


def prepare_shutdown(
    config: Config,
    stop_event: asyncio.Event,
    finished_event: asyncio.Event,
    server: asyncio.Server,
) -> tuple[Callable[[], Coroutine[None, None, None]], Callable[[], Coroutine[None, None, None]]]:
    async def shutdown() -> None:
        await _shutdown_server(config, stop_event, finished_event, server)

    async def stop_monitor():
        global SHUTTING_DOWN
        if SHUTTING_DOWN:
            return

        while not stop_event.is_set():
            await asyncio.sleep(0.1)
        if SHUTTING_DOWN:
            return

        await shutdown()

    return shutdown, stop_monitor


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
        handler_task = None
        try:
            handler_task = asyncio.create_task(handler(reader, writer))
            CLIENT_HANDLERS.add(handler_task)
            await handler_task
        except asyncio.CancelledError:
            logger.debug(f"Connection handler for {addr} was cancelled")
        except Exception as e:
            logger.error(f"Error in handler for {addr}: {e}", exc_info=True)
        finally:
            if handler_task:
                CLIENT_HANDLERS.remove(handler_task)
                logger.debug(f"Handler task for {addr} removed from active handlers")
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
    signals = (signal.SIGINT, signal.SIGTERM)
    shutdown, stop_monitor = prepare_shutdown(config, stop_event, finished_event, server)
    for s in signals:
        loop.add_signal_handler(
            s,
            lambda lp: lp.create_task(shutdown()),
            loop,
        )

    stop_task = asyncio.create_task(stop_monitor())
    addr = server.sockets[0].getsockname()
    logger.info(f"Serving on {addr}")

    async def serve(server: asyncio.Server) -> None:
        async with server:
            server_task = asyncio.create_task(server.serve_forever())
            while not stop_event.is_set():
                await asyncio.sleep(0.1)
                if server_task.done():
                    if not stop_event.is_set():
                        stop_event.set()
                    break
        server_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await server_task

    try:
        await serve(server)
    except Exception as e:
        logger.error(f"Server error: {e}", exc_info=True)
    finally:
        if not stop_event.is_set():
            stop_event.set()
        await finished_event.wait()
        logger.info("Server has shut down.")
        with contextlib.suppress(asyncio.CancelledError):
            stop_task.cancel()
            await stop_task


def run_server(
    config: Config, handler_factory: Callable[[Config, asyncio.Event], ConnHandler]
) -> int:
    try:
        if config.with_uvloop:
            import uvloop

            uvloop.run(_run_server(config, handler_factory))
        else:
            asyncio.run(_run_server(config, handler_factory))
    except Exception:
        return 1
    return 0
