import logging
import sys
from dataclasses import asdict, dataclass
from enum import IntEnum
from functools import wraps
from pathlib import Path
from typing import Callable

import click

logger = logging.getLogger(__name__)


class LogLevel(IntEnum):
    CRITICAL = logging.CRITICAL
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    INFO = logging.INFO
    DEBUG = logging.DEBUG

    def __str__(self) -> str:
        return self.name


@dataclass
class Config:
    verbose: int
    address: str
    port: int
    buffer: int
    with_uvloop: bool
    log_level: LogLevel

    def run_configure(self) -> None:
        if self.verbose >= 2:
            self.log_level = LogLevel.DEBUG
        logging.basicConfig(
            level=self.log_level,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        if self.with_uvloop:
            logger.debug("uvloop is enabled in configuration")
            try:
                import uvloop

                uvloop.install()
                logger.info("Using uvloop event loop")
            except ImportError:
                logger.warning("uvloop is not installed; using default event loop")
                self.with_uvloop = False

        if self.verbose < 1:
            return

        def snake_to_normal(s: str) -> str:
            return s.replace("_", " ").capitalize()

        eprint_verbose("Configuration:", self, level=1)
        cfg_as_dict = asdict(self)
        reprs = [f"{snake_to_normal(k)}:" for k in cfg_as_dict]
        max_len = max(len(r) for r in reprs)
        for i, v in enumerate(cfg_as_dict.values()):
            repr = reprs[i].rjust(max_len)
            eprint_verbose(f"{repr}    {v}", self, level=1)


def _ensure_common_options(func: Callable) -> Callable:
    func = click.option("-v", "--verbose", count=True)(func)
    func = click.option("-a", "--address", default="0.0.0.0", help="Server address")(func)
    func = click.option("-p", "--port", default=19991, help="Server port")(func)
    func = click.option(
        "-b",
        "--buffer",
        default=8192,
        type=int,
        help="Buffer size for reading/writing data",
    )(func)
    func = click.option(
        "--with-uvloop/--without-uvloop",
        default=True,
        help="Use uvloop if available",
    )(func)
    func = click.option("--log-level", default=LogLevel.INFO, type=LogLevel, help="Logging level")(
        func
    )
    return func


def eprint(msg: str) -> None:
    """Print a message to stderr."""
    click.echo(msg, err=True)


def eprint_verbose(msg: str, cfg: Config, level: int = 1) -> None:
    """Print a message to stderr if verbosity level is sufficient."""
    if cfg.verbose >= level:
        eprint(msg)


def with_common_options(func: Callable) -> Callable:

    @wraps(func)
    @click.pass_context
    def wrapper(ctx, *args, **kwargs):
        cfg = Config(
            verbose=kwargs.pop("verbose"),
            address=kwargs.pop("address"),
            port=kwargs.pop("port"),
            buffer=kwargs.pop("buffer"),
            with_uvloop=kwargs.pop("with_uvloop"),
            log_level=kwargs.pop("log_level"),
        )
        cfg.run_configure()
        ctx.obj = cfg
        cmd_name = Path(sys.argv[0]).name
        logger.info("Starting command: %s", Path(sys.argv[0]).name)
        try:
            func(*args, **kwargs)
        except Exception as e:
            eprint(f"Error during {cmd_name} executior {e}")
            if cfg.verbose >= 1:
                click.echo(e.__traceback__, err=True)
            return 1
        return 0

    return click.command()(_ensure_common_options(wrapper))
