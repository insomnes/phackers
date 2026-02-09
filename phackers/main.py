import click

from phackers.cli import Config, with_common_options
from phackers.server import run_server
from phackers.tasks.echo import create_echo_handler
from phackers.tasks.prime import create_prime_handler


@with_common_options
def echo() -> int:
    """A simple echo server."""

    cfg: Config = click.get_current_context().obj
    return run_server(cfg, create_echo_handler)


@with_common_options
def prime() -> int:
    """A simple prime number server."""

    cfg: Config = click.get_current_context().obj
    return run_server(cfg, create_prime_handler)
