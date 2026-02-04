import click

from phackers.cli import Config, with_common_options
from phackers.echo import create_echo_handler
from phackers.server import run_server


@with_common_options
def echo() -> int:
    """A simple echo server."""

    cfg: Config = click.get_current_context().obj
    try:
        run_server(cfg, create_echo_handler)
        return 0
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if cfg.verbose >= 1:
            raise
        return 1
