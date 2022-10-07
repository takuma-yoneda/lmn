#!/usr/bin/env python3
from __future__ import annotations
from rmx import __version__
from rmx import logger
import sys
import argparse


def global_parser():
    from . import run, sync
    commands = [run, sync]

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version="{} - version {}".format(
            "RMX", __version__
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False
    )

    # Register subparsers
    subparsers = parser.add_subparsers()
    name2subparser = {}
    for cmd in commands:
        subp = subparsers.add_parser(
            cmd.name,
            parents=[cmd.parser],  # HACK: this registers cmd.parser as the sub parser (https://docs.python.org/3.9/library/argparse.html#argparse.ArgumentParser)
            description=cmd.description,
            help=cmd.description,
            add_help=False  # Avoid help collision
            # formatter_class=PdmFormatter,
        )

        # NOTE: I don't particularly like this, but I follow how PDM handles (sub)commands.
        # This registers cmd.handler function as args.handler and it will be called later.
        subp.set_defaults(handler=cmd.handler)
        name2subparser[cmd.name] = subp
    return parser


def core(args):
    # Ensure same behavior while testing and using the CLI
    args = args or sys.argv[1:]

    # Get parser and parse arguments
    parser = global_parser()
    parsed = parser.parse_args(args)

    if parsed.verbose:
        from logging import DEBUG
        logger.setLevel(DEBUG)

    # Load config and fuse it with parsed arguments
    from ._config_loader import load_config
    project, remote_conf = load_config(parsed.machine)
    parsed.handler(project, remote_conf, parsed)


def main(args: list[str] | None = None) -> None:
    """
    Entrypoint for the CLI.
    Arguments are only for test (it is always None if calling via CLI).
    """
    core(args)
