#!/usr/bin/env python3
from __future__ import annotations
from lmn import __version__
from lmn import logger
import sys
import argparse
from typing import List, Optional


def global_parser():
    from . import brun, run, sync, nv
    commands = [brun, run, sync, nv]

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version="{} - version {}".format(
            "LMN", __version__
        )
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False
    )

    # It's a bit annoying that I need to have `--verbose` specified in every subcommand as well...
    # Otherwise the parser complains
    parser.add_argument(
        "--verbose",
        default=False,
        action="store_true",
        help="Be verbose"
    )

    # Register subparsers
    subparsers = parser.add_subparsers()
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
    return parser


def core(args):
    # Ensure same behavior while testing and using the CLI
    args = args or sys.argv[1:]

    # Get parser and parse arguments
    parser = global_parser()
    parsed = parser.parse_args(args)

    from logging import INFO, DEBUG
    logger.setLevel(INFO)
    if parsed.verbose:
        logger.setLevel(DEBUG)

    if len(args) == 0:
        # For the case that only `lmn` is given as a command
        parser.print_help()
        sys.exit(0)

    # Load config and fuse it with parsed arguments
    from ._config_loader import load_config
    project, remote_conf, preset_conf = load_config(parsed.machine)
    parsed.handler(project, remote_conf, parsed, preset_conf)


def main(args: Optional[List[str]] = None) -> None:
    """
    Entrypoint for the CLI.
    Arguments are only for test (it is always None if calling via CLI).
    """
    core(args)
