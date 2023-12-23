#!/usr/bin/env python3
"""Bare-run: Run the given command without rsync, container setup, startup command or cd to workdir."""


from __future__ import annotations
from copy import deepcopy
import os
from pathlib import Path
from argparse import ArgumentParser
from argparse import Namespace
from lmn import logger
from lmn.helpers import find_project_root
from lmn.helpers import replace_lmn_envvars
from lmn.cli._config_loader import Project, Machine
from lmn.machine import CLISSHClient

from typing import TYPE_CHECKING, Optional
if TYPE_CHECKING:
    from ._config_loader import Machine

def _get_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument(
        "machine",
        action="store",
        type=str,
        help="Machine",
    )
    parser.add_argument(
        "--verbose",
        default=False,
        action="store_true",
        help="Be verbose"
    )
    parser.add_argument(
        "remote_command",
        default=False,
        action="store",
        nargs="+",
        type=str,
        help="Command to execute in a remote machine.",
    )
    return parser


def print_conf(mode: str, machine: Machine, image: Optional[str] = None):
    output = f'Running with [{mode}] mode on [{machine.remote_conf.base_uri}]'
    if image is not None:
        output += f' with image: [{image}]'
    logger.info(output)

def handler(project: Project, machine: Machine, parsed: Namespace, preset: dict):
    logger.debug(f'handling command for {__file__}')
    logger.debug(f'parsed: {parsed}')

    # HACK
    mode = 'ssh'

    if isinstance(parsed.remote_command, list):
        cmd = ' '.join(parsed.remote_command)
    else:
        cmd = parsed.remote_command

    # Runtime info
    runtime_options = Namespace(dry_run=parsed.dry_run,
                                rel_workdir=None,
                                cmd=cmd)

    env = {**project.env, **machine.env}
    lmndirs = machine.get_lmndirs(project.name)

    startup = ' ; '.join([e for e in [project.startup, machine.startup] if e.strip()])

    # If parsed.mode is not set, try to read from the config file.
    from lmn.runner import SSHRunner
    ssh_client = CLISSHClient(machine.remote_conf)
    ssh_runner = SSHRunner(ssh_client, lmndirs)
    print_conf(mode, machine)
    ssh_runner.exec(runtime_options.cmd,
                    relative_workdir=runtime_options.rel_workdir,
                    startup=startup,
                    env=env,
                    dry_run=runtime_options.dry_run)


name = 'brun'
description = 'Bare run: Just run the command without any setup'
parser = _get_parser()
