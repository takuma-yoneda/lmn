#!/usr/bin/env python3
"""Simply run nvidia-smi command on a specified remote."""


from __future__ import annotations
from copy import deepcopy
import os
from pathlib import Path
from argparse import ArgumentParser
from argparse import Namespace
from lmn import logger
from lmn.helpers import find_project_root
from lmn.config import SlurmConfig
from lmn.helpers import replace_lmn_envvars
from lmn.cli._config_loader import Project, Machine
from lmn.machine import SimpleSSHClient

from lmn.runner import SlurmRunner
from .sync import _sync_output, _sync_code


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
    # parser.add_argument(
    #     "-m",
    #     "--mode",
    #     action="store",
    #     type=str,
    #     default=None,
    #     choices=["ssh", "docker", "slurm", "singularity", "slurm-sing", "sing-slurm"],
    #     help="What mode to run",
    # )
    return parser

from ._config_loader import Machine


def print_conf(mode: str, machine: Machine, image: str | None = None):
    output = f'Running with [{mode}] mode on [{machine.remote_conf.base_uri}]'
    if image is not None:
        output += f' with image: [{image}]'
    logger.info(output)

def handler(project: Project, machine: Machine, parsed: Namespace, preset: dict):
    logger.debug(f'handling command for {__file__}')
    logger.debug(f'parsed: {parsed}')

    # HACK
    parsed.remote_command = 'nvidia-smi'
    mode = 'ssh'

    # Runtime info
    curr_dir = Path(os.getcwd()).resolve()
    proj_rootdir = find_project_root()
    rel_workdir = curr_dir.relative_to(proj_rootdir)
    logger.debug(f'relative working dir: {rel_workdir}')  # cwd.relative_to(project_root)
    if isinstance(parsed.remote_command, list):
        cmd = ' '.join(parsed.remote_command)
    else:
        cmd = parsed.remote_command

    runtime_options = Namespace(dry_run=parsed.dry_run,
                                rel_workdir=rel_workdir,
                                cmd=cmd)

    env = {**project.env, **machine.env}
    lmndirs = machine.get_lmndirs(project.name)

    startup = ' && '.join([e for e in [project.startup, machine.startup] if e.strip()])

    # If parsed.mode is not set, try to read from the config file.
    from lmn.runner import SSHRunner
    ssh_client = SimpleSSHClient(machine.remote_conf)
    ssh_runner = SSHRunner(ssh_client, lmndirs)
    print_conf(mode, machine)
    ssh_runner.exec(runtime_options.cmd,
                    runtime_options.rel_workdir,
                    startup=startup,
                    env=env,
                    dry_run=runtime_options.dry_run)


name = 'nv'
description = 'run nvidia-smi on a remote server'
parser = _get_parser()
