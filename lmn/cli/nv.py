#!/usr/bin/env python3
"""Simply run nvidia-smi command on a specified remote."""


from __future__ import annotations
from copy import deepcopy
import os
from pathlib import Path
from argparse import ArgumentParser
from argparse import Namespace
from lmn import logger
from lmn.cli.brun import handler as brun_handler

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from lmn.cli._config_loader import Project, Machine


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
    return parser


def handler(project: Project, machine: Machine, parsed: Namespace, preset: dict):
    parsed.remote_command = 'nvidia-smi'
    brun_handler(project, machine, parsed, preset)


name = 'nv'
description = 'run nvidia-smi on a remote server'
parser = _get_parser()
