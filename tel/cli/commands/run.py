#!/usr/bin/env python3

import os
import argparse
import shutil

from tel.machine import Machine
from tel.project import Project
from tel.cli import AbstractCLICommand
from typing import Optional, List

Arguments = List[str]
RSYNC_DESTINATION_PATH = "/tmp/".rstrip('/')

class CLIRunCommand(AbstractCLICommand):

    KEY = 'run'

    @staticmethod
    def parser(parent: Optional[argparse.ArgumentParser] = None,
               args: Optional[Arguments] = None) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(parents=[parent])
        parser.add_argument(
            "-v",
            "--mount",
            default=False,
            const=True,
            action="store",
            nargs="?",
            type=str,
            help="Whether to mount the current project into the container. "
                 "Pass a comma-separated list of paths to mount multiple projects",
        )
        parser.add_argument(
            "-d",
            "--disown",
            action="store_true",
            help="Do not block to wait for the process to exit. stdout/stderr will not be shown with this option.",
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

    @staticmethod
    def execute(machine: Machine, parsed: argparse.Namespace) -> None:
        from tel.machine import LocalMachine
        from tel.cli.utils import rsync

        project = machine.project

        # sync
        # sync_remote = parsed.sync or parsed.sync_mirror
        sync_remote = not isinstance(machine, LocalMachine)
        print('sync remote', sync_remote)
        if sync_remote:
            # A trick to create non-existing directory before running rsync (https://www.schwertly.com/2013/07/forcing-rsync-to-create-a-remote-path-using-rsync-path/)
            rsync_options = f"--rsync-path='mkdir -p {project.remote_dir} && mkdir -p {project.remote_outdir} && rsync'"
            rsync(source_dir=project.root_dir, target_dir=machine.uri(project.remote_dir), options=rsync_options)

        result = machine.execute(parsed.remote_command, disown=parsed.disown, shell=True)
        print('--- result ---\n', result)
        print('=== completed ===')




