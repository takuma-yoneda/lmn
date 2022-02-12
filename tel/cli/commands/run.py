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
            "-L",
            "--local-forward",
            type=str,
            action="store",
            help="Local port forwarding. A general format is '1234:hostname:2345'."
                 "You can also just specify a number like '8080'. This is interpreted as '8080:localhost:8080."
                 "This cannot be used with -d/--disown option.",
        )
        parser.add_argument(
            "-R",
            "--remote-forward",
            action="store",
            help="Remote port forwarding. A general format is '1234:hostname:2345'."
                 "You can also just specify a number like '8080'. This is interpreted as '8080:localhost:8080."
                 "This cannot be used with -d/--disown option.",
        )
        parser.add_argument(
            "-X",
            "--x-forward",
            action="store_true",
            help="X11 forwarding",
        )
        parser.add_argument(
            "--entrypoint",
            action="store",
            help="Docker entrypoint",
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
        """Deploy the local repository and execute the command on a machine.

        1. get SSH connection to machine
        2. run rsync(project.root_dir, project.remote_rootdir)
        3. run machine.execute(parsed.remote_command)
        """
        from tel.machine import LocalMachine
        from tel.cli.utils import rsync

        # Check confilicts in parsed arguments
        assert not ((parsed.disown and parsed.local_forward) or (parsed.disown and parsed.remote_forward)), '-d/--disown option cannot be used with port forwarding option.'
        conn = machine.remote_conf.get_connection()

        if parsed.local_forward:
            if ':' in parsed.local_forward:
                local_port, remote_host, remote_port = parsed.local_forward.split(':')
            else:
                local_port, remote_host, remote_port = parsed.local_forward, 'localhost', parsed.local_forward

            port_forward = lambda : conn.forward_local(local_port, remote_port, remote_host)
            # TODO: I should write a new context manager that internally either use local/remote forward or none.
            # You can actually copy & paste from the source. The lines of code is not that big.

        project = machine.project

        # rsync the remote directory
        if not isinstance(machine, LocalMachine):
            # A trick to create non-existing directory before running rsync (https://www.schwertly.com/2013/07/forcing-rsync-to-create-a-remote-path-using-rsync-path/)
            rsync_options = f"--rsync-path='mkdir -p {project.remote_rootdir} && mkdir -p {project.remote_outdir} && rsync'"
            rsync(source_dir=project.root_dir, target_dir=machine.uri(project.remote_rootdir), options=rsync_options)

        print('remote command', parsed.remote_command)
        result = machine.execute(parsed.remote_command, disown=parsed.disown, shell=True, x_forward=parsed.x_forward)
        print('--- result ---\n', result)
        print('=== completed ===')
