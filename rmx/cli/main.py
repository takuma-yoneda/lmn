#!/usr/bin/env python3
import os
from os.path import join as pjoin
import sys
import argparse
import pathlib

import rmx
from rmx import __version__
from rmx.helpers import find_project_root

from rmx.cli.commands.run import CLIRunCommand
from rmx.cli.commands.sync import CLISyncCommand
from rmx.cli.commands.status import CLIStatusCommand
from rmx import logger

_supported_commands = {
    'run': CLIRunCommand,
    'sync': CLISyncCommand,
    # 'status': CLIStatusCommand,
    # 'create': CLICreateCommand,
    # 'info': CLIInfoCommand,
    # 'build': CLIBuildCommand,
    # 'run': CLIRunCommand,
    # 'clean': CLICleanCommand,
    # 'push': CLIPushCommand,
    # 'decorate': CLIDecorateCommand,
    # 'machine': CLIMachineCommand,
    # 'endpoint': CLIEndpointCommand,
}

def run():
    """
    - Read config file
    - Collect local project repo info (root directory, etc)
    - parse args and pass them to a proper subcommand
    """

    print(f"rmx - Remote code execution for ML researchers - v{rmx.__version__}")
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        'command',
        choices=_supported_commands.keys()
    )
    # print help (if needed)
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help']:
        parser.print_help()
        return
    # ---
    # parse `command`
    parsed, remaining = parser.parse_known_args()

    # get command
    command = _supported_commands[parsed.command]
    # let the command parse its arguments
    cmd_parser = command.get_parser(remaining)
    parsed = cmd_parser.parse_args(remaining)
    # sanitize workdir

    if parsed.verbose:
        from logging import DEBUG
        logger.setLevel(DEBUG)

    # TODO: find a correct project directory
    parsed.workdir = find_project_root()
    current_dir = pathlib.Path(os.getcwd()).resolve()
    relative_workdir = current_dir.relative_to(parsed.workdir)
    logger.info(f'Project root directory: {parsed.workdir}')
    logger.info(f'relative working dir: {relative_workdir}')  # cwd.relative_to(project_root)
    parsed.name = parsed.workdir.stem

    # Read from rmx config file and reflect it
    # TODO: clean this up
    from rmx.helpers import parse_config
    config = parse_config(parsed.workdir)

    command.execute(config, parsed, relative_workdir=relative_workdir)

if __name__ == '__main__':
    run()
