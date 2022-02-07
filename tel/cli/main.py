#!/usr/bin/env python3
import os
import sys
import argparse
import pathlib

import tel
from tel import __version__
from tel.machine import Machine, SSHMachine, DockerMachine

from tel.cli.commands.run import CLIRunCommand

_supported_commands = {
    'run': CLIRunCommand
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
    print(f"tel - Remote code execution for ML researchers - v{tel.__version__}")
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
    print(f'main.py -- parsed: {parsed}\tremaining:{remaining}')
    # get command
    command = _supported_commands[parsed.command]
    # let the command parse its arguments
    cmd_parser = command.get_parser(remaining)
    parsed = cmd_parser.parse_args(remaining)
    # sanitize workdir
    # parsed.workdir = os.path.abspath(parsed.workdir)
    # TEMP: use current dir as a project
    current_dir = pathlib.Path(os.getcwd()).resolve()
    parsed.workdir = current_dir
    parsed.name = current_dir.stem

    # execute command
    # machine = SSHMachine('takuma', 'birch.ttic.edu')# TEMP
    # machine = DockerMachine('takuma', 'birch.ttic.edu')# TEMP
    # command.execute(machine, parsed)


    from tel.project import Project
    from tel.machine import RemoteConfig
    remote_conf = RemoteConfig('takuma', 'birch.ttic.edu')
    project = Project(parsed.name, parsed.workdir)
    machine = DockerMachine(project, remote_conf)
    command.execute(machine, parsed)

    # enable debug
    # if parsed.debug:
    #     cpklogger.setLevel(logging.DEBUG)
    # get machine
    # machine = get_machine(parsed, cpkconfig.machines)
    # avoid commands using `parsed.machine`
    # parsed.machine = None
    # execute command
    # try:
    #     with machine:
    #         command.execute(machine, parsed)
    # except CPKException as e:
    #     cpklogger.error(str(e))
    # except KeyboardInterrupt:
    #     cpklogger.info(f"Operation aborted by the user")

if __name__ == '__main__':
    run()
