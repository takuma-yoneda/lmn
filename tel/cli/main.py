#!/usr/bin/env python3
import os
import sys
import argparse
import pathlib

import tel
from tel import __version__
from tel.config import DockerContainerConfig, SlurmConfig
from tel.helpers import find_project_root
from tel.machine import Machine, SSHMachine, DockerMachine, SlurmMachine

from tel.cli.commands.run import CLIRunCommand
from simple_slurm_command import SlurmCommand

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

    # TODO: find a correct project directory
    # TEMP: use current dir as a project
    current_dir = pathlib.Path(os.getcwd()).resolve()
    parsed.workdir = find_project_root()
    print('Project root directory:', parsed.workdir)
    print('relative working dir:', current_dir.relative_to(parsed.workdir))  # cwd.relative_to(project_root)
    relative_workdir = current_dir.relative_to(parsed.workdir)
    parsed.name = parsed.workdir.stem


    # Read from tel config file and reflect it
    # TODO: clean this up
    from tel.helpers import parse_config
    config = parse_config()
    if parsed.machine not in config['machines']:
        raise KeyError(
            f'Machine {parsed.machine} not found in the configuration.\n'
            f'Available machines are: {config["machines"].keys()}'
        )
    machine_conf = config['machines'].get(parsed.machine)
    user, host = machine_conf['user'], machine_conf['host']


    # execute command
    # machine = SSHMachine('takuma', 'birch.ttic.edu')# TEMP
    # machine = DockerMachine('takuma', 'birch.ttic.edu')# TEMP
    # command.execute(machine, parsed)


    from tel.project import Project
    from tel.machine import RemoteConfig
    from tel.config import DockerContainerConfig, SlurmConfig
    remote_conf = RemoteConfig(user, host)

    # TODO: This looks horrible. Clean up.
    # If args.image is given, look for the corresponding name in config
    # if not found, regard it as a full name of the image
    if parsed.image:
        if parsed.image in config['docker-images']:
            image = config['docker-images'].get(parsed.image).get('name')
        else:
            image = parsed.image
        docker_conf = DockerContainerConfig(image, f'{user}-{parsed.name}')
    else:
        docker_conf = None

    # Parse slurm configuration
    slurm_conf = machine_conf.get('slurm')
    slurm_conf = SlurmConfig(**slurm_conf) if slurm_conf else None
    # if slurm_conf:
    #     slurm_conf.output = f'{SlurmCommand.JOB_ARRAY_MASTER_ID}_{SlurmCommand.JOB_ARRAY_ID}.out'
    print('slurm_conf', slurm_conf)

    project = Project(parsed.name, parsed.workdir, remote_dir=machine_conf.get('root_dir'),
                      out_dir=parsed.outdir,
                      docker=docker_conf, slurm=slurm_conf)
    print('project:', project)


    # NOTE: If default slurm config is set on a machine, tel assumes to always use slurm.
    if 'slurm' in config['machines'][parsed.machine]:
        machine = SlurmMachine(project, remote_conf)
    else:
        machine = SSHMachine(project, remote_conf)

    command.execute(machine, parsed, relative_workdir=relative_workdir)

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
