#!/usr/bin/env python3

import argparse
import pathlib
from rmx.cli import AbstractCLICommand
from typing import Optional, List
from rmx import logger

Arguments = List[str]
RSYNC_DESTINATION_PATH = "/tmp/".rstrip('/')

class CLISyncCommand(AbstractCLICommand):

    KEY = 'sync'

    @staticmethod
    def parser(parent: Optional[argparse.ArgumentParser] = None,
               args: Optional[Arguments] = None) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(parents=[parent])

        parser.add_argument(
            "machine",
            action="store",
            type=str,
            help="Machine",
        )
        return parser

    @staticmethod
    def execute(config: dict, parsed: argparse.Namespace, relative_workdir: pathlib.Path=pathlib.Path('.')) -> None:
        """Deploy the local repository and execute the command on a machine.

        1. get SSH connection to machine
        2. run rsync(project.local_dir, project.remote_dir)
        3. run machine.execute(parsed.remote_command)
        """
        from rmx.cli.utils import rsync

        # Read from rmx config file and reflect it
        # TODO: clean this up
        from rmx.machine import RemoteConfig
        if parsed.machine not in config['machines']:
            raise KeyError(
                f'Machine "{parsed.machine}" not found in the configuration. '
                f'Available machines are: {" ".join(config["machines"].keys())}'
            )
        machine_conf = config['machines'].get(parsed.machine)
        user, host = machine_conf['user'], machine_conf['host']
        remote_conf = RemoteConfig(user, host)

        project_conf = config.get('project')


        from os.path import join as pjoin
        from rmx.project import Project
        project = Project(name=parsed.name,
                          local_dir=parsed.workdir,
                          remote_root_dir=machine_conf.get('root_dir'))
        logger.info('project: {project}')


        # NOTE: Diff from run.py: 1. no need to know "mode"
        # NOTE: Order to check 'mode'
        # 1. If specified in cli --> use that mode
        # 2. If default_mode is set (config file) --> use that mode
        # 3. Use ssh mode
        # mode = parsed.mode if parsed.mode else machine_conf.get('default_mode', 'ssh')

        from rmx.machine import SSHClient
        ssh_client = SSHClient(remote_conf)

        # TODO: Hmmm ugly... let's fix it later
        # rsync the remote directory
        # A trick to create non-existing directory before running rsync (https://www.schwertly.com/2013/07/forcing-rsync-to-create-a-remote-path-using-rsync-path/)
        if 'rsync' in config:
            exclude = config['rsync'].get('exclude')
        else:
            exclude = []

        if project_conf and 'rsync' in project_conf:
            exclude.extend(project_conf['rsync'].get('exclude', []))

        # A trick to create directories right before performing rsync
        rsync_options = f"--rsync-path='mkdir -p {project.remote_dir} && mkdir -p {project.remote_outdir} && mkdir -p {project.remote_mountdir} && rsync'"
        rsync(source_dir=project.local_dir, target_dir=ssh_client.uri(project.remote_dir), options=rsync_options,
              exclude=exclude, dry_run=parsed.dry_run, transfer_rootdir=False)

        # rsync the directories to mount
        mount_dirs = project_conf.get('mount', [])
        for mount_dir in mount_dirs:
            rsync(source_dir=mount_dir, target_dir=ssh_client.uri(project.remote_mountdir),
                exclude=exclude, dry_run=parsed.dry_run)

        # Rsync remote outdir with the local outdir.
        if project.local_outdir:
            from rmx.cli.utils import rsync
            # Check if there's any output file (the first line is always 'total [num-files]')
            result = ssh_client.run(f'ls -l {project.remote_outdir} | grep -v "^total" | wc -l', hide=True)
            num_output_files = int(result.stdout)
            logger.info(f'{num_output_files} files are in the output directory')
            if num_output_files:
                rsync(source_dir=ssh_client.uri(project.remote_outdir), target_dir=project.local_outdir, dry_run=parsed.dry_run)
