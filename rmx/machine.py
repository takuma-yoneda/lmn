#!/usr/bin/env python3
from abc import ABC
import os
from os.path import join as pjoin
from os.path import expandvars
from typing import Optional, Union
from docker import DockerClient
from rmx.config import DockerContainerConfig
from rmx.helpers import posixpath2str, replace_rmx_envvars
from rmx.project import Project

import fabric
import invoke

from rmx import logger

RMX_DOCKER_ROOTDIR = '/rmx'

# NOTE: Should I have ssh-conf, slurm-conf and docker-conf separately??
# I guess RemoteConfig should ONLY store the info on how to login to the host?
# docker info and slurm info should really reside in project.
class RemoteConfig:
    from fabric import Connection
    def __init__(self, user, host, port=22, slurm_node=False) -> None:
        self.user = user
        self.host = host
        # self.port = port
        self.slurm_node = slurm_node

    @property
    def base_uri(self) -> str:
        return f'{self.user}@{self.host}'

    def get_connection(self) -> Connection:
        from fabric import Connection
        from fabric.config import Config
        config = Config()
        config.user = self.user
        conn = Connection(host=self.host, config=config, inline_ssh_env=True)
        return conn

    def get_dict(self):
        return {key: val for key, val in vars(self).items() if not (key.startswith('__') or callable(val))}

class SSHClient:
    def __init__(self, remote_conf: RemoteConfig) -> None:
        self.remote_conf = remote_conf
        self.conn = self.remote_conf.get_connection()

    def uri(self, path):
        return f'{self.remote_conf.base_uri}:{path}'

    def run(self, cmd, directory='$HOME', disown=False, hide=False, env=None, pty=False, dry_run=False):
        import re
        
        # TODO: Check if $HOME would work or not!!
        env = {} if env is None else env
        env = replace_rmx_envvars(env)

        # Perform shell escaping for envvars
        # TEMP: shell escaping only when env contains space
        import shlex
        env = {key: shlex.quote(str(val)) if " " in str(val) else str(val) for key, val in env.items()}

        if dry_run:
            logger.info('--- dry run ---')
            logger.info(f'cmd: {cmd}')
            logger.debug(locals())
        else:
            with self.conn.cd(directory):
                # promise = self.conn.run(cmd, asynchronous=True)
                if disown:
                    # NOTE: asynchronous=True --> disown=True
                    # asynchronous=True returns a Promise to which you can attach and listen to stdio.
                    # disown=True completely disowns the process.
                    self.conn.run(cmd, disown=True, hide=hide, env=env, pty=pty)
                    return

                # NOTE: if you use asynchronous=True, stdout/stderr does not show up
                # when you use it on slurm. I have no idea why, tho.
                logger.info(f'ssh client env: {env}')
                result = self.conn.run(cmd, asynchronous=False, hide=hide, env=env, pty=pty)
            return result

    def put(self, file_like, target_path=None):
        self.conn.put(file_like, str(target_path))

    def port_forward(self):
        raise NotImplementedError

    def x_forward(self):
        raise NotImplementedError


class SSHMachine:
    def __init__(self, client: SSHClient, project: Project) -> None:
        self.client = client
        self.project = project

    def execute(self, cmd, relative_workdir, startup=None, disown=False, use_gpus=True, x_forward=False, env=None, dry_run=False) -> None:
        env = {} if env is None else env
        if isinstance(cmd, list):
            cmd = ' '.join(cmd) if len(cmd) > 1 else cmd[0]

        if startup:
            cmd = f'{startup} && {cmd}'
        # cmd = f'bash -c \'{cmd}\''
        # cmd = f'bash -c \'{cmd}\''

        logger.info(f'ssh run with command: {cmd}')
        logger.info(f'cd to {self.project.remote_dir / relative_workdir}')
        rmxenv = {'RMX_CODE_DIR': self.project.remote_dir, 'RMX_OUTPUT_DIR': self.project.remote_outdir, 'RMX_MOUNT_DIR': self.project.remote_mountdir, 'RMX_USER_COMMAND': cmd}
        env.update(rmxenv)
        return self.client.run(cmd, directory=(self.project.remote_dir / relative_workdir),
                               disown=disown, env=env, dry_run=dry_run, pty=True)


class SlurmMachine:
    """Use srun/sbatch to submit the command on a remote machine.

    If your local machine has slurm (i.e., you're on slurm login-node), I guess you don't need this tool.
    Thus SlurmMachine inherits SSHMachine.
    """
    def __init__(self, client: SSHClient, project, slurm_conf) -> None:
        self.client = client
        self.project = project
        self.slurm_conf = slurm_conf

    def execute(self, cmd, relative_workdir, startup=None, interactive=False, num_sequence=1,
                env=None, job_name=None, shell='bash', dry_run=False, sweeping=False) -> None:
        # TODO: I should notice that SSHMachine.execute, SlurmMachin.execute, and DockerMachine.execute don't really share arguments.
        # Should treat them as separate classes.
        from simple_slurm_command import SlurmCommand
        env = {} if env is None else env

        if isinstance(cmd, list):
            cmd = ' '.join(cmd)

        if num_sequence > 1:
            # Use sbatch and set dependency to singleton
            self.slurm_conf.dependency = 'singleton'
            if interactive:
                logger.warn('num_sequence is set to {n_sequence} > 1. Force disabling interactive mode')
                interactive = False

        # Obtain slurm cli command
        s = self.slurm_conf

        if job_name is None:
            import randomname
            import random
            proj_name_maxlen = 15
            rand_num = random.randint(0, 100)
            job_name = f'rmx-{self.project.name[:proj_name_maxlen]}-{randomname.get_name()}-{rand_num}'
        slurm_command = SlurmCommand(cpus_per_task=s.cpus_per_task,
                                     job_name=job_name,
                                     partition=s.partition,
                                     time=s.time,
                                     exclude=s.exclude,
                                     constraint=s.constraint,
                                     dependency=s.dependency,
                                     output=s.output,
                                     error=s.error
                                     )

        if interactive and (s.output is not None):
            # User may expect stdout shown on the console.
            logger.info('--output/--error argument for Slurm is ignored in interactive mode.')

        # Hmmmm, Fabric seems to have an issue to handle envvar that contains spaces...
        # This is the issue of using "inline_ssh_env" that essentially sets envvars by putting bunch of export KEY=VAL before running shells.
        # The documentation clearly says developers need to handle shell escaping for non-trivial values.
        rmxenv = {'RMX_CODE_DIR': self.project.remote_dir, 'RMX_OUTPUT_DIR': self.project.remote_outdir, 'RMX_MOUNT_DIR': self.project.remote_mountdir, 'RMX_USER_COMMAND': cmd}
        env.update(rmxenv)

        if startup:
            cmd = f'{startup} && {cmd}'

        if interactive:
            # cmd = f'{shell} -i -c \'{cmd}\''
            # Create a temp bash file and put it on the remote server.
            workdir = self.project.remote_dir / relative_workdir
            logger.info('exec file: ' + f"#!/usr/bin/env {shell}\n{cmd}\n{shell}")
            from io import StringIO
            file_obj = StringIO(f"#!/usr/bin/env {shell}\n{cmd}\n{shell}")
            self.client.put(file_obj, workdir / '.srun-script.sh')
            cmd = slurm_command.srun('.srun-script.sh', pty=shell)

            logger.info(f'srun mode:\n{cmd}')
            logger.info(f'cd to {workdir}')
            return self.client.run(cmd, directory=workdir,
                                   disown=False, env=env, pty=True, dry_run=dry_run)
        else:
            cmd = slurm_command.sbatch(cmd, shell=f'/usr/bin/env {shell}')
            logger.info(f'sbatch mode:\n{cmd}')
            logger.info(f'cd to {self.project.remote_dir / relative_workdir}')

            cmd = '\n'.join([cmd] * num_sequence)
            result = self.client.run(cmd, directory=(self.project.remote_dir / relative_workdir),
                                     disown=False, env=env, dry_run=dry_run)
            if result.stderr:
                logger.warn('sbatch job submission failed:', result.stderr)
            jobid = result.stdout.strip().split()[-1]  # stdout format: Submitted batch job 8156833
            logger.debug(f'jobid {jobid}')


            if not dry_run:
                # TODO: store {jobid: (job_name, cmd, env, directory)} dictionary!
                # Maybe should check if there's other processes with same name (i.e., sequential jobs) are running?
                # TODO: Create an interface for the submitted jobs. It should also be used by "status" command.
                import json
                from .helpers import get_timestamp, read_timestamp
                from datetime import datetime
                now = datetime.now()

                # New entry
                new_entry = {'remote': self.client.remote_conf.get_dict(),
                             'timestamp': get_timestamp(),
                             'command': cmd,
                             'envvar': env,
                             'project': self.project.get_dict(),
                             'relative_workdir': relative_workdir,
                             'shell': shell,
                             'jobid': jobid,
                             'sweeping': sweeping}

                # Prepare jsonification
                from rmx.helpers import posixpath2str
                new_entry = posixpath2str(new_entry)

                launch_logfile = expandvars('$HOME/.rmx/launched.json')
                if os.path.isfile(launch_logfile):
                    with open(launch_logfile, 'r') as f:
                        data = json.load(f)
                else:
                    data = []

                # TODO: Use helpers.LaunchLogManager
                # file mode should be w+? a? a+?? Look at this: https://stackoverflow.com/a/58925279/7057866
                # with open(launch_logfile, 'w') as f:
                #     new_entries = [new_entry]

                #     # Remove whatever that is older than 30 hours
                #     for entry in data:
                #         dt = (now - read_timestamp(entry.get('timestamp'))).seconds
                #         if dt > 60 * 60 * 30:
                #             continue
                #         new_entries.append(entry)

                #     # Save new entries
                #     json.dump(new_entries, f)


class DockerMachine:
    """A class to execute any code for a specific project.
    """
    def __init__(self, docker_client: DockerClient, project: Project, docker_conf: DockerContainerConfig) -> None:
        self.client = docker_client
        self.project = project
        self.docker_conf = docker_conf

        project_rootdir = pjoin(RMX_DOCKER_ROOTDIR, project.name)
        self.codedir = pjoin(project_rootdir, 'code')
        self.outdir = pjoin(project_rootdir, 'output')
        self.mountdir = pjoin(project_rootdir, 'mount')

        # TODO: Pull the image if it doesn't exsit locally


    def execute(self, cmd, relative_workdir, startup=None, disown=False, shell=True, use_gpus=True, x_forward=False, env=None):
        env = {} if env is None else env

        # Using docker
        import docker
        from docker.types import Mount

        if shell:
            self.docker_conf.tty = True

        if use_gpus:
            self.docker_conf.use_gpus()

        # TODO: Fix it later
        if x_forward:
            raise KeyError('Docker X forwarding is not supported yet.')
            # self.docker_conf.use_x_forward(target_home=f'/home/{self.remote_conf.user}')

        if isinstance(cmd, list):
            cmd = ' '.join(cmd) if len(cmd) > 1 else cmd[0]

        # Mount project dir
        rmxenv = {'RMX_CODE_DIR': self.codedir, 'RMX_OUTPUT_DIR': self.outdir, 'RMX_MOUNT_DIR': self.mountdir, 'RMX_USER_COMMAND': cmd}
        allenv = {**rmxenv, **env, **self.docker_conf.environment}
        allenv = replace_rmx_envvars(allenv)
        self.docker_conf.environment = allenv
        self.docker_conf.mounts += [Mount(target=self.codedir, source=self.project.remote_dir, type='bind'),
                                    Mount(target=self.outdir, source=self.project.remote_outdir, type='bind'),
                                    Mount(target=self.mountdir, source=self.project.remote_mountdir, type='bind')]
        logger.info(f'mounts: {self.docker_conf.mounts}')
        logger.info(f'docker_conf: {self.docker_conf}')

        if self.docker_conf.tty:
            startup = 'sleep 2' if startup is None else startup
            cmd = f'/bin/bash -c \'{startup} && {cmd} && chmod -R a+rw {self.outdir} \''
        logger.info(f'docker run with command:{cmd}')

        logger.info(f'container codedir: {str(self.codedir / relative_workdir)}')
        # NOTE: Intentionally being super verbose to make arguments explicit.
        d = self.docker_conf
        container = self.client.containers.run(d.image,
                                               cmd,
                                               name=d.name,
                                               remove=d.remove,  # Keep it running as we need to change
                                               network=d.network,
                                               ipc_mode=d.ipc_mode,
                                               detach=d.detach,
                                               tty=d.tty,
                                               mounts=d.mounts,
                                               environment=d.environment,
                                               device_requests=d.device_requests,
                                               working_dir=str(self.codedir / relative_workdir),
                                               # entrypoint='/bin/bash -c "sleep 10 && xeyes"'  # Use it if you wanna overwrite entrypoint
                                               )
        logger.info(f'container: {container}')
        if disown:
            logger.warn('NOTE: disown is set to True. Output files will not be transported to your local directory.')
        else:
            # Block and listen to the stream from container
            stream = container.logs(stream=True, follow=True)
            logger.info('--- listening container stdout/stderr ---\n')
            for char in stream:
                # logger.info(char.decode('utf-8'))
                # 'ignore' ignores decode error that happens when multi-byte char is passed.
                print(char.decode('utf-8', 'ignore'), end='')

    def get_status(self, all=False):
        """List the status of containers associated to the project (it simply filters based on container name)

        Only returns running container as default. Set all=True if you want to have a full list.
        """

        # Filter by image name
        container_list = self.client.containers.list(all=all, filters={'ancestor': self.project.docker.image})
        return container_list

