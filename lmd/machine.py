#!/usr/bin/env python3
from abc import ABC
import os
from os.path import join as pjoin
from typing import Optional, Union
from docker import DockerClient
from lmd.config import DockerContainerConfig
from lmd.project import Project

import fabric
import invoke

DOCKER_WORKDIR = '/ws'
DOCKER_OUTDIR = '/output'

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

class SSHClient:
    def __init__(self, remote_conf: RemoteConfig) -> None:
        self.remote_conf = remote_conf
        self.conn = self.remote_conf.get_connection()

    def uri(self, path):
        return f'{self.remote_conf.base_uri}:{path}'

    def run(self, cmd, directory='$HOME', disown=False, hide=False, env=None, pty=False, dry_run=False):
        # TODO: Check if $HOME would work or not!!
        env = {} if env is None else env

        if dry_run:
            print('--- dry run ---')
            print('cmd:', cmd)
            print(locals())
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
                print('ssh client env', env)
                result = self.conn.run(cmd, asynchronous=False, hide=hide, env=env, pty=pty)
            return result

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

        print('ssh run with command:', cmd)
        print('cd to', self.project.remote_rootdir / relative_workdir)
        lmdenv = {'LMD_ROOT_DIR': self.project.remote_rootdir, 'LMD_OUTPUT_DIR': self.project.remote_outdir}
        env.update(lmdenv)
        return self.client.run(cmd, directory=(self.project.remote_rootdir / relative_workdir),
                               disown=disown, env=env, dry_run=dry_run)


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
                env=None, job_name=None, shell='bash', dry_run=False) -> None:
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
                print('WARN: num_sequence is set to {n_sequence} > 1. Force disabling interactive mode')
                interactive = False

        # Obtain slurm cli command
        s = self.slurm_conf

        if job_name is None:
            import randomname
            import random
            proj_name_maxlen = 15
            rand_num = random.randint(0, 100)
            job_name = f'lmd-{self.project.name[:proj_name_maxlen]}-{randomname.get_name()}-{rand_num}'
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
            print('--output/--error argument for Slurm is ignored in interactive mode.')

        lmdenv = {'LMD_ROOT_DIR': self.project.remote_rootdir, 'LMD_OUTPUT_DIR': self.project.remote_outdir}
        env.update(lmdenv)

        if startup:
            cmd = f'{startup} && {cmd}'

        if interactive:
            cmd = f'{shell} -i -c \'{cmd}\''
            cmd = slurm_command.srun(cmd, pty=shell)

            print('srun mode:\n', cmd)
            print('cd to', self.project.remote_rootdir / relative_workdir)
            return self.client.run(cmd, directory=(self.project.remote_rootdir / relative_workdir),
                                   disown=False, env=env, pty=True, dry_run=dry_run)
        else:
            cmd = slurm_command.sbatch(cmd, shell=f'/usr/bin/env {shell}')
            print('sbatch mode:\n', cmd)
            print('cd to', self.project.remote_rootdir / relative_workdir)

            cmd = '\n'.join([cmd] * num_sequence)
            self.client.run(cmd, directory=(self.project.remote_rootdir / relative_workdir),
                            disown=False, env=env, dry_run=dry_run)


class DockerMachine:
    """A class to execute any code for a specific project.
    """
    def __init__(self, docker_client: DockerClient, project: Project, docker_conf: DockerContainerConfig) -> None:
        self.client = docker_client
        self.project = project
        self.docker_conf = docker_conf

        self.workdir = pjoin(DOCKER_WORKDIR, project.name)
        self.outdir = pjoin(DOCKER_OUTDIR, project.name)

        # TODO: Pull the image if it doesn't exsit locally


    def execute(self, cmd, relative_workdir, startup=None, disown=False, shell=True, use_gpus=True, x_forward=False, env=None):
        env = [] if env is None else env

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

        # Mount project dir
        container_workdir = pjoin(DOCKER_WORKDIR)
        container_outdir = pjoin(DOCKER_OUTDIR)
        self.docker_conf.mounts += [Mount(target=container_workdir, source=self.project.remote_rootdir, type='bind'),
                                        Mount(target=container_outdir, source=self.project.remote_outdir, type='bind')]
        self.docker_conf.environment.update({'LMD_PROJECT_ROOT': container_workdir, 'LMD_OUTPUT_ROOT': container_outdir})
        self.docker_conf.environment.update(env)
        print('mounts', self.docker_conf.mounts)
        print('docker_conf', self.docker_conf)

        if isinstance(cmd, list):
            cmd = ' '.join(cmd) if len(cmd) > 1 else cmd[0]
        if self.docker_conf.tty:
            startup = 'sleep 2' if startup is None else startup
            cmd = f'/bin/bash -c \'{startup} && {cmd} && chmod -R a+rw {container_outdir} \''
        print('docker run with command:', cmd)

        print('container workdir:', str(container_workdir / relative_workdir))
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
                                               working_dir=str(container_workdir / relative_workdir),
                                               # entrypoint='/bin/bash -c "sleep 10 && xeyes"'  # Use it if you wanna overwrite entrypoint
                                               )
        print('container', container)
        if disown:
            print('NOTE: disown is set to True. Output files will not be transported to your local directory.')
        else:
            # Block and listen to the stream from container
            stream = container.logs(stream=True, follow=True)
            print('--- listening container stdout/stderr ---\n')
            for char in stream:
                print(char.decode('utf-8'), end='')

    def get_status(self, all=False):
        """List the status of containers associated to the project (it simply filters based on container name)

        Only returns running container as default. Set all=True if you want to have a full list.
        """

        # Filter by image name
        container_list = self.client.containers.list(all=all, filters={'ancestor': self.project.docker.image})
        return container_list

