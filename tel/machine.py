#!/usr/bin/env python3
from abc import ABC
import os
from os.path import join as pJoin
from typing import Optional, Union
from tel.project import Project

DOCKER_WORKDIR = '/ws'
DOCKER_OUTDIR = '/output'

class RemoteConfig:
    from fabric import Connection
    def __init__(self, user, host, port=22) -> None:
        self.user = user
        self.host = host
        # self.port = port

    @property
    def base_uri(self) -> str:
        return f'{self.user}@{self.host}'

    def get_connection(self) -> Connection:
        from fabric import Connection
        from fabric.config import Config
        config = Config()
        config.user = self.user
        conn = Connection(host=self.host, config=config)
        return conn


class Machine(ABC):
    def __init__(self, project: Project, remote_conf: RemoteConfig) -> None:
        self.remote_conf = remote_conf
        self.project = project

    def execute(self, cmd, root_dir) -> bool:
        raise NotImplementedError()


class SSHMachine(Machine):
    def __init__(self, project: Project, remote_conf: RemoteConfig) -> None:
        super().__init__(project, remote_conf)

    def uri(self, path):
        return f'{self.remote_conf.base_uri}:{path}'

    # TODO: Do I need this??
    @property
    def remote_uri(self):
        return f'{self.remote_conf.base_uri}:{self.project.remote_dir}'

    def execute(self, cmd, root_dir, asynchronous=False) -> bool:
        conn = self.remote_conf.get_connection()

        if isinstance(cmd, list):
            cmd = ' '.join(cmd)

        print('remote command', cmd)
        with conn.cd(root_dir):
            if asynchronous:
                print('running asynchronously...')
                promise = conn.run(cmd, asynchronous=True)
                result = promise.join()
            else:
                result = conn.run(cmd)

        # Rsync remote outdir with the local outdir.
        if self.project.out_dir:
            from tel.cli.utils import rsync
            rsync(source_dir=self.uri(self.project.remote_outdir), target_dir=self.project.out_dir)

        return result


class DockerMachine(SSHMachine):
    """A class to execute any code for a specific project.

    You may wonder why project is fed to __init__ rather than to execute.
    This is because I have an intuition that this would allow us to use polymorphism more effectively.
    Conceptually, having a machine (DockerMachine) and project as a separate entity and
    feed them to a 'runner' may make sense, however, that will necessiate to use if statement
    to switch execution behavior (why? for example, docker-machine requires to use project.docker_image, while ssh-machine does not)
    Alright, then why not just feed project to DockerMachine.execute?
    First of all, as a premise, let's agree that "what docker image to use" should be defined in a 'project'.
    Then, the responsibility of DockerMahchine.execute() is 1. instantiate docker-client (this requires docker-image specification) and 2. actually run docker.
    If you code like that, what is the point of making an entire class for that? That can be done in a single function.
    Also there's no portability. When you instantiate docker_machine and try to execute another command, you go through the gigantic DockerMachine.execute again,
    and there's pretty much no resources that is reused.
    I believe at this point you understand the point of feeding project to __init__. Though, I accept the argument if this class should be called "Machine".
    """
    def __init__(self, project: Project, remote_conf: RemoteConfig ) -> None:
        import docker
        super().__init__(project, remote_conf)
        base_url = "ssh://" + self.remote_conf.base_uri
        self.client = docker.DockerClient(base_url=base_url, use_ssh_client=True)  # TODO: Make it singleton??

        self.workdir = pJoin(DOCKER_WORKDIR, project.name)
        self.outdir = pJoin(DOCKER_OUTDIR, project.name)

        assert self.project.docker_image, 'project.docker_image cannot be None when DockerMachine is used.'


    def execute(self, cmd, disown=False, shell=True):
        import docker
        from docker.types import Mount
        # https://github.com/docker/docker-py/issues/2395#issuecomment-907243275
        gpu = [
            docker.types.DeviceRequest(
                count=-1,
                capabilities=[['gpu'], ['nvidia'], ['compute'], ['compat32'], ['graphics'], ['utility'], ['video'], ['display']]
            )
        ]

        cmd = ' '.join(cmd)
        if shell:
            startup = 'sleep 2'
            cmd = f'/bin/bash -c \'{startup} && {cmd}\''
        print('command', cmd)

        # Mount project dir
        mounts = [Mount(target=self.workdir, source=self.project.remote_dir, type='bind'),
                  Mount(target=self.outdir, source=self.project.remote_outdir, type='bind')]
        envvars = {'TEL_PROJECT_ROOT': self.workdir, 'TEL_OUTPUT_ROOT': self.outdir}
        print('mounts', mounts)
        container = self.client.containers.run(self.project.docker_image, cmd,
                                               name=f'{self.remote_conf.user}-{self.project.name}',
                                               remove=True,
                                               network='host',
                                               ipc_mode='host',
                                               detach=True,
                                               tty=shell,
                                               mounts=mounts,
                                               environment=envvars,
                                               working_dir=self.workdir,
                                               device_requests=gpu)
        print('container', container)
        if disown:
            print('NOTE: disown is set to True. Generated files will not be transported to your local directory.')
        else:
            stream = container.logs(stream=True, follow=True)
            print('--- listening container stdout/stderr ---\n')
            for char in stream:
                print(char.decode('utf-8'), end='')

            # Rsync remote outdir with the local outdir.
            if self.project.out_dir:
                from tel.cli.utils import rsync
                print('Sending back generated files...')
                rsync(source_dir=self.uri(self.project.remote_outdir), target_dir=self.project.out_dir)


class SlurmMachine(Machine):
    def __init__(self, user, host, port=22) -> None:
        super().__init__(user, host, port=port)


class LocalMachine(Machine):
    def __init__(self, user, host, port=22) -> None:
        super().__init__(user, host, port=port)
