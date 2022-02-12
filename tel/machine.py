#!/usr/bin/env python3
from abc import ABC
import os
from os.path import join as pJoin
from typing import Optional, Union
from tel.project import Project

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
        conn = Connection(host=self.host, config=config)
        return conn


class Machine(ABC):
    def __init__(self, project: Project, remote_conf: RemoteConfig) -> None:
        self.remote_conf = remote_conf
        self.project = project

    def execute(self, cmd, disown=False, shell=True, use_gpus=True, x_forward=False) -> bool:
        raise NotImplementedError()


class SSHMachine(Machine):
    def __init__(self, project: Project, remote_conf: RemoteConfig) -> None:
        super().__init__(project, remote_conf)

        # Get connection (necessary in any case)
        self.conn = self.remote_conf.get_connection()

        # Instantiate docker client if the project uses it
        if project.docker is not None:
            import docker
            base_url = "ssh://" + self.remote_conf.base_uri
            self.client = docker.DockerClient(base_url=base_url, use_ssh_client=True)  # TODO: Make it singleton??


    def uri(self, path):
        return f'{self.remote_conf.base_uri}:{path}'

    # TODO: Do I need this??
    @property
    def remote_uri(self):
        return f'{self.remote_conf.base_uri}:{self.project.remote_rootdir}'

    def execute(self, cmd, disown=False, shell=True, use_gpus=True, x_forward=False) -> bool:

        if self.project.docker is None:
            if isinstance(cmd, list):
                cmd = ' '.join(cmd) if len(cmd) > 1 else cmd[0]
            print('ssh run with command:', cmd)
            with self.conn.cd(self.project.remote_rootdir):
                promise = self.conn.run(cmd, asynchronous=True)
                result = promise.join()
                # NOTE: If you wanna run synchronously
                # result = self.conn.run(cmd)


            return result
        else:
            # Using docker
            assert self.project.docker is not None
            import docker
            from docker.types import Mount

            # Reflect the options
            self.project.docker.tty = True

            if use_gpus:
                self.project.docker.use_gpus()

            if x_forward:
                self.project.docker.use_x_forward(target_home=f'/home/{self.remote_conf.user}')

            # Mount project dir
            container_workdir = pJoin(DOCKER_WORKDIR, self.project.name)
            container_outdir = pJoin(DOCKER_OUTDIR, self.project.name)
            self.project.docker.mounts += [Mount(target=container_workdir, source=self.project.remote_rootdir, type='bind'),
                                           Mount(target=container_outdir, source=self.project.remote_outdir, type='bind')]
            self.project.docker.environment.update({'TEL_PROJECT_ROOT': container_workdir, 'TEL_OUTPUT_ROOT': container_outdir})
            print('mounts', self.project.docker.mounts)
            print(self.project.docker)

            if isinstance(cmd, list):
                cmd = ' '.join(cmd) if len(cmd) > 1 else cmd[0]
            if self.project.docker.tty:
                startup = 'sleep 2'
                cmd = f'/bin/bash -c \'{startup} && {cmd}\''
            print('docker run with command:', cmd)

            # NOTE: Intentionally being super verbose to make arguments explicit.
            container = self.client.containers.run(self.project.docker.image,
                                                cmd,
                                                name=f'{self.remote_conf.user}-{self.project.name}',
                                                remove=self.project.docker.remove,
                                                network=self.project.docker.network,
                                                ipc_mode=self.project.docker.ipc_mode,
                                                detach=self.project.docker.detach,
                                                tty=self.project.docker.tty,
                                                mounts=self.project.docker.mounts,
                                                environment=self.project.docker.environment,
                                                device_requests=self.project.docker.device_requests,
                                                working_dir=container_workdir
                                                # entrypoint='/bin/bash -c "sleep 10 && xeyes"'  # Use it if you wanna overwrite entrypoint
                                                )
            print('container', container)
            if disown:
                print('NOTE: disown is set to True. Output files will not be transported to your local directory.')
            else:
                stream = container.logs(stream=True, follow=True)
                print('--- listening container stdout/stderr ---\n')
                for char in stream:
                    print(char.decode('utf-8'), end='')


        # Rsync remote outdir with the local outdir.
        if self.project.out_dir:
            from tel.cli.utils import rsync
            rsync(source_dir=self.uri(self.project.remote_outdir), target_dir=self.project.out_dir)



class SlurmMachine(SSHMachine):
    """Use srun/sbatch to submit the command on a remote machine."""
    def __init__(self, project: Project, remote_conf: RemoteConfig) -> None:
        super().__init__(project, remote_conf)

    def execute(self, cmd, disown=False, shell=True, use_gpus=True, x_forward=False, batch=True) -> bool:
        from simple_slurm_command import SlurmCommand
        assert not x_forward, 'X11 forwarding is not supported in slurm'

        if isinstance(cmd, list):
            cmd = ' '.join(cmd)

        # Obtain slurm cli command
        s = self.project.slurm
        slurm_command = SlurmCommand(cpus_per_task=s.cpus_per_task,
                                     job_name=f'{self.remote_conf.user}-{self.project.name}',
                                     partition=s.partition,
                                     time=s.time,
                                     exclude=s.exclude,
                                     constraint=s.constraint,
                                     dependency=s.dependency,
                                     output=s.output)

        if batch:
            cmd = slurm_command.sbatch(cmd)
            print('sbatch\n', cmd)
        else:
            cmd = slurm_command.srun(cmd)
            print('srun\n', cmd)

        # Does it work??
        super().execute(cmd, self.project.root_dir)

class DockerMachine(SSHMachine):
    """A class to execute any code for a specific project.

    DEPRECATED: Use SSHMachine with project.docker config.

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
    def __init__(self, project: Project, remote_conf: RemoteConfig) -> None:
        super().__init__(project, remote_conf)

        import docker
        base_url = "ssh://" + self.remote_conf.base_uri
        self.client = docker.DockerClient(base_url=base_url, use_ssh_client=True)  # TODO: Make it singleton??

        self.workdir = pJoin(DOCKER_WORKDIR, project.name)
        self.outdir = pJoin(DOCKER_OUTDIR, project.name)

        assert self.project.docker, 'project.docker cannot be None when DockerMachine is used.'

        # TODO: Pull the image if it doesn't exsit locally


    def execute(self, cmd, disown=False, shell=True, use_gpus=True, x_forward=False):
        import docker
        from docker.types import Mount

        # Reflect the options
        self.project.docker.tty = shell

        if use_gpus:
            self.project.docker.use_gpus()

        if x_forward:
            self.project.docker.use_x_forward(target_home=f'/home/{self.remote_conf.user}')

        # Mount project dir
        self.project.docker.mounts += [Mount(target=self.workdir, source=self.project.remote_rootdir, type='bind'),
                                       Mount(target=self.outdir, source=self.project.remote_outdir, type='bind')]
        self.project.docker.environment.update({'TEL_PROJECT_ROOT': self.workdir, 'TEL_OUTPUT_ROOT': self.outdir})
        print('mounts', self.project.docker.mounts)
        print(self.project.docker)

        cmd = ' '.join(cmd)
        if self.project.docker.tty:
            startup = 'sleep 2'
            cmd = f'/bin/bash -c \'{startup} && {cmd}\''
        print('command', cmd)

        # NOTE: Intentionally being super verbose to make arguments explicit.
        container = self.client.containers.run(self.project.docker.image,
                                               cmd,
                                               name=f'{self.remote_conf.user}-{self.project.name}',
                                               remove=self.project.docker.remove,
                                               network=self.project.docker.network,
                                               ipc_mode=self.project.docker.ipc_mode,
                                               detach=self.project.docker.detach,
                                               tty=self.project.docker.tty,
                                               mounts=self.project.docker.mounts,
                                               environment=self.project.docker.environment,
                                               device_requests=self.project.docker.device_requests,
                                               working_dir=self.workdir,
                                               entrypoint='/bin/bash -c "sleep 10 && xeyes"'
                                               )
        print('container', container)
        if disown:
            print('NOTE: disown is set to True. Output files will not be transported to your local directory.')
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

    def get_status(self, all=False):
        """List the status of containers associated to the project (it simply filters based on container name)

        Only returns running container as default. Set all=True if you want to have a full list.
        """

        # Filter by image name
        container_list = self.client.containers.list(all=all, filters={'ancestor': self.project.docker.image})
        return container_list




class LocalMachine(Machine):
    def __init__(self, user, host, port=22) -> None:
        super().__init__(user, host, port=port)
