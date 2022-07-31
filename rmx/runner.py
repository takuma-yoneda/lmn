from __future__ import annotations
from argparse import Namespace
from os import kill
from rmx import logger
from rmx.config import DockerContainerConfig
from rmx.machine import SimpleSSHClient
from docker import DockerClient

def get_rmxenvs(cmd: str, rmxdirs: Namespace):
    return {'RMX_CODE_DIR': rmxdirs.codedir, 
            'RMX_MOUNT_DIR': rmxdirs.mountdir,
            'RMX_OUTPUT_DIR': rmxdirs.outdir,
            'RMX_USER_COMMAND': cmd}


class SSHRunner:
    def __init__(self, client: SimpleSSHClient, rmxdirs: Namespace) -> None:
        self.client = client
        self.rmxdirs = rmxdirs

    def exec(self, cmd: str, relative_workdir, env: dict | None = None, startup: str = "", dry_run: bool = False,
             disown: bool = False):
        env = {} if env is None else env
        # if isinstance(cmd, list):
        #     cmd = ' '.join(cmd) if len(cmd) > 1 else cmd[0]

        if startup:
            cmd = f'{startup} && {cmd}'

        logger.info(f'ssh run with command: {cmd}')
        logger.info(f'cd to {self.rmxdirs.codedir / relative_workdir}')
        rmxenv = get_rmxenvs(self.rmxdirs, cmd)
        env.update(rmxenv)
        return self.client.run(cmd, directory=(self.rmxdirs.codedir / relative_workdir),
                               disown=disown, env=env, dry_run=dry_run, pty=True)


class DockerRunner:
    def __init__(self, client: DockerClient, rmxdirs: Namespace) -> None:
        self.client = client
        self.rmxdirs = rmxdirs

    def exec(self, cmd: str, relative_workdir, docker_conf: DockerContainerConfig,
             kill_existing_container: bool = True, interactive: bool = True) -> None:
        from rmx.helpers import replace_rmx_envvars

        rmxenv = get_rmxenvs(cmd, self.rmxdirs)
        allenv = {**rmxenv, **docker_conf.env}
        allenv = replace_rmx_envvars(allenv)

        # NOTE: target: container, source: remote host
        logger.info(f'mounts: {docker_conf.mounts}')
        logger.info(f'docker_conf: {docker_conf}')

        if docker_conf.tty:
            # TODO: Use dockerpty
            startup = 'sleep 2' if docker_conf.startup is None else startup
            cmd = f'/bin/bash -c \'{startup} && {cmd} && chmod -R a+rw {str(self.rmxdirs.outdir)} \''
        logger.info(f'docker run with command:{cmd}')

        logger.info(f'container codedir: {str(self.rmxdirs.codedir / relative_workdir)}')

        if kill_existing_container:
            from docker.errors import NotFound
            import docker
            try:
                container = self.client.containers.get(docker_conf.name)
            except NotFound:
                container = None
            
            if container:
                logger.info(f'Removing existing container: {docker_conf.name}')
                container.remove(force=True)

        # NOTE: Intentionally being super verbose to make arguments explicit.
        d = docker_conf
        if interactive:
            logger.warn('interactive is True. Force setting detach=False, tty=True, stdin_open=True.')
            container = self.client.containers.create(d.image,
                                                cmd,
                                                name=d.name,
                                                network=d.network,
                                                ipc_mode=d.ipc_mode,
                                                detach=False,
                                                tty=True,
                                                stdin_open=True,
                                                mounts=d.mounts,
                                                environment=d.environment,
                                                device_requests=d.device_requests,
                                                working_dir=str(self.rmxdirs.codedir / relative_workdir),
                                                # entrypoint='/bin/bash -c "sleep 10 && xeyes"'  # Use it if you wanna overwrite entrypoint
                                                )
            logger.info(f'container: {container}')
            import dockerpty
            dockerpty.start(self.client.api, container.id)
        else:
            container = self.client.containers.run(d.image,
                                                cmd,
                                                name=d.name,
                                                remove=d.remove,  # Keep it running as we need to change
                                                network=d.network,
                                                ipc_mode=d.ipc_mode,
                                                detach=d.detach,
                                                tty=d.tty,
                                                stdin_open=True,
                                                mounts=d.mounts,
                                                environment=d.environment,
                                                device_requests=d.device_requests,
                                                working_dir=str(self.rmxdirs.codedir / relative_workdir),
                                                # entrypoint='/bin/bash -c "sleep 10 && xeyes"'  # Use it if you wanna overwrite entrypoint
                                                )
            logger.info(f'container: {container}')
            # Block and listen to the stream from container
            stream = container.logs(stream=True, follow=True)
            logger.info('--- listening container stdout/stderr ---\n')
            for char in stream:
                # logger.info(char.decode('utf-8'))
                # 'ignore' ignores decode error that happens when multi-byte char is passed.
                print(char.decode('utf-8', 'ignore'), end='')


class SlurmRunner:
    def __init__(self) -> None:
        pass