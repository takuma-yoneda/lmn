from __future__ import annotations
from argparse import Namespace
from typing import Iterable
import threading
from rmx import logger
from docker import DockerClient
from rmx.config import DockerContainerConfig
from rmx.machine import SimpleSSHClient
from rmx.helpers import replace_rmx_envvars

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
        rmxenv = get_rmxenvs(cmd, self.rmxdirs)
        allenv = {**env, **rmxenv}
        allenv = {key: replace_rmx_envvars(val, rmxenv) for key, val in allenv.items()}
        return self.client.run(cmd, directory=(self.rmxdirs.codedir / relative_workdir),
                               disown=disown, env=allenv, dry_run=dry_run, pty=True)


class DockerRunner:
    def __init__(self, client: DockerClient, rmxdirs: Namespace) -> None:
        self.client = client
        self.rmxdirs = rmxdirs

    def exec(self, cmd: str, relative_workdir, docker_conf: DockerContainerConfig,
             kill_existing_container: bool = True, interactive: bool = True, quiet: bool = False) -> None:

        rmxenv = get_rmxenvs(cmd, self.rmxdirs)
        allenv = {**docker_conf.env, **rmxenv}
        allenv = {key: replace_rmx_envvars(val, rmxenv) for key, val in allenv.items()}

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
                logger.warn(f'Removing the existing container: {docker_conf.name}')
                container.remove(force=True)

        # NOTE: Intentionally being super verbose to make arguments explicit.
        d = docker_conf
        if interactive:
            logger.info('interactive is True. Force setting detach=False, tty=True, stdin_open=True.')
            container = self.client.containers.create(d.image,
                                                cmd,
                                                name=d.name,
                                                network=d.network,
                                                ipc_mode=d.ipc_mode,
                                                detach=False,
                                                tty=True,
                                                stdin_open=True,
                                                mounts=d.mounts,
                                                environment=allenv,
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
                                                detach=True,
                                                tty=False,  # If True, stdout/stderr are mixed
                                                stdin_open=True,  # It's useful to keep it open, as you may manually attach the container later
                                                mounts=d.mounts,
                                                environment=allenv,
                                                device_requests=d.device_requests,
                                                working_dir=str(self.rmxdirs.codedir / relative_workdir),
                                                # entrypoint='/bin/bash -c "sleep 10 && xeyes"'  # Use it if you wanna overwrite entrypoint
                                                )
            logger.info(f'container: {container}')

            def log_stream(stream: Iterable):
                """print out log stream"""
                for char in stream:
                    # logger.info(char.decode('utf-8'))
                    # 'ignore' ignores decode error that happens when multi-byte char is passed.
                    print(char.decode('utf-8', 'ignore'), end='')


            if quiet:
                pass

                # Attach log stream in a separate thread, and only output stderr
                # stream = container.logs(stdout=False, stderr=True, stream=True, follow=True)
                # logger.info('quiet is True, only listening to stderr.')
                # logger.info('--- listening container stderr ---\n')
                # thr = threading.Thread(target=log_stream, args=(stream, ))
                # thr.start()
                # thr.join()  # This blocks forever ;P

            else:
                # Block and listen to the stream from container
                stream = container.logs(stream=True, follow=True)
                logger.info('--- listening container stdout/stderr ---\n')
                log_stream(stream)


class SlurmRunner:
    """Use srun/sbatch to submit the command on a remote machine.
    If your local machine has slurm (i.e., you're on slurm login-node), I guess you don't need this tool.
    Thus SlurmMachine inherits SSHMachine.
    """
    def __init__(self, client: SimpleSSHClient, rmxdirs: Namespace) -> None:
        self.client = client
        self.rmxdirs = rmxdirs

    def exec(self, cmd: str, relative_workdir, slurm_conf, env: dict | None = None,
             startup: str = "", num_sequence: int = 1,
             interactive: bool = None, dry_run: bool = False):
        from simple_slurm_command import SlurmCommand
        env = {} if env is None else env

        if isinstance(cmd, list):
            cmd = ' '.join(cmd)

        if num_sequence > 1:
            # Use sbatch and set dependency to singleton
            slurm_conf.dependency = 'singleton'
            if interactive:
                logger.warn(f'num_sequence is set to {num_sequence} (> 1). Force disabling interactive mode')
                interactive = False

        s = slurm_conf
        slurm_command = SlurmCommand(cpus_per_task=s.cpus_per_task,
                                     job_name=s.job_name,
                                     partition=s.partition,
                                     time=s.time,
                                     exclude=s.exclude,
                                     constraint=s.constraint,
                                     dependency=s.dependency,
                                     output=s.output,
                                     error=s.error)

        if interactive and (s.output is not None):
            # User may expect stdout shown on the console.
            logger.info('--output/--error argument for Slurm is ignored in interactive mode.')

        rmxenv = get_rmxenvs(cmd, self.rmxdirs)
        allenv = {**env, **rmxenv}
        allenv = {key: replace_rmx_envvars(val, rmxenv) for key, val in allenv.items()}

        if startup:
            cmd = f'{startup} && {cmd}'

        if interactive:
            # cmd = f'{shell} -i -c \'{cmd}\''
            # Create a temp bash file and put it on the remote server.
            workdir = self.rmxdirs.codedir / relative_workdir
            logger.info('exec file: ' + f"#!/usr/bin/env {s.shell}\n{cmd}\n{s.shell}")
            from io import StringIO
            file_obj = StringIO(f"#!/usr/bin/env {s.shell}\n{cmd}\n{s.shell}")
            self.client.put(file_obj, workdir / '.srun-script.sh')
            cmd = slurm_command.srun('.srun-script.sh', pty=s.shell)

            logger.info(f'srun mode:\n{cmd}')
            logger.info(f'cd to {workdir}')
            return self.client.run(cmd, directory=workdir,
                                   disown=False, env=allenv, pty=True, dry_run=dry_run)
        else:
            cmd = slurm_command.sbatch(cmd, shell=f'/usr/bin/env {s.shell}')
            logger.info(f'sbatch mode:\n{cmd}')
            logger.info(f'cd to {self.rmxdirs.codedir / relative_workdir}')

            cmd = '\n'.join([cmd] * num_sequence)
            result = self.client.run(cmd, directory=(self.rmxdirs.codedir / relative_workdir),
                                     disown=False, env=allenv, dry_run=dry_run)
            if result.stderr:
                logger.warn('sbatch job submission failed:', result.stderr)
            jobid = result.stdout.strip().split()[-1]  # stdout format: Submitted batch job 8156833
            logger.debug(f'jobid {jobid}')
