from __future__ import annotations
import threading
import time
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, List, Optional, Union
from tempfile import NamedTemporaryFile
from lmn import logger
from lmn.helpers import replace_lmn_envvars


if TYPE_CHECKING:
    from argparse import Namespace
    from docker import DockerClient
    from lmn.machine import CLISSHClient
    from lmn.container.docker import DockerContainerConfig
    from lmn.scheduler.pbs import PBSConfig


def get_lmnenvs(cmd: str, lmndirs: Namespace):
    envvars = {
        "LMN_CODE_DIR": lmndirs.codedir,
        "LMN_MOUNT_DIR": lmndirs.mountdir,
        "LMN_OUTPUT_DIR": lmndirs.outdir,
        "LMN_SCRIPT_DIR": lmndirs.scriptdir,
        # "LMN_USER_COMMAND": cmd,
    }

    # Provide RMX_* envvars for backward compatibility
    return {
        **envvars,
        **{key.replace("LMN", "RMX"): val for key, val in envvars.items()},
    }


class SSHRunner:
    def __init__(self, client: CLISSHClient, lmndirs: Namespace) -> None:
        self.client = client
        self.lmndirs = lmndirs

    def exec(self, cmd: str, relative_workdir, env: Optional[dict] = None, startup: Union[str, List[str]] = "", dry_run: bool = False):
        env = {} if env is None else env
        # if isinstance(cmd, list):
        #     cmd = ' '.join(cmd) if len(cmd) > 1 else cmd[0]

        if startup:
            if isinstance(startup, list):
                startup = ' ; '.join(startup)
            cmd = f'{startup} ; {cmd}'

        logger.debug(f'ssh run with command: {cmd}')
        if relative_workdir is not None:
            workdir = self.lmndirs.codedir / relative_workdir
            logger.debug(f'cd to {workdir}')
        else:
            workdir = None
        lmnenv = get_lmnenvs(cmd, self.lmndirs)
        allenv = {**env, **lmnenv}
        allenv = {key: replace_lmn_envvars(val, lmnenv) for key, val in allenv.items()}
        return self.client.run(cmd, directory=workdir, env=allenv, dry_run=dry_run)


class DockerRunner:
    def __init__(self, client: DockerClient, lmndirs: Namespace) -> None:
        self.client = client
        self.lmndirs = lmndirs

    def exec(self, cmd: str, relative_workdir, docker_conf: DockerContainerConfig, startup: str = "",
             kill_existing_container: bool = True, interactive: bool = True, quiet: bool = False,
             log_stderr_background: bool = False, use_cli: bool = True) -> None:

        if startup:
            raise NotImplementedError(
                "Currently startup command before launching the container is not supported.\n"
                "For now, you can only specify `startup` command that runs in the container."
            )

        if log_stderr_background:
            assert not interactive, 'log_stderr_background=True cannot be used with interactive=True'

        lmnenv = get_lmnenvs(cmd, self.lmndirs)
        allenv = {**docker_conf.env, **lmnenv}
        allenv = {key: replace_lmn_envvars(val, lmnenv) for key, val in allenv.items()}

        # NOTE: target: container, source: remote host
        logger.debug(f'mounts: {docker_conf.mount_from_host}')
        logger.debug(f'docker_conf: {docker_conf}')

        logger.debug(f'container codedir: {str(self.lmndirs.codedir / relative_workdir)}')

        if kill_existing_container:
            from docker.errors import NotFound
            try:
                container = self.client.containers.get(docker_conf.name)
            except NotFound:
                container = None

            if container:
                logger.warning(f'Removing the existing container: {docker_conf.name}')
                container.remove(force=True)

        # NOTE: Consider using Volume rather than Mount
        # - https://docker-py.readthedocs.io/en/1.2.3/volumes/
        # - https://docs.docker.com/storage/bind-mounts/

        # NOTE: Intentionally being super verbose to make arguments explicit.
        d = docker_conf

        # HACK: if docker.startup is a list, flatten it to a string
        if isinstance(docker_conf.startup, list):
            docker_conf.startup = ' ; '.join(docker_conf.startup)

        # TEMP: When running in non-interactive mode and command fails, container disappears before we attach to its log stream,
        # and thus we cannot observe its error message. A naive way to avoid it is to wait for a bit before command execution.
        if not interactive:
            if docker_conf.startup:
                docker_conf.startup = ' ; '.join((docker_conf.startup, 'sleep 2'))
            else:
                docker_conf.startup = 'sleep 2'

        if docker_conf.startup:
            cmd = ' ; '.join((docker_conf.startup, cmd))
        cmd = f'{cmd} && chmod -R a+r {str(self.lmndirs.outdir)}'

        assert d.tty

        # Use python-on-whales (i.e., docker cli)
        import python_on_whales
        whale_client = python_on_whales.DockerClient(host=f'ssh://{self.client.api._custom_adapter.ssh_host}')
        logger.debug(f'docker run with command: {cmd}')

        if interactive:
            try:
                whale_client.run(
                    d.image,
                    ['/bin/bash', '-c', cmd],
                    detach=False,
                    envs=allenv,
                    gpus=d.gpus,
                    interactive=interactive,
                    ipc=d.ipc_mode,
                    mounts=[('type=bind', f'source={src}', f'destination={tgt}') for src, tgt in d.mount_from_host.items()],
                    name=d.name,
                    networks=[d.network],
                    remove=True,
                    tty=True,
                    user=f'{d.user_id}:{d.group_id}',
                    workdir=str(self.lmndirs.codedir / relative_workdir),
                )
            except python_on_whales.exceptions.DockerException as e:
                # NOTE: hide error as the exception is also raised when the command in the container returns non-zero exit value.
                import traceback
                logger.debug(f'python_on_whales failed!!:\n{str(e)}')
                logger.debug(traceback.format_exc())

        else:
            # NOTE:
            # Ideally, I'd like to use python-on-whales for non-interactive mode as well,
            # but somehow its detached mode runs very slowly...

            # Handle mount here
            from docker.types import Mount
            from lmn.container.docker import get_gpu_device
            mounts = [Mount(target=tgt, source=src, type='bind') for src, tgt in docker_conf.mount_from_host.items()]

            cmd = f'/bin/bash -c \'{cmd}\''
            container = self.client.containers.run(
                d.image,
                cmd,
                name=d.name,
                remove=d.remove,  # Keep it running as we need to change
                network=d.network,
                ipc_mode=d.ipc_mode,
                detach=True,
                tty=(not log_stderr_background),  # If True, stdout/stderr are mixed, but I observed sometimes some stdout are not showing up when tty=False
                stdin_open=True,  # It's useful to keep it open, as you may manually attach the container later
                mounts=mounts,
                environment=allenv,
                device_requests=[get_gpu_device()],
                working_dir=str(self.lmndirs.codedir / relative_workdir),
                user=f'{d.user_id}:{d.group_id}',
            )
            logger.debug(f'container: {container}')

            def log_stream(stream: Iterable):
                """Print out log stream."""
                for char in stream:
                    # logger.info(char.decode('utf-8'))
                    # 'ignore' ignores decode error that happens when multi-byte char is passed.
                    print(char.decode('utf-8', 'ignore'), end='')

            if not quiet:
                if log_stderr_background:
                    # Attach log stream in a separate thread, and only output stderr
                    stream = container.logs(stdout=False, stderr=True, stream=True, follow=True)
                    logger.info('quiet is True, only listening to stderr.')
                    logger.info('--- listening container stderr ---\n')
                    thr = threading.Thread(target=log_stream, args=(stream, ))
                    thr.start()
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
    def __init__(self, client: CLISSHClient, lmndirs: Namespace) -> None:
        self.client = client
        self.lmndirs = lmndirs

    def exec(self, cmd: str, relative_workdir, conf, env: Optional[dict] = None,
             env_from_host: List[str] = [],
             startup: Union[str, List[str]] = "", timestamp: str = "", num_sequence: int = 1,
             interactive: bool = None, dry_run: bool = False):
        """
        Args:
            - env_from_host (List[str]): Used for Singularity, inherit specified envvars from host
        """
        from lmn.scheduler.slurm import SlurmCommand
        env = {} if env is None else env

        # TODO: Verify env_from_list contains valid environment variable names
        # (i.e., cannot have spaces or quotes!!)

        if isinstance(cmd, list):
            cmd = ' '.join(cmd)

        if num_sequence > 1:
            # Use sbatch and set dependency to singleton
            conf.dependency = 'singleton'
            if interactive:
                logger.warning(f'num_sequence is set to {num_sequence} (> 1). Force disabling interactive mode')
                interactive = False

        s = conf
        slurm_command = SlurmCommand(cpus_per_task=s.cpus_per_task,
                                     job_name=s.job_name,
                                     partition=s.partition,
                                     time=s.time,
                                     nodelist=s.nodelist,
                                     exclude=s.exclude,
                                     constraint=s.constraint,
                                     dependency=s.dependency,
                                     output=s.output,
                                     error=s.error)

        if interactive and (s.output is not None):
            # User may expect stdout shown on the console.
            # logger.info('--output/--error argument for Slurm is ignored in interactive mode.')
            pass

        lmnenv = get_lmnenvs(cmd, self.lmndirs)
        allenv = {**env, **lmnenv}
        allenv = {key: replace_lmn_envvars(val, lmnenv) for key, val in allenv.items()}

        if startup:
            if isinstance(startup, list):
                startup = ' ; '.join(startup)
            cmd = f'{startup} ; {cmd}'

        workdir = Path(self.lmndirs.codedir) / relative_workdir

        slurm_options = []
        import shlex
        exports = [f'export {key}={shlex.quote(str(val))}' for key, val in allenv.items()]

        # DEPRECATED
        if env_from_host:
            logger.warn('"env_from_host" in "slurm" or "pbs" is deprecated. Please move it inside "singularity".')
            # Only matters for Singularity
            exports += [
                *[f'export SINGULARITYENV_{envvar}=${envvar}' for envvar in env_from_host],
                *[f'export APPTAINERENV_{envvar}=${envvar}' for envvar in env_from_host],
            ]

        if not interactive:
            sbatch_cmd = slurm_command.sbatch(cmd, shell=f'/usr/bin/env {s.shell}')
            # HACK: rather than submitting with `sbatch << EOF\n ...\n EOF`,
            # I use `sbatch file-name` to avoid `$ENVVAR` to be evaluated right at the submission time
            # I want the `$ENVVAR` to be evaluated after the compute is allocated.
            slurm_options += sbatch_cmd.split('\n')[2:-2]  # Strip `sbatch << EOF`, '#!/usr/bin/env/ bash', {cmd} and `EOF`

        exec_str = '\n'.join((
            # f'#!/usr/bin/env {s.shell}',
            # NOTE: without `-S` option, `bash -i` will be considered a single command and will end up in command not found.
            # Reference: https://unix.stackexchange.com/a/657774/556831
            f'#!/usr/bin/env -S {s.shell} -i',
            *slurm_options,
            *exports,
            cmd
        ))
        logger.debug(f'\n=== execution string ===\n{exec_str}\n========================')

        script_fpath = Path(self.lmndirs.scriptdir) / f'.script-{timestamp}.sh'
        with NamedTemporaryFile(mode='w+') as temp_file:
            temp_file.write(exec_str)  # Write the string to the temporary file
            temp_file.flush()  # This is necessary!!
            self.client.put(temp_file.name, script_fpath)

        if interactive:
            cmd = slurm_command.srun(str(script_fpath), pty=s.shell)
        else:
            cmd = '\n'.join([f'sbatch {script_fpath}'] * num_sequence)

        try:
            self.client.run(cmd, directory=workdir, dry_run=dry_run)
        except RuntimeError as e:
            # NOTE: hide error as the exception is also raised when the command in the container returns non-zero exit value.
            import traceback
            logger.debug(f'self.client.run(...) failed!!:\n{str(e)}')
            logger.debug(traceback.format_exc())


class PBSRunner:
    def __init__(self, client: CLISSHClient, lmndirs: Namespace) -> None:
        self.client = client
        self.lmndirs = lmndirs

    def exec(self, cmd: str, relative_workdir, conf: PBSConfig, env: Optional[dict] = None,
             env_from_host: List[str] = [],
             startup: Union[str, List[str]] = "", timestamp: str = "", num_sequence: int = 1,
             interactive: bool = None, dry_run: bool = False):
        from lmn.scheduler.pbs import PBSCommand
        env = {} if env is None else env

        assert num_sequence == 1, 'Jobs with dependency are not supported on PBS mode yet.'

        if isinstance(cmd, list):
            cmd = ' '.join(cmd)

        lmnenv = get_lmnenvs(cmd, self.lmndirs)
        allenv = {**env, **lmnenv}
        allenv = {key: replace_lmn_envvars(val, lmnenv) for key, val in allenv.items()}

        if startup:
            # Use ';' rather than '&&' to avoid error when startup fails
            if isinstance(startup, list):
                startup = ' ; '.join(startup)
            cmd = f'{startup} ; {cmd}'

        workdir = Path(self.lmndirs.codedir) / relative_workdir

        slurm_options = []
        import shlex
        exports = [f'export {key}={shlex.quote(str(val))}' for key, val in allenv.items()]

        # DEPRECATED
        if env_from_host:
            logger.warn('"env_from_host" in "slurm" or "pbs" is deprecated. Please move it inside "singularity".')
            # Only matters for Singularity
            exports += [
                *[f'export SINGULARITYENV_{envvar}=${envvar}' for envvar in env_from_host],
                *[f'export APPTAINERENV_{envvar}=${envvar}' for envvar in env_from_host],
            ]

        if not interactive:
            qsub_cmd = PBSCommand.qsub(cmd, conf, interactive=False)
            # HACK: rather than submitting with `sbatch << EOF\n ...\n EOF`,
            # I use `sbatch file-name` to avoid `$ENVVAR` to be evaluated right at the submission time
            # I want the `$ENVVAR` to be evaluated after the compute is allocated.
            slurm_options += qsub_cmd.split('\n')[1:-2]  # Strip `qsub << EOF`, {cmd} and `EOF`

        exec_str = '\n'.join((
            # NOTE: without `-S` option, `bash -i` will be considered a single command and will end up in command not found.
            # Reference: https://unix.stackexchange.com/a/657774/556831
            '#!/usr/bin/env -S bash -i',
            *slurm_options,
            *exports,
            cmd
        ))
        logger.debug(f'\n=== execution string ===\n{exec_str}\n========================')

        script_fpath = Path(self.lmndirs.scriptdir) / f'.script-{timestamp}.sh'
        with NamedTemporaryFile(mode='w+') as temp_file:
            temp_file.write(exec_str)  # Write the string to the temporary file
            temp_file.flush()  # This is necessary!!
            self.client.put(temp_file.name, script_fpath)

        if interactive:
            cmd = PBSCommand.qsub(str(script_fpath),
                                  conf,
                                  qsub_cmd=f'chmod +x {script_fpath} && qsub',  # HACK to make the script executable
                                  interactive=True)
        else:
            cmd = '\n'.join([f'qsub {script_fpath}'] * num_sequence)

        logger.debug(f'submission command: {cmd}')

        try:
            self.client.run(cmd, directory=workdir, dry_run=dry_run)
        except RuntimeError as e:
            # NOTE: hide error as the exception is also raised when the command in the container returns non-zero exit value.
            import traceback
            logger.debug(f'self.client.run(...) failed!!:\n{str(e)}')
            logger.debug(traceback.format_exc())