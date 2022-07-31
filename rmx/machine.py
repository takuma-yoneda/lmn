#!/usr/bin/env python3
import os
from os.path import expandvars
from rmx.helpers import posixpath2str, replace_rmx_envvars

from rmx import logger

RMX_DOCKER_ROOTDIR = '/rmx'

# NOTE: Should I have ssh-conf, slurm-conf and docker-conf separately??
# I guess RemoteConfig should ONLY store the info on how to login to the host?
# docker info and slurm info should really reside in project.
class RemoteConfig:
    """Represents a configuration to connect to a remote server.
    This is used by SimpleSSHClient.
    """

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


class SimpleSSHClient:
    """Given a remote config, this provides an interface to ssh into a remote machine.
    """
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


class SlurmMachine:
    """Use srun/sbatch to submit the command on a remote machine.

    If your local machine has slurm (i.e., you're on slurm login-node), I guess you don't need this tool.
    Thus SlurmMachine inherits SSHMachine.
    """
    def __init__(self, client: SimpleSSHClient, project, slurm_conf) -> None:
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
