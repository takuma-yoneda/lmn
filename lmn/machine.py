#!/usr/bin/env python3
import os
from os.path import expandvars
from subprocess import CompletedProcess
from lmn.helpers import posixpath2str, replace_lmn_envvars
from typing import Optional
import invoke

from lmn import logger

LMN_DOCKER_ROOTDIR = '/lmn'

# NOTE: Should I have ssh-conf, slurm-conf and docker-conf separately??
# I guess RemoteConfig should ONLY store the info on how to login to the host?
# docker info and slurm info should really reside in project.
class RemoteConfig:
    """Represents a configuration to connect to a remote server.
    This is used by SimpleSSHClient.
    """
    def __init__(self, user, host, port=22) -> None:
        self.user = user
        self.host = host
        self._conn = None
        # self.port = port

    @property
    def base_uri(self) -> str:
        return f'{self.user}@{self.host}'

    # from fabric import Connection
    # def get_connection(self) -> Connection:
    #     from fabric import Connection
    #     from fabric.config import Config
    #     config = Config()
    #     config.user = self.user
    #     conn = Connection(host=self.host, config=config, inline_ssh_env=True)
    #     conn.client.load_system_host_keys()

    #     # if self.auth_interactive_dumb:
    #     #     from lmn.helper.ssh import overwrite_auth_fn
    #     #     conn.client = overwrite_auth_fn(conn.client)

    #     self._conn = conn
    #     return conn

    def get_dict(self):
        return {key: val for key, val in vars(self).items() if not (key.startswith('__') or callable(val))}

    def __del__(self):
        # Close the connection when the instance is destructed
        if self._conn:
            self._conn.close()


class CLISSHClient:
    """Use the native CLI to run ssh"""
    def __init__(self, remote_conf: RemoteConfig) -> None:
        self.remote_conf = remote_conf
        # self.conn = self.remote_conf.get_connection()

    def uri(self, path):
        return f'{self.remote_conf.base_uri}:{path}'

    def run(self, cmd, directory="$HOME", env=None, capture_output: bool = False, dry_run: bool = False) -> Optional[str]:
        from lmn.cli._utils import run_cmd
        import re
        """
        Args:
            - capture_output (bool): If True, the returned object will contain stdout/stderr, **but the process won't be interactive**
        """
        # TODO: remove `env`: handle that within `cmd`!
        # TODO: Support disown (if that is necessary for sweep)
        env = {} if env is None else env

        # Perform shell escaping for envvars
        # NOTE: Fabric seems to have an issue to handle envvar that contains spaces...
        # This is the issue of using "inline_ssh_env" that essentially sets envvars by putting bunch of export KEY=VAL before running shells.
        # The documentation clearly says developers need to handle shell escaping for non-trivial values.
        # TEMP: shell escaping only when env contains space
        import shlex
        env = {key: shlex.quote(str(val)) if " " in str(val) else str(val) for key, val in env.items()}

        # NOTE:
        # -t: Force pseudo-terminal allocation.
        ssh_options = ['-t', f'-o ControlPath=~/.ssh/lmn-ssh-socket-{self.remote_conf.host}']
        ssh_base_cmd = f'ssh {" ".join(ssh_options)} {self.remote_conf.base_uri}'

        remote_cmds = []
        if directory is not None:
            remote_cmds += [f'cd {directory}']
            # ssh_cmd += f'cd {directory} && '
        remote_cmds += [cmd]

        ssh_cmd = f"{ssh_base_cmd} '{' && '.join(remote_cmds)}'"
        result = run_cmd(ssh_cmd, get_output=capture_output)
        return result

    def put(self, fpath, target_path=None) -> None:
        from lmn.cli._utils import run_cmd
        # TODO: Move the ControlPath to global config
        options = [f'-o ControlPath=$HOME/.ssh/lmn-ssh-socket-{self.remote_conf.host}']
        options = [os.path.expandvars(opt) for opt in options]
        cmd = ['scp', *options, fpath, f'{self.remote_conf.base_uri}:{target_path}']
        # Setting get_output=True has a side-effect of not showing stdout on the terminal
        run_cmd(cmd, shell=False, get_output=True)

    def port_forward(self):
        raise NotImplementedError

    def x_forward(self):
        raise NotImplementedError


# DEPRECATED
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

        # Perform shell escaping for envvars
        # NOTE: Fabric seems to have an issue to handle envvar that contains spaces...
        # This is the issue of using "inline_ssh_env" that essentially sets envvars by putting bunch of export KEY=VAL before running shells.
        # The documentation clearly says developers need to handle shell escaping for non-trivial values.
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
                logger.debug(f'ssh client env: {env}')
                try:
                    result = self.conn.run(cmd, asynchronous=False, hide=hide, env=env, pty=pty)
                except invoke.exceptions.UnexpectedExit as e:
                    logger.info(f'Caught an exception!!:\n{str(e)}')
                    import sys
                    sys.exit(1)

            return result

    def put(self, file_like, target_path=None):
        self.conn.put(file_like, str(target_path))

    def port_forward(self):
        raise NotImplementedError

    def x_forward(self):
        raise NotImplementedError
