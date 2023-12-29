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
        self.port = port

    @property
    def base_uri(self) -> str:
        return f'{self.user}@{self.host}'

    def get_dict(self):
        return {key: val for key, val in vars(self).items() if not (key.startswith('__') or callable(val))}


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