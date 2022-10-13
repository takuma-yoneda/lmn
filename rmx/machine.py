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
                logger.info(f'ssh client env: {env}')
                result = self.conn.run(cmd, asynchronous=False, hide=hide, env=env, pty=pty)
            return result

    def put(self, file_like, target_path=None):
        self.conn.put(file_like, str(target_path))

    def port_forward(self):
        raise NotImplementedError

    def x_forward(self):
        raise NotImplementedError
