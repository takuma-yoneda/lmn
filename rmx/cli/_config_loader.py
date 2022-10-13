"""Load config file and fuse it with runtime options"""
from __future__ import annotations
from argparse import Namespace
import os
import pathlib
from pathlib import Path
from rmx import logger
from rmx.helpers import find_project_root, parse_config
from posixpath import expandvars
from dotenv import dotenv_values

from rmx.machine import RemoteConfig

DOCKER_ROOT_DIR = '/rmx'
REMOTE_ROOT_DIR = '/tmp'

class Project:
    """Maintains the info specific to the local project"""
    def __init__(self, name, rootdir, outdir=None, exclude=None, startup: str = "", 
                 mount_dirs: dict | None = None, mount_from_host: dict | None = None,
                 env: dict | None = None) -> None:
        self.name = name
        self.rootdir = Path(rootdir)
        self.outdir = self.rootdir / ".output" if outdir is None else outdir
        self.exclude = exclude
        self.startup = startup
        self.env = env if env is not None else {}
        self.mount_dirs = mount_dirs if mount_dirs is not None else {}
        self.mount_from_host = mount_from_host if mount_from_host is not None else {}

        self._make_directories()

    def _make_directories(self):
        self.rootdir.mkdir(parents=True, exist_ok=True)
        self.outdir.mkdir(parents=True, exist_ok=True)

    def get_dict(self):
        return {key: val for key, val in vars(self).items() if not (key.startswith('__') or callable(val))}

    def __repr__(self):
        return repr(f'<Project {self.name}>')


class Machine:
    """Maintains machine configuration.
    - RemoteConfig (user, hostname, uri)
    """
    def __init__(self, remote_conf: RemoteConfig, rmxdir,
                 startup: str = "",
                 env: dict | None = None,
                 parsed_conf: dict | None = None) -> None:
        self.remote_conf = remote_conf
        self.rmxdir = Path(rmxdir)
        self.env = env if env is not None else {}
        self.startup = startup
        self.parsed_conf = parsed_conf

        # aliases
        self.user = remote_conf.user
        self.host = remote_conf.host
        self.base_uri = remote_conf.base_uri

    def uri(self, path) -> str:
        """Returns user@hostname:path"""
        return f'{self.remote_conf.base_uri}:{path}'
    
    def get_rmxdirs(self, project_name: str) -> Namespace:
        rootdir = self.rmxdir / project_name
        return Namespace(
            codedir=str(rootdir / 'code'),
            mountdir=str(rootdir / 'mount'),
            outdir=str(rootdir / 'output')
        )


def get_docker_rmxdirs(rmxdir: Path | str, project_name: str) -> Namespace:
        rootdir = Path(rmxdir) / project_name
        return Namespace(
            codedir=str(rootdir / 'code'),
            mountdir=str(rootdir / 'mount'),
            outdir=str(rootdir / 'output')
        )


def load_config(machine_name: str):
    proj_rootdir = find_project_root()
    config = parse_config(proj_rootdir)

    pconf = config.get('project', {})
    mconf = config['machines'].get(machine_name)

    name = pconf.get('name', proj_rootdir.stem)
    logger.info(f'Project name: {name}')
    logger.info(f'Project root directory: {proj_rootdir}')

    mount_dirs = pconf.get('mount', [])
    mount_from_host = pconf.get('mount_from_host', {})

    if 'mount' in mconf:
        mount_dirs = mconf.get('mount', [])
    if 'mount_from_host' in mconf:
        mount_from_host = mconf.get('mount_from_host', {})

    # Load extra env vars from .env.secret
    secret_env = dotenv_values((proj_rootdir / ".env.secret").resolve())
    if secret_env:
        logger.info(f'Loaded following envs from .env.secret: {dict(secret_env)}')

    project_env = pconf.get('environment', {})
    project = Project(name,
                      proj_rootdir,
                      outdir=pconf.get('outdir'),
                      exclude=pconf.get('exclude', []),
                      startup=pconf.get('startup', ""),
                      env={**project_env, **secret_env},
                      mount_dirs=mount_dirs,
                      mount_from_host=mount_from_host)

    
    if machine_name not in config['machines']:
        raise KeyError(
            f'Machine "{machine_name}" not found in the configuration. '
            f'Available machines are: {" ".join(config["machines"].keys())}'
        )
    user, host = mconf['user'], mconf['host']
    remote_conf = RemoteConfig(user, host)

    machine = Machine(remote_conf,
                      rmxdir=mconf.get('root_dir', f'{REMOTE_ROOT_DIR}/{remote_conf.user}'),
                      env=mconf.get('environment', {}),
                      parsed_conf=mconf)

    return project, machine
