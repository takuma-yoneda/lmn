"""Load config file and fuse it with runtime options"""
from __future__ import annotations
from argparse import Namespace
from typing import Optional, List, Union
import os
import pathlib
from pathlib import Path
from lmn import logger
from lmn.helpers import find_project_root, parse_config
from posixpath import expandvars

from lmn.machine import RemoteConfig

DOCKER_ROOT_DIR = '/lmn'

# NOTE: I used to set it to /tmp/lmn, but that causes an issue:
# When multiple users use this directory, the one who used this first set the permission of /tmp/lmn
# to be his/hers, thus others trying to use it later cannot access it.
REMOTE_ROOT_DIR = '/tmp'

class Project:
    """Maintains the info specific to the local project"""
    def __init__(self, name: str, rootdir: Union[str, Path],
                 outdir: Optional[str] = None, exclude: Optional[List[str]] = None,
                 startup: Union[str, List[str]] = "",
                 mount_from_host: Optional[dict] = None,
                 env: Optional[dict] = None) -> None:
        self.name = name
        self.rootdir = Path(rootdir)
        self.outdir = self.rootdir / ".output" if outdir is None else outdir
        self.exclude = exclude
        if isinstance(startup, list):
            startup = '; '.join(startup)
        self.startup = startup
        self.env = env if env is not None else {}
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
    def __init__(self, remote_conf: RemoteConfig, lmndir: Union[str, Path],
                 parsed_conf: dict,
                 startup: Union[str, List[str]] = "",
                 env: Optional[dict] = None) -> None:
        self.remote_conf = remote_conf
        self.lmndir = Path(lmndir)
        self.env = env if env is not None else {}
        startup = startup or parsed_conf.startup
        if isinstance(startup, list):
            startup = '; '.join(startup)
        self.startup = startup
        self.parsed_conf = parsed_conf

        # aliases
        self.user = remote_conf.user
        self.host = remote_conf.host
        self.base_uri = remote_conf.base_uri

    def uri(self, path) -> str:
        """Returns user@hostname:path"""
        return f'{self.remote_conf.base_uri}:{path}'
    
    def get_lmndirs(self, project_name: str) -> Namespace:
        rootdir = self.lmndir / project_name
        return Namespace(
            codedir=str(rootdir / 'code'),
            mountdir=str(rootdir / 'mount'),
            outdir=str(rootdir / 'output'),
            scriptdir=str(rootdir / 'script'),
        )


def get_docker_lmndirs(lmndir: Union[Path, str], project_name: str) -> Namespace:
    rootdir = Path(lmndir) / project_name
    return Namespace(
        codedir=str(rootdir / 'code'),
        mountdir=str(rootdir / 'mount'),
        outdir=str(rootdir / 'output'),
        scriptdir=str(rootdir / 'script'),
    )


def load_config(machine_name: str):
    from lmn.config import ProjectConfig, MachineConfig
    proj_rootdir = find_project_root()
    config = parse_config(proj_rootdir)

    # Error checking in config file
    if 'machines' not in config:
        logger.error('The configuration file does not contain "machines" section.')
        import sys; sys.exit(1)

    if machine_name not in config['machines']:
        logger.error(
            f'Machine "{machine_name}" not found in your configuration. \n'
            f'Available machines are: {", ".join(config["machines"].keys())}'
        )
        import sys; sys.exit(1)

    # TODO:
    # Both pconf and mconf can definitely be pydantic objects

    _pconf = config.get('project', {})
    pconf = ProjectConfig(**_pconf)
    if pconf.name is None:
        pconf.name = proj_rootdir.stem

    mconf = MachineConfig(**config['machines'][machine_name])

    # Parse special config params
    preset_conf = {
        'slurm-configs': config.get('slurm-configs', {}),
        'docker-images': config.get('docker-images', {}),
    }

    logger.info(f'Project name     : {pconf.name}')
    logger.info(f'Project directory: {proj_rootdir}')

    # Load extra env vars from .env.secret
    secret_env_path = (proj_rootdir / ".secret.env").resolve()
    if not secret_env_path.is_file():
        secret_env_path = (proj_rootdir / ".env.secret").resolve()

        # Just a friendly reminder
        if secret_env_path.is_file():
            logger.info('Reading from ".env.secret" file will be deprecated in the future. Please rename it to ".secret.env".')

    from dotenv import dotenv_values
    secret_env = dotenv_values(secret_env_path)

    if secret_env:
        logger.debug(f'Loaded the following envs from secret env file: {list(dict(secret_env).keys())}')

    # TODO: Project should take ProjectConfig object as an argument
    project = Project(pconf.name,
                      proj_rootdir,
                      outdir=pconf.outdir,
                      exclude=pconf.exclude,
                      startup=pconf.startup,
                      env={**pconf.environment, **secret_env},
                      mount_from_host={**pconf.mount_from_host, **mconf.mount_from_host})

    remote_conf = RemoteConfig(mconf.user, mconf.host)

    if mconf.root_dir is None:
        lmndir = f'{REMOTE_ROOT_DIR}/{remote_conf.user}/lmn'
    else:
        lmndir = f"{mconf.root_dir}/{remote_conf.user}"
    machine = Machine(remote_conf,
                      parsed_conf=mconf,
                      lmndir=lmndir,
                      env=mconf.environment)

    return project, machine, preset_conf
