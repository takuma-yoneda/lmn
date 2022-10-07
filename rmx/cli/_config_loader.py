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
LOCAL_OUTPUT_DIR = expandvars('${HOME}/.rmx/output')

class Project:
    """Maintains the info specific to the local project"""
    def __init__(self, name, rootdir, outdir=None, exclude=None, startup: str = "", 
                 mount_dirs: dict | None = None, mount_from_host: dict | None = None,
                 env: dict | None = None) -> None:
        self.name = name
        self.rootdir = Path(rootdir)
        self.outdir = Path(LOCAL_OUTPUT_DIR) / name if outdir is None else outdir
        self.exclude = exclude
        self.startup = startup
        self.env = env if env is not None else {}
        self.mount_dirs = mount_dirs if mount_dirs is not None else {}
        self.mount_from_host = mount_from_host if mount_from_host is not None else {}

        self._make_directories()

    def _make_directories(self):
        # TODO: use pathlib API
        import os
        os.makedirs(self.rootdir, exist_ok=True)
        os.makedirs(self.outdir, exist_ok=True)

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


# class Docker:  # Docker Conf
#     def __init__(self, image, rmxdir) -> None:
#         self.rmxdir = Path(rmxdir)
#         self.image = image

#     def get_rmxdirs(self, project_name: str) -> Namespace:
#         rootdir = self.rmxdir / project_name
#         return Namespace(
#             codedir=str(rootdir / 'code'),
#             mountdir=str(rootdir / 'mount'),
#             outdir=str(rootdir / 'output')
#         )


# class Singularity:  # Singularity Conf
#     def __init__(self, image, overlay, rmxdir) -> None:
#         self.rmxdir = Path(rmxdir)
#         self.image = image
#         self.overlay = overlay

#     def get_rmxdirs(self, project_name: str) -> Namespace:
#         rootdir = self.rmxdir / project_name
#         return Namespace(
#             codedir=str(rootdir / 'code'),
#             mountdir=str(rootdir / 'mount'),
#             outdir=str(rootdir / 'output')
#         )


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


    # TODO: These machine-specific parts should go into each handler!
    # sync command does not have a notion of "mode" !!
    # mode = parsed.mode or mconf.get('default_mode')

    # docker = None
    # sconf = None
    # sing = None
    # if mode is None:
    #     logger.warn('mode is not set. Setting it to SSH mode')
    #     mode = 'ssh'
    # elif mode == 'docker':
    #     # Docker specific configurations
    #     image = parsed.image or mconf.get('docker', {}).get('image')
    #     if image is None:
    #         raise KeyError('docker image is not specified.')
    #     docker = Docker(image=image, rmxdir=DOCKER_ROOT_DIR)

    # elif mode == 'slurm' or mode == 'slurm-sing':
    #     # Slurm specific configurations
    #     from rmx.config import SlurmConfig
    #     import randomname
    #     import random
    #     if 'slurm' not in mconf:
    #         raise ValueError('Configuration must have an entry for "slurm" to use slurm mode.')

    #     proj_name_maxlen = 15
    #     rand_num = random.randint(0, 100)
    #     job_name = f'rmx-{project.name[:proj_name_maxlen]}-{randomname.get_name()}-{rand_num}'

    #     sconf = SlurmConfig(job_name, **mconf['slurm'])

    #     if mode == 'slurm-sing':
    #         # sconf = SlurmConfig(job_name, **mconf['slurm'])
    #         image = mconf.get('singularity', {}).get('sif_file')
    #         overlay = mconf.get('singularity', {}).get('overlay')

    #         # TODO: Use Docker to store singularity info
    #         sing = Singularity(image=image, overlay=overlay, rmxdir=DOCKER_ROOT_DIR)

    machine = Machine(remote_conf,
                      rmxdir=mconf.get('root_dir', f'{REMOTE_ROOT_DIR}/{remote_conf.user}'),
                      env=mconf.get('environment', {}),
                      parsed_conf=mconf)

    return project, machine
