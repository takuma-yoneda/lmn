"""Load config file and fuse it with runtime options"""
from __future__ import annotations
from argparse import Namespace
import os
import pathlib
from pathlib import Path
from rmx import logger
from rmx.helpers import find_project_root, parse_config
from posixpath import expandvars

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
    def __init__(self, remote_conf: RemoteConfig, mode, rmxdir, 
                 startup: str = "",
                 env: dict | None = None, docker_conf: Docker | None = None) -> None:
        self.mode = mode
        self.remote_conf = remote_conf
        self.rmxdir = Path(rmxdir)
        self.env = env if env is not None else {}
        self.docker = docker_conf
        self.startup = startup

        if mode == 'docker' and docker_conf is None:
            raise KeyError('in docker mode, you must specify Docker config')

            
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

class Docker:  # Docker Conf
    def __init__(self, image, rmxdir) -> None:
        self.rmxdir = Path(rmxdir)
        self.image = image

    def get_rmxdirs(self, project_name: str) -> Namespace:
        rootdir = self.rmxdir / project_name
        return Namespace(
            codedir=str(rootdir / 'code'),
            mountdir=str(rootdir / 'mount'),
            outdir=str(rootdir / 'output')
        )


def load_config(parsed):
    if parsed.verbose:
        from logging import DEBUG
        logger.setLevel(DEBUG)

    proj_rootdir = find_project_root()
    config = parse_config(proj_rootdir)

    pconfig = config.get('project', {})

    name = pconfig.get('name', proj_rootdir.stem)
    logger.info(f'Project name: {name}')
    logger.info(f'Project root directory: {proj_rootdir}')

    # Runtime info
    curr_dir = pathlib.Path(os.getcwd()).resolve()
    rel_workdir = curr_dir.relative_to(proj_rootdir)
    logger.info(f'relative working dir: {rel_workdir}')  # cwd.relative_to(project_root)
    if isinstance(parsed.remote_command, list):
        cmd = ' '.join(parsed.remote_command)
    else:
        cmd = parsed.remote_command

    runtime_options = Namespace(dry_run=parsed.dry_run,
                                cmd=cmd,
                                rel_workdir=rel_workdir)

    project = Project(name,
                      proj_rootdir,
                      outdir=pconfig.get('outdir'),
                      exclude=pconfig.get('exclude', []),
                      startup=pconfig.get('startup', ""),
                      env=pconfig.get('environment', {}),
                      mount_dirs=pconfig.get('mounts', []),
                      mount_from_host=pconfig.get('mount_from_host', {})
                      )

    
    if parsed.machine not in config['machines']:
        raise KeyError(
            f'Machine "{parsed.machine}" not found in the configuration. '
            f'Available machines are: {" ".join(config["machines"].keys())}'
        )
    mconf = config['machines'].get(parsed.machine)
    user, host = mconf['user'], mconf['host']
    remote_conf = RemoteConfig(user, host)
    mode = parsed.mode or mconf.get('default_mode')

    docker = None
    if mode is None:
        logger.warn('mode is not set. Setting it to SSH mode')
        mode = 'ssh'
    elif mode == 'docker':
        # Create Docker Config
        image = parsed.image or mconf.get('docker', {}).get('name')
        if image is None:
            raise KeyError('docker image is not specified.')
        docker = Docker(image=image, rmxdir=DOCKER_ROOT_DIR)

    machine = Machine(remote_conf,
                      mode=mode,
                      rmxdir=mconf.get('root_dir', REMOTE_ROOT_DIR),
                      env=mconf.get('environment'),
                      docker_conf=docker)

    return project, machine, runtime_options