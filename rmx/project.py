#!/usr/bin/env python3
import os
from os.path import join as pjoin
from os.path import expandvars

from rmx.config import DockerContainerConfig, SlurmConfig, SingularityConfig

REMOTE_ROOT_DIR = '/tmp'
LOCAL_OUTPUT_DIR = expandvars('${HOME}/.rmx/output')

# TODO: Maybe better to separate docker project / slurm project etc??
# The only diff will be whether to use self.docker_image / self.singularity_image
class Project:
    def __init__(self, name, local_dir, local_outdir=None, remote_root_dir=None) -> None:
        self.name = name
        self.local_dir = local_dir
        self.local_outdir = pjoin(LOCAL_OUTPUT_DIR, name) if local_outdir is None else local_outdir

        remote_root_dir = REMOTE_ROOT_DIR if remote_root_dir is None else remote_root_dir
        self.remote_dir = pjoin(remote_root_dir, name, 'code')
        self.remote_outdir = pjoin(remote_root_dir, name, 'output')
        self.remote_mountdir = pjoin(remote_root_dir, name, 'mount')

        self._make_directories()

    def _make_directories(self):
        import os
        os.makedirs(self.local_dir, exist_ok=True)
        os.makedirs(self.local_outdir, exist_ok=True)

    def get_dict(self):
        return {key: val for key, val in vars(self).items() if not (key.startswith('__') or callable(val))}

    def __repr__(self):
        return repr(f'<Project {self.name}>')
