#!/usr/bin/env python3
import os
from os.path import join as pjoin
from os.path import expandvars

from lmd.config import DockerContainerConfig, SlurmConfig, SingularityConfig

REMOTE_ROOT_DIR = '/tmp'
LOCAL_OUTPUT_DIR = expandvars('${HOME}/.lmd/output')

# TODO: Maybe better to separate docker project / slurm project etc??
# The only diff will be whether to use self.docker_image / self.singularity_image
class Project:
    def __init__(self, name, root_dir, remote_dir=None, out_dir=None, remote_outdir=None) -> None:
        self.name = name
        self.root_dir = root_dir
        self.out_dir = pjoin(LOCAL_OUTPUT_DIR, name) if out_dir is None else out_dir
        self.remote_rootdir = pjoin(REMOTE_ROOT_DIR, name) if remote_dir is None else remote_dir
        self.remote_outdir = pjoin(self.remote_rootdir, 'output') if remote_outdir is None else remote_outdir

        self._make_directories()

    def _make_directories(self):
        import os
        os.makedirs(self.root_dir, exist_ok=True)
        os.makedirs(self.out_dir, exist_ok=True)

    def get_dict(self):
        return {key: val for key, val in vars(self).items() if not (key.startswith('__') or callable(val))}

    def __repr__(self):
        return repr(f'<Project {self.name}>')
