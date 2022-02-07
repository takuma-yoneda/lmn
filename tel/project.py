#!/usr/bin/env python3
import os
from os.path import join as pjoin
from os.path import expandvars

REMOTE_ROOT_DIR = '/tmp'
LOCAL_OUTPUT_DIR = expandvars('${HOME}/.tel/output')

class Project:
    def __init__(self, name, root_dir, remote_dir=None, out_dir=None, remote_outdir=None) -> None:
        self.name = name
        self.root_dir = root_dir
        self.out_dir = pjoin(LOCAL_OUTPUT_DIR, name) if out_dir is None else out_dir
        self.remote_dir = pjoin(REMOTE_ROOT_DIR, name) if remote_dir is None else remote_dir
        self.remote_outdir = pjoin(self.remote_dir, 'output') if remote_outdir is None else remote_outdir

        self.docker_image = 'takumaynd/mltools'

        self._prepare_directories()

    def _prepare_directories(self):
        import os
        os.makedirs(self.root_dir, exist_ok=True)
        os.makedirs(self.out_dir, exist_ok=True)
