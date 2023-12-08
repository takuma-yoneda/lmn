#!/usr/bin/env python3
from __future__ import annotations
import os
import subprocess
import sys
from lmn import logger
from lmn.machine import RemoteConfig
from typing import List, Optional, Union
from pathlib import Path


def rsync(source_dir: Union[Path, str], target_dir: Union[Path, str], remote_conf: RemoteConfig, options: Optional[List[str]] = None,
          exclude: Optional[List[str]] = None, dry_run: bool = False, transfer_rootdir: bool = True, to_local: bool = False):
    """
    source_dir: hoge/fuga/source-dir/content-files
    target_dir: Hoge/Fuga/target-dir

    if transfer_rootdir is True:
      target_dir: Hoge/Fuga/target-dir/source-dir/content-files

    else:
      target_dir: Hoge/Fuga/target-dir/content-files
    """
    import shutil
    exclude = [] if exclude is None else exclude
    options = [] if options is None else options

    # make sure rsync is installed
    if shutil.which("rsync") is None:
        raise RuntimeError("rsync command is not found.")

    source_dir = str(source_dir).rstrip('/') + ('' if transfer_rootdir else '/')
    target_dir = str(target_dir).rstrip('/') + '/'
    logger.info(f"Syncing code... ({remote_conf.base_uri}:{target_dir})")

    # TODO: Move the ControlPath to global config
    options += [f'-e "ssh -o \'ControlPath=~/.ssh/lmn-ssh-socket-{remote_conf.host}\'"']
    options += ['--archive', '--compress']
    options += [f'--exclude \'{ex}\'' for ex in exclude]
    options_str = ' '.join(options)
    if to_local:
        cmd = f"rsync {options_str} {remote_conf.base_uri}:{source_dir} {target_dir}"
    else:
        cmd = f"rsync {options_str} {source_dir} {remote_conf.base_uri}:{target_dir}"
    logger.debug(f'running command: {cmd}')

    if not dry_run:
        run_cmd(cmd, shell=True)
        logger.info("Sync finished!")


def run_cmd(cmd, get_output: bool = False, shell: bool = True, ignore_error: bool = False) -> Optional[str]:
    # TODO: 
    # - Do we ever need shell = False ??
    # - Do we ever need ignore_error = True ??
    # What is the downside of using get_output = True ??
    # - I guess you can't interact with the process with get_output = True.

    subprocess_env = dict(os.environ)
    logger.debug(f'running command: {cmd}')
    if get_output:
        result = subprocess.run(cmd, shell=shell, capture_output=True)
        if result.returncode != 0 and not ignore_error:
            stderr = result.stderr.read().decode('utf-8')
            msg = f"The command {cmd} returned exit code {result.returncode}\n---\n{stderr}\n---"
            raise RuntimeError(msg)
        return result.stdout.decode('utf-8').rstrip()

    result = subprocess.run(cmd,
                            stdout=sys.stdout, stderr=sys.stderr,
                            env=subprocess_env,
                            shell=shell)
    # Check for errors
    if not ignore_error and result.returncode != 0:
        msg = f"The command {cmd} returned exit code {result.returncode}"
        raise RuntimeError(msg)
