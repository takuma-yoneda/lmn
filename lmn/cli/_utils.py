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
          exclude: Optional[List[str]] = None, dry_run: bool = False, transfer_rootdir: bool = True):
    """
    source_dir: hoge/fuga/source-dir/content-files
    target_dir: Hoge/Fuga/target-dir

    if transfer_rootdir is True:
      target_dir: Hoge/Fuga/target-dir/source-dir/content-files

    else:
      target_dir: Hoge/Fuga/target-dir/content-files
    """
    # TODO: replace with https://github.com/laktak/rsyncy (?)
    # ^ This one supports visualizing progress bar

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
    cmd = f"rsync {options_str} {source_dir} {remote_conf.base_uri}:{target_dir}"
    logger.debug(f'running command: {cmd}')

    if not dry_run:
        out = run_cmd(cmd, shell=True)
        logger.info("Sync finished!")

        if out.returncode != 0:
            raise OSError(f'The following rsync command failed:\n{out.args}\n\n{out.stderr.decode("utf-8")}')
        return out


def run_cmd(cmd, get_output=False, shell=False) -> subprocess.CompletedProcess:

    if shell and isinstance(cmd, (list, tuple)):
        cmd = " ".join([str(s) for s in cmd])

    if get_output:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=shell)
        proc.wait()
        if proc.returncode != 0:
            msg = "The command {} returned exit code {}".format(cmd, proc.returncode)
            raise RuntimeError(msg)
        out = proc.stdout.read().decode("utf-8").rstrip()
        logger.info(out)
        return out
    else:
        res = subprocess.run(cmd, shell=shell, capture_output=True)
        return res


def run_cmd2(cmd, shell: bool = True, raise_on_error: bool = False):
    subprocess_env = dict(os.environ)
    logger.debug(f'running command: {cmd}')
    result = subprocess.run(cmd,
                            stdout=sys.stdout, stderr=sys.stderr,
                            env=subprocess_env,
                            shell=shell)
    # Check for errors
    if raise_on_error and result.returncode != 0:
        msg = "The command {} returned exit code {}".format(cmd, result.returncode)
        raise RuntimeError(msg)

    return result
