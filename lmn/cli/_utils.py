#!/usr/bin/env python3
import os
import subprocess
import sys
from lmn import logger

def rsync(source_dir, target_dir, options='', exclude=None, dry_run=False, transfer_rootdir=True):
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
    # make sure rsync is installed
    if shutil.which("rsync") is None:
        raise RuntimeError("rsync binary is not found.")
    # ---
    logger.info(f"Syncing code...")
    source_dir = str(source_dir).rstrip('/') + ('' if transfer_rootdir else '/')
    target_dir = str(target_dir).rstrip('/') + '/'

    exclude_str = ' '.join(f'--exclude \'{ex}\'' for ex in exclude)

    # cmd = f"rsync --info=progress2 --archive --compress {exclude_str} {options} {source_dir} {target_dir}"
    cmd = f"rsync --progress --stats --archive --compress {exclude_str} {options} {source_dir} {target_dir}"
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
    result = subprocess.run(cmd,
                            stdout=sys.stdout, stderr=sys.stderr,
                            env=subprocess_env,
                            shell=shell)
    # Check for errors
    if raise_on_error and result.returncode != 0:
        msg = "The command {} returned exit code {}".format(cmd, result.returncode)
        raise RuntimeError(msg)

    return result
