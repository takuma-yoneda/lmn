#!/usr/bin/env python3
import subprocess
from rmx import logger

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

    cmd = f"rsync --info=progress2 --archive --compress {exclude_str} {options} {source_dir} {target_dir}"
    logger.info(f'running command: {cmd}')
    run_cmd(cmd, shell=True, dry_run=dry_run)
    logger.info("Sync finished!")


def run_cmd(cmd, get_output=False, shell=False, dry_run=False):

    if shell and isinstance(cmd, (list, tuple)):
        cmd = " ".join([str(s) for s in cmd])

    if dry_run:
        logger.info('dry_run:', cmd)
        return

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
        res = subprocess.run(cmd, shell=shell)
        return res.returncode
