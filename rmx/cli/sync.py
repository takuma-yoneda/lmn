from argparse import ArgumentParser, Namespace
from rmx import logger
from rmx.cli._utils import rsync
from rmx.cli._config_loader import Project, Machine
from rmx.machine import SimpleSSHClient


RSYNC_DESTINATION_PATH = "/tmp/".rstrip('/')


def _get_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument(
        "machine",
        action="store",
        type=str,
        help="Machine",
    )
    parser.add_argument(
        "--verbose",
        default=False,
        action="store_true",
        help="Be verbose"
    )
    return parser


def _sync_code(project: Project, machine: Machine, dry_run: bool = False):
    # rsync_options = f"--rsync-path='mkdir -p {project.remote_dir} && mkdir -p {project.remote_outdir} && mkdir -p {project.remote_mountdir} && rsync'"

    # A trick to create directories right before performing rsync
    rmxdirs = machine.get_rmxdirs(project.name)
    rsync_options = f"--rsync-path='mkdir -p {rmxdirs.codedir} && mkdir -p {rmxdirs.outdir} && mkdir -p {rmxdirs.mountdir} && rsync'"

    rsync(source_dir=project.rootdir, target_dir=machine.uri(rmxdirs.codedir),
          exclude=project.exclude, options=rsync_options, dry_run=dry_run, transfer_rootdir=False)

    # rsync the directories to mount
    for mount_dir in project.mount_dirs:
        rsync(source_dir=mount_dir, target_dir=machine.uri(rmxdirs.mountdir),
              exclude=project.exclude, dry_run=dry_run)


def _sync_output(project: Project, machine: Machine, dry_run: bool = False):
    # Rsync remote outdir with the local outdir.
    if project.outdir:
        rmxdirs = machine.get_rmxdirs(project.name)
        # Check if there's any output file (the first line is always 'total [num-files]')
        ssh_client = SimpleSSHClient(machine.remote_conf)

        result = ssh_client.run(f'ls -l {rmxdirs.outdir} | grep -v "^total" | wc -l', hide=True)
        num_output_files = int(result.stdout)
        logger.info(f'{num_output_files} files are in the output directory')
        if num_output_files > 0:
            rsync(source_dir=machine.uri(rmxdirs.outdir), target_dir=project.outdir,
                  dry_run=dry_run)
    else:
        logger.warn('project.outdir is set to None. Doing nothing here.')



def handler(project: Project, machine: Machine, parsed: Namespace):
    """Deploy the local repository and execute the command on a machine.

    1. get SSH connection to machine
    2. run rsync(project.local_dir, project.remote_dir)
    3. run machine.execute(parsed.remote_command)
    """
    logger.info(f'handling command for {__file__}')
    logger.info(f'parsed: {parsed}')

    _sync_code(project, machine, dry_run=parsed.dry_run)
    _sync_output(project, machine, dry_run=parsed.dry_run)


name = 'sync'
description = 'sync command'
parser = _get_parser()
