from argparse import ArgumentParser, Namespace

from lmn import logger
from lmn.cli._config_loader import Machine, Project
from lmn.cli._utils import rsync
from lmn.machine import CLISSHClient

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
    lmndirs = machine.get_lmndirs(project.name)
    rsync_options = [f"--rsync-path='mkdir -p {lmndirs.codedir} && mkdir -p {lmndirs.outdir} && mkdir -p {lmndirs.mountdir} && mkdir -p {lmndirs.scriptdir} && rsync'"]

    try:
        rsync(source_dir=project.rootdir, target_dir=lmndirs.codedir, remote_conf=machine.remote_conf,
              exclude=project.exclude, options=rsync_options, dry_run=dry_run, transfer_rootdir=False)

        # rsync the directories to mount
        # for mount_dir in project.mount_dirs:
        #     rsync(source_dir=mount_dir, target_dir=lmndirs.mountdir, remote_conf=machine.remote_conf,
        #           exclude=project.exclude, dry_run=dry_run)
    except OSError:
        import sys
        import traceback
        print(traceback.format_exc(0), file=sys.stderr)
        sys.exit(1)


def _sync_output(project: Project, machine: Machine, dry_run: bool = False):
    # Rsync remote outdir with the local outdir.
    if project.outdir:
        try:
            lmndirs = machine.get_lmndirs(project.name)
            # Check if there's any output file (the first line is always 'total [num-files]')
            ssh_client = CLISSHClient(machine.remote_conf)

            result = ssh_client.run(f'ls -l {lmndirs.outdir} | grep -v "^total" | wc -l', capture_output=True)
            num_output_files = int(result)

            logger.info(f'{num_output_files} files are in the output directory')
            if num_output_files > 0:
                rsync(source_dir=lmndirs.outdir, target_dir=project.outdir, 
                      remote_conf=machine.remote_conf,
                      dry_run=dry_run, to_local=True)
                logger.info(f'The output files are copied to {str(project.outdir)}')

        except OSError:
            # NOTE: Only show the last stack of traceback (If I remember corectly...)
            import sys
            import traceback
            print(traceback.format_exc(0), file=sys.stderr)
            sys.exit(1)
    else:
        logger.warning('project.outdir is set to None. Doing nothing here.')


def handler(project: Project, machine: Machine, parsed: Namespace, preset: dict):
    """Deploy the local repository and execute the command on a machine.

    1. get SSH connection to machine
    2. run rsync(project.local_dir, project.remote_dir)
    3. run machine.execute(parsed.remote_command)
    """
    logger.debug(f'handling command for {__file__}')
    logger.debug(f'parsed: {parsed}')

    _sync_code(project, machine, dry_run=parsed.dry_run)
    _sync_output(project, machine, dry_run=parsed.dry_run)


name = 'sync'
description = 'sync command'
parser = _get_parser()
