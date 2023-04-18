from __future__ import annotations
from copy import deepcopy
import os
from pathlib import Path
from argparse import ArgumentParser
from argparse import Namespace
from lmn import logger
from lmn.helpers import find_project_root
from lmn.config import SlurmConfig
from lmn.helpers import replace_lmn_envvars
from lmn.cli._config_loader import Project, Machine
from lmn.machine import SimpleSSHClient

from lmn.runner import SlurmRunner
from .sync import _sync_output, _sync_code


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
    parser.add_argument(
        "--image",
        default=None,
        help="specify a docker image"
    )
    parser.add_argument(
        "--name",
        default=None,
        help="specify docker container name"
    )
    parser.add_argument(
        "--sconf",
        default=None,
        help="specify a slurm configuration to be used"
    )
    parser.add_argument(
        "--dconf",
        default=None,
        help="specify a docker configuration to be used"
    )
    parser.add_argument(
        "-m",
        "--mode",
        action="store",
        type=str,
        default=None,
        choices=["ssh", "docker", "slurm", "singularity", "slurm-sing", "sing-slurm"],
        help="What mode to run",
    )
    parser.add_argument(
        "-d",
        "--disown",
        action="store_true",
        help="Do not block to wait for the process to exit. stdout/stderr will not be shown with this option.",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="When a job with the same name already exists, kill it and run the new one (only for Docker mode)",
    )
    parser.add_argument(
        "-X",
        "--x-forward",
        action="store_true",
        help="X11 forwarding",
    )
    parser.add_argument(
        "--no-sync",
        action="store_true",
        help="Do not perform rsync. This means your local files will not be synced with remote server.",
    )
    parser.add_argument(
        "--contain",
        action="store_true",
        help="With this flag, rsync will copy the project directory to a new unique location on remote, rather than the predetermined one.",
    )
    parser.add_argument(
        "-n",
        "--num-sequence",
        action="store",
        type=int,
        default=1,
        help="number of sequence in Slurm sequential jobs"
    )
    parser.add_argument(
        "--sweep",
        action="store",
        type=str,
        help="specify sweep range (e.g., --sweep 0-255) this changes the value of $LMN_RUN_SWEEP_IDX"
    )
    parser.add_argument(
        "remote_command",
        default=False,
        action="store",
        nargs="+",
        type=str,
        help="Command to execute in a remote machine.",
    )
    return parser

from ._config_loader import Machine


def parse_sweep_idx(sweep_str):
    # Parse input
    # format #0: 8 --> 8
    # format #1: 1-10 --> range(1, 10)
    # format #2: 1,2,7 --> [1, 2, 7]
    if '-' in sweep_str:
        # format #1
        begin, end = [int(val) for val in sweep_str.split('-')]
        assert begin < end
        sweep_ind = range(begin, end)
    elif ',' in sweep_str:
        sweep_ind = [int(e) for e in sweep_str.strip().split(',')]
    elif sweep_str.isnumeric():
        sweep_ind = [int(sweep_str)]
    else:
        raise KeyError("Format for --sweep option is not recognizable. Format examples: '1-10', '8', '1,2,7'.")

    return sweep_ind


def print_conf(mode: str, machine: Machine, image: str | None = None):
    output = f'Running with [{mode}] mode on [{machine.remote_conf.base_uri}]'
    if image is not None:
        output += f' with image: [{image}]'
    logger.info(output)

def handler(project: Project, machine: Machine, parsed: Namespace, preset: dict):
    """
    Args:
    - project (Project): stores project-specific configurations
    - machine (Machine): stores machine-specific configurations
    - preset (dict)    : stores preset configurations for slurm or docker images
    """
    logger.debug(f'handling command for {__file__}')
    logger.debug(f'parsed: {parsed}')

    # Runtime info
    curr_dir = Path(os.getcwd()).resolve()
    proj_rootdir = find_project_root()
    rel_workdir = curr_dir.relative_to(proj_rootdir)
    logger.debug(f'relative working dir: {rel_workdir}')  # cwd.relative_to(project_root)
    if isinstance(parsed.remote_command, list):
        cmd = ' '.join(parsed.remote_command)
    else:
        cmd = parsed.remote_command

    runtime_options = Namespace(dry_run=parsed.dry_run,
                                cmd=cmd,
                                rel_workdir=rel_workdir,
                                disown=parsed.disown,
                                name=parsed.name,
                                sweep=parsed.sweep,
                                num_sequence=parsed.num_sequence,
                                no_sync=parsed.no_sync,
                                sconf=parsed.sconf,
                                dconf=parsed.dconf,
                                force=parsed.force)

    # Sync code first
    if parsed.no_sync:
        logger.warning('--no-sync option is True, local files will not be synced.')

    if not parsed.no_sync:
        if parsed.contain:
            # Generate a unique path and set it to machine.lmndir
            # BUG: This generates the same hash every time!! This stack overflow answer is obviously wrong: https://stackoverflow.com/a/6048639/19913466
            # import hashlib
            # import time
            # hashlib.sha1().update(str(time.time()).encode("utf-8"))
            # _hash = hashlib.sha1().hexdigest()
            from lmn.helpers import get_timestamp
            _hash = get_timestamp()
            machine.lmndir = Path(f'{machine.lmndir}/{_hash}')
            runtime_options.name = _hash
            logger.warning(f'--contain flag is set.\n\tsetting the remote lmndir to {machine.lmndir}\n\tsetting jobs suffix to {_hash}')

        _sync_code(project, machine, runtime_options.dry_run)

    env = {**project.env, **machine.env}
    env_from_host = []
    lmndirs = machine.get_lmndirs(project.name)

    startup = ' && '.join([e for e in [project.startup, machine.startup] if e.strip()])

    # If parsed.mode is not set, try to read from the config file.
    mode = parsed.mode or machine.parsed_conf.get('mode')
    if mode is None:
        logger.warning('mode is not set. Setting it to SSH mode')
        mode = 'ssh'

    if mode == 'ssh':
        from lmn.runner import SSHRunner
        ssh_client = SimpleSSHClient(machine.remote_conf)
        ssh_runner = SSHRunner(ssh_client, lmndirs)
        print_conf(mode, machine)
        ssh_runner.exec(runtime_options.cmd,
                        runtime_options.rel_workdir,
                        startup=startup,
                        env=env,
                        dry_run=runtime_options.dry_run)

    elif mode == 'docker':
        from docker import DockerClient
        from lmn.runner import DockerRunner
        from lmn.config import DockerContainerConfig
        base_url = "ssh://" + machine.base_uri
        # client = DockerClient(base_url=base_url, use_ssh_client=True)
        client = DockerClient(base_url=base_url)  # dockerpty hangs with use_ssh_client=True

        # Specify job name
        name = f'{machine.user}-lmn-{project.name}'
        if runtime_options.name is not None:
            name = f'{name}--{runtime_options.name}'

        if runtime_options.dry_run:
            raise ValueError('dry run is not yet supported for Docker mode')

        from docker.types import Mount
        from lmn.cli._config_loader import DOCKER_ROOT_DIR, get_docker_lmndirs
        docker_lmndirs = get_docker_lmndirs(DOCKER_ROOT_DIR, project.name)

        if not runtime_options.no_sync:
            mounts = [Mount(target=docker_lmndirs.codedir, source=lmndirs.codedir, type='bind'),
                    Mount(target=docker_lmndirs.outdir, source=lmndirs.outdir, type='bind'),
                    Mount(target=docker_lmndirs.mountdir, source=lmndirs.mountdir, type='bind')]
        else:
            mounts = []
        mounts += [Mount(target=tgt, source=src, type='bind') for src, tgt in project.mount_from_host.items()]

        docker_runner = DockerRunner(client, docker_lmndirs)

        # Docker specific configurations
        docker_pconf = machine.parsed_conf.get('docker', {})
        image = parsed.image or docker_pconf.get('image')
        user_id = docker_pconf.get('user_id', 0)
        group_id = docker_pconf.get('group_id', 0)

        if 'mount_from_host' in docker_pconf:
            logger.warn('''
            `mount_from_host` configuration under `docker` in a config file will be ignored.\n
            Please place it under `project`.
            ''')


        if not isinstance(user_id, int):
            raise ValueError('user_id must be an integer')

        if not isinstance(group_id, int):
            raise ValueError('group_id must be an integer')


        if image is None:
            raise KeyError('docker image is not specified.')

        print_conf(mode, machine, image)
        if runtime_options.sweep:
            assert runtime_options.disown, "You must set -d option to use sweep functionality."
            sweep_ind = parse_sweep_idx(runtime_options.sweep)

            single_sweep = (len(sweep_ind) == 1)

            for sweep_idx in sweep_ind:
                _name = f'{name}-{sweep_idx}'
                logger.info(f'Launching sweep {sweep_idx}: {_name}')
                env.update({'LMN_RUN_SWEEP_IDX': sweep_idx})
                docker_conf = DockerContainerConfig(
                    image=image,
                    name=_name,
                    mounts=mounts,
                    startup=startup,
                    env=env,
                    user_id=user_id,
                    group_id=group_id
                )
                docker_runner.exec(runtime_options.cmd,
                                   runtime_options.rel_workdir,
                                   docker_conf,
                                   interactive=False,
                                   kill_existing_container=runtime_options.force,
                                   quiet=not single_sweep)
            # import time
            # logger.warning('Sleeping for 5 seconds to see if the container fails...')
            # logger.warning('You can safely exit anytime')
            # time.sleep(10)
        else:
            docker_conf = DockerContainerConfig(
                image=image,
                name=name,
                mounts=mounts,
                startup=startup,
                env=env,
                user_id=user_id,
                group_id=group_id,
            )
            docker_runner.exec(runtime_options.cmd,
                               runtime_options.rel_workdir,
                               docker_conf,
                               interactive=not runtime_options.disown,
                               kill_existing_container=runtime_options.force)


    elif mode in ['slurm', 'slurm-sing', 'sing-slurm']:
        # Slurm specific configurations
        from lmn.config import SlurmConfig
        import randomname
        import random
        if 'slurm' not in machine.parsed_conf:
            raise ValueError('Configuration must have an entry for "slurm" to use slurm mode.')

        # NOTE: slurm seems to be fine with duplicated name.
        proj_name_maxlen = 15
        rand_num = random.randint(0, 100)
        job_name = f'lmn-{project.name[:proj_name_maxlen]}-{randomname.get_name()}-{rand_num}'

        # Parse from slurm config options (aside from default)
        if parsed.sconf is not None:
            logger.debug('parsed.sconf is specified. Loading custom preset conf.')
            sconf = preset.get('slurm-configs', {}).get(parsed.sconf, {})
            if sconf is None:
                raise KeyError(f'configuration: {parsed.sconf} cannot be found in "slurm-configs".')

        else:
            sconf = machine.parsed_conf['slurm']
        slurm_conf = SlurmConfig(job_name, **sconf)

        # logger.info(f'slurm_conf: {machine.sconf}')
        ssh_client = SimpleSSHClient(machine.remote_conf)
        slurm_runner = SlurmRunner(ssh_client, lmndirs)
        run_opt = runtime_options

        # Specify job name
        name = f'{machine.user}-lmn-{project.name}'
        if runtime_options.name is not None:
            name = f'{name}--{run_opt.name}'
        slurm_conf.job_name = name

        if mode in ['slurm-sing', 'sing-slurm']:
            # Decorate run_opt.cmd for Singularity

            # sconf = SlurmConfig(job_name, **mconf['slurm'])
            if machine.parsed_conf is None or 'singularity' not in machine.parsed_conf:
                raise ValueError('Entry "singularity" not found in the config file.')

            image = machine.parsed_conf.get('singularity', {}).get('sif_file')
            overlay = machine.parsed_conf.get('singularity', {}).get('overlay')
            writable_tmpfs = machine.parsed_conf.get('singularity', {}).get('writable_tmpfs', False)

            # Overwrite lmn envvars.  Hmm I don't like this...
            from lmn.cli._config_loader import DOCKER_ROOT_DIR, get_docker_lmndirs
            sing_lmndirs = get_docker_lmndirs(DOCKER_ROOT_DIR, project.name)

            # TODO: Do we need this??
            env.update({
                'LMN_CODE_DIR': sing_lmndirs.codedir, 
                'LMN_MOUNT_DIR': sing_lmndirs.mountdir,
                'LMN_OUTPUT_DIR': sing_lmndirs.outdir
            })

            # Backward compat
            env.update({
                'RMX_CODE_DIR': sing_lmndirs.codedir,
                'RMX_MOUNT_DIR': sing_lmndirs.mountdir,
                'RMX_OUTPUT_DIR': sing_lmndirs.outdir
            })

            # NOTE: Without --containall, nvidia-smi command fails with "couldn't find libnvidia-ml.so library in your system."
            # NOTE: Without bash -c '{cmd}', if you put PYTHONPATH=/foo/bar, it fails with no such file or directory 'PYTHONPATH=/foo/bar'
            # TODO: Will the envvars be taken over to the internal shell (by this extra bash command)?
            # sing_cmd = "singularity run --nv --containall {options} {sif_file} bash -c '{cmd}'"
            sing_cmd = 'singularity run --nv --containall {options} {sif_file} bash -c -- "{cmd}"'
            options = []

            # Bind
            bind = '-B {source}:{target}'
            if not runtime_options.no_sync:
                options += [bind.format(target=sing_lmndirs.codedir, source=lmndirs.codedir),
                            bind.format(target=sing_lmndirs.outdir, source=lmndirs.outdir),
                            bind.format(target=sing_lmndirs.mountdir, source=lmndirs.mountdir)]
            options += [bind.format(target=tgt, source=src) for src, tgt in project.mount_from_host.items()]

            # Overlay
            if overlay:
                options += [f'--overlay {overlay}']

            if writable_tmpfs:
                # This often solves the following error:
                # OSError: [Errno 30] Read-only file system
                options += ['--writable-tmpfs']

            # Environment variables
            # TODO: Better to use --env-file option and read from a file
            # Escaping quotes and commas will be much easier in that way.
            options += ['--env ' + ','.join(f'{key}="{val}"' for key, val in env.items())]

            # NOTE: Since CUDA_VISIBLE_DEVICES is often comma-separated values (i.e., CUDA_VISIBLE_DEVICES=0,1),
            # and `singularity run --env FOO=BAR,HOGE=PIYO` considers comma to be a separator for envvars,
            # It fails without special handling.
            env_from_host = ['CUDA_VISIBLE_DEVICES']

            # Workdir
            _workdir = sing_lmndirs.codedir / runtime_options.rel_workdir
            options += [f'--pwd {_workdir}']

            # Overwrite command
            options = ' '.join(options)

            # Trying my best to escape quotations (https://stackoverflow.com/a/18886646/19913466)
            logger.debug(f'run_opt.cmd before escape: {run_opt.cmd}')
            escaped_cmd = run_opt.cmd.encode('unicode-escape').replace(b'"', b'\\"').decode('utf-8')
            logger.debug(f'run_opt.cmd after escape: {escaped_cmd}')

            run_opt.cmd = sing_cmd.format(options=options, sif_file=image, cmd=escaped_cmd)

        print_conf(mode, machine, image=image if mode in ['slurm-sing', 'sing-slurm'] else None)
        if run_opt.sweep:
            assert run_opt.disown, "You must set -d option to use sweep functionality."
            sweep_ind = parse_sweep_idx(run_opt.sweep)

            _slurm_conf = deepcopy(slurm_conf)
            for sweep_idx in sweep_ind:
                # NOTE: This special prefix "SINGULARITYENV_" is stripped and the rest is passed to singularity container,
                # even with --containall or --cleanenv !!
                # Example: (https://docs.sylabs.io/guides/3.1/user-guide/environment_and_metadata.html?highlight=environment%20variable)
                #     $ SINGULARITYENV_HELLO=world singularity exec centos7.img env | grep HELLO
                #     HELLO=world
                env.update({'SINGULARITYENV_LMN_RUN_SWEEP_IDX': sweep_idx})

                # Oftentimes, a user specifies $LMN_RUN_SWEEP_IDX as an argument to the command,
                # and that will be evaluated right before singularity launches
                env.update({'LMN_RUN_SWEEP_IDX': sweep_idx})

                _name = f'{slurm_conf.job_name}-{sweep_idx}'
                logger.info(f'Launching sweep {sweep_idx}: {_name}')
                _slurm_conf.job_name = _name
                slurm_runner.exec(run_opt.cmd, run_opt.rel_workdir, slurm_conf=_slurm_conf,
                                  startup=startup,
                                  interactive=False, num_sequence=run_opt.num_sequence,
                                  env=env, env_from_host=env_from_host, dry_run=run_opt.dry_run)
        else:
            slurm_runner.exec(run_opt.cmd, run_opt.rel_workdir, slurm_conf=slurm_conf,
                              startup=startup, interactive=not run_opt.disown, num_sequence=run_opt.num_sequence,
                              env=env, env_from_host=env_from_host, dry_run=run_opt.dry_run)
    else:
        raise ValueError(f'Unrecognized mode: {mode}')

    # Sync output files
    if not runtime_options.no_sync:
        _sync_output(project, machine, dry_run=parsed.dry_run)

name = 'run'
description = 'run command'
parser = _get_parser()
