from copy import deepcopy
import os
import pathlib
from argparse import ArgumentParser
from argparse import Namespace
from rmx import logger
from rmx.helpers import find_project_root
from rmx.config import SlurmConfig
from rmx.helpers import replace_rmx_envvars
from rmx.cli._config_loader import Project, Machine
from rmx.machine import SimpleSSHClient

from rmx.runner import SlurmRunner
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
        help="specify docker image"
    )
    parser.add_argument(
        "--name",
        default=None,
        help="specify docker container name"
    )
    parser.add_argument(
        "-m",
        "--mode",
        action="store",
        type=str,
        default=None,
        choices=["ssh", "docker", "slurm", "singularity", "slurm-sing"],
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
        help="specify sweep range (e.g., --sweep 0-255) this changes the value of $RMX_RUN_SWEEP_IDX"
    )
    parser.add_argument(
        "--num_sequence",
        action="store",
        type=int,
        help="(For slurm) number of repetitions for a sequential job."
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


def handler(project: Project, machine: Machine, parsed: Namespace):
    logger.info(f'handling command for {__file__}')
    logger.info(f'parsed: {parsed}')

    # Runtime info
    curr_dir = pathlib.Path(os.getcwd()).resolve()
    proj_rootdir = find_project_root()
    rel_workdir = curr_dir.relative_to(proj_rootdir)
    logger.info(f'relative working dir: {rel_workdir}')  # cwd.relative_to(project_root)
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
                                force=parsed.force)

    # Sync code first
    if parsed.no_sync:
        logger.warn('--no-sync option is True, local files will not be synced.')

    if not parsed.no_sync:
        _sync_code(project, machine, runtime_options.dry_run)

    env = {**project.env, **machine.env}
    rmxdirs = machine.get_rmxdirs(project.name)

    startup = '&&'.join([e for e in [project.startup, machine.startup] if e.strip()])

    # If parsed.mode is not set, try to read from the config file.
    mode = parsed.mode or machine.parsed_conf.get('mode')
    if mode is None:
        logger.warn('mode is not set. Setting it to SSH mode')
        mode = 'ssh'

    if mode == 'ssh':
        from rmx.runner import SSHRunner
        ssh_client = SimpleSSHClient(machine.remote_conf)
        ssh_runner = SSHRunner(ssh_client, rmxdirs)
        ssh_runner.exec(runtime_options.cmd, 
                        runtime_options.rel_workdir,
                        startup=startup,
                        env=env,
                        dry_run=runtime_options.dry_run)

    elif mode == 'docker':
        from docker import DockerClient
        from rmx.runner import DockerRunner
        from rmx.config import DockerContainerConfig
        base_url = "ssh://" + machine.base_uri
        # client = DockerClient(base_url=base_url, use_ssh_client=True)
        client = DockerClient(base_url=base_url)  # dockerpty hangs with use_ssh_client=True

        # Specify job name
        name = f'{machine.user}-rmx-{project.name}'
        if runtime_options.name is not None:
            name = f'{name}--{runtime_options.name}'

        if runtime_options.dry_run:
            raise ValueError('dry run is not yet supported for Docker mode')

        from docker.types import Mount
        from rmx.cli._config_loader import DOCKER_ROOT_DIR, get_docker_rmxdirs
        docker_rmxdirs = get_docker_rmxdirs(DOCKER_ROOT_DIR, project.name)

        if not runtime_options.no_sync:
            mounts = [Mount(target=docker_rmxdirs.codedir, source=rmxdirs.codedir, type='bind'),
                    Mount(target=docker_rmxdirs.outdir, source=rmxdirs.outdir, type='bind'),
                    Mount(target=docker_rmxdirs.mountdir, source=rmxdirs.mountdir, type='bind')]
        else:
            mounts = []
        mounts += [Mount(target=tgt, source=src, type='bind') for src, tgt in project.mount_from_host.items()]

        docker_runner = DockerRunner(client, docker_rmxdirs)

        # Docker specific configurations
        image = parsed.image or machine.parsed_conf.get('docker', {}).get('image')
        if image is None:
            raise KeyError('docker image is not specified.')

        if runtime_options.sweep:
            assert runtime_options.disown, "You must set -d option to use sweep functionality."
            sweep_ind = parse_sweep_idx(runtime_options.sweep)

            for sweep_idx in sweep_ind:
                env.update({'RMX_RUN_SWEEP_IDX': sweep_idx})
                docker_conf = DockerContainerConfig(
                    image=image,
                    name=f'{name}-{sweep_idx}',
                    mounts=mounts,
                    env=env
                )
                docker_runner.exec(runtime_options.cmd,
                                   runtime_options.rel_workdir,
                                   docker_conf,
                                   interactive=False,
                                   kill_existing_container=runtime_options.force,
                                   quiet=True)
        else:
            docker_conf = DockerContainerConfig(
                image=image,
                name=name,
                mounts=mounts,
                env=env
            )
            docker_runner.exec(runtime_options.cmd,
                               runtime_options.rel_workdir,
                               docker_conf,
                               interactive=not runtime_options.disown,
                               kill_existing_container=runtime_options.force)


    elif mode == "slurm" or mode == 'slurm-sing':
        # Slurm specific configurations
        from rmx.config import SlurmConfig
        import randomname
        import random
        if 'slurm' not in machine.parsed_conf:
            raise ValueError('Configuration must have an entry for "slurm" to use slurm mode.')

        proj_name_maxlen = 15
        rand_num = random.randint(0, 100)
        job_name = f'rmx-{project.name[:proj_name_maxlen]}-{randomname.get_name()}-{rand_num}'
        slurm_conf = SlurmConfig(job_name, **machine.parsed_conf['slurm'])

        # logger.info(f'slurm_conf: {machine.sconf}')
        ssh_client = SimpleSSHClient(machine.remote_conf)
        slurm_runner = SlurmRunner(ssh_client, rmxdirs)
        run_opt = runtime_options

        # Specify job name
        name = f'{machine.user}-rmx-{project.name}'
        if runtime_options.name is not None:
            name = f'{name}--{run_opt.name}'
        slurm_conf.job_name = name

        if mode == 'slurm-sing':
            # Decorate run_opt.cmd for Singularity

            # sconf = SlurmConfig(job_name, **mconf['slurm'])
            image = machine.parsed_conf.get('singularity', {}).get('sif_file')
            overlay = machine.parsed_conf.get('singularity', {}).get('overlay')

            # Overwrite rmx envvars.  Hmm I don't like this...
            from rmx.cli._config_loader import DOCKER_ROOT_DIR, get_docker_rmxdirs
            sing_rmxdirs = get_docker_rmxdirs(DOCKER_ROOT_DIR, project.name)

            # TODO: Do we need this??
            env.update({
                'RMX_CODE_DIR': sing_rmxdirs.codedir, 
                'RMX_MOUNT_DIR': sing_rmxdirs.mountdir,
                'RMX_OUTPUT_DIR': sing_rmxdirs.outdir
            })

            # NOTE: Without --containall, nvidia-smi command fails with "couldn't find libnvidia-ml.so library in your system."
            # NOTE: Without bash -c '{cmd}', if you put PYTHONPATH=/foo/bar, it fails with no such file or directory 'PYTHONPATH=/foo/bar'
            # TODO: Will the envvars be taken over to the internal shell (by this extra bash command)?
            sing_cmd = "singularity run --nv --containall {options} {sif_file} bash -c '{cmd}'"
            options = []

            # Bind
            bind = '-B {source}:{target}'
            if not runtime_options.no_sync:
                options += [bind.format(target=sing_rmxdirs.codedir, source=rmxdirs.codedir),
                        bind.format(target=sing_rmxdirs.outdir, source=rmxdirs.outdir),
                        bind.format(target=sing_rmxdirs.mountdir, source=rmxdirs.mountdir)]
            options += [bind.format(target=tgt, source=src) for src, tgt in project.mount_from_host.items()]

            # Overlay
            if overlay:
                options += [f'--overlay {overlay}']

            # TEMP: Only for tticslurm
            assert 'CUDA_VISIBLE_DEVICES' not in env, 'CUDA_VISIBLE_DEVICES will be automatically set. You should not specify it manually.'
            env['CUDA_VISIBLE_DEVICES'] = '$CUDA_VISIBLE_DEVICES'  # This let Singularity container use the envvar on the host

            # TODO: This CANNOT set RMX_RUN_SWEEP_IDX !!
            # Environment variables
            options += ['--env ' + ','.join(f'{key}="{val}"' for key, val in env.items())]

            # Workdir
            _workdir = sing_rmxdirs.codedir / runtime_options.rel_workdir
            options += [f'--pwd {_workdir}']

            # Overwrite command
            options = ' '.join(options)
            run_opt.cmd = sing_cmd.format(options=options, sif_file=machine.sing.image, cmd=run_opt.cmd)

        if run_opt.sweep:
            assert run_opt.disown, "You must set -d option to use sweep functionality."
            sweep_ind = parse_sweep_idx(run_opt.sweep)

            _slurm_conf = deepcopy(slurm_conf)
            for sweep_idx in sweep_ind:
                env.update({'RMX_RUN_SWEEP_IDX': sweep_idx})
                _slurm_conf.job_name = f'{slurm_conf.job_name}-{sweep_idx}'
                slurm_runner.exec(run_opt.cmd, run_opt.rel_workdir, slurm_conf=_slurm_conf,
                                  startup=startup,
                                  interactive=False, num_sequence=run_opt.num_sequence,
                                  env=env, dry_run=run_opt.dry_run)
        else:
            slurm_runner.exec(run_opt.cmd, run_opt.rel_workdir, slurm_conf=slurm_conf,
                              startup=startup, interactive=not run_opt.disown, num_sequence=run_opt.num_sequence,
                              env=env, dry_run=run_opt.dry_run)
    else:
        raise ValueError(f'Unrecognized mode: {mode}')

    # Sync output files
    if not runtime_options.no_sync:
        _sync_output(project, machine, dry_run=parsed.dry_run)

name = 'run'
description = 'run command'
parser = _get_parser()
