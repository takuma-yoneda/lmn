from __future__ import annotations
import time
from copy import deepcopy
import os
from pathlib import Path
from argparse import ArgumentParser
from argparse import Namespace
from lmn import logger
from lmn.helpers import find_project_root, replace_lmn_envvars
from lmn.machine import CLISSHClient
from lmn.runner import SlurmRunner, PBSRunner
from lmn.cli.sync import _sync_output, _sync_code


from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from lmn.cli._config_loader import Project, Machine


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
        "--pbsconf",
        default=None,
        help="specify a PBS configuration to be used"
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
    # parser.add_argument(
    #     "-X",
    #     "--x-forward",
    #     action="store_true",
    #     help="X11 forwarding",
    # )
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

    # TODO: Let's use Pydantic
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

    # TODO:
    # - If configured, run a pre-flight ssh with ControlMaster to establish & retain the connection
    # - The following ssh / rsync should rely on this connection
    from lmn.helpers import establish_persistent_ssh
    establish_persistent_ssh(machine.remote_conf)

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
        # ssh_client = SimpleSSHClient(machine.remote_conf)
        ssh_client = CLISSHClient(machine.remote_conf)
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
        from lmn.container.docker import DockerContainerConfig

        if 'docker' not in machine.parsed_conf:
            raise ValueError('Configuration must have an entry for "docker" to use docker mode.')

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

        # Load Docker-specific configurations
        docker_pconf = DockerContainerConfig(**{
            'name': name,
            **machine.parsed_conf['docker']
        })

        if startup:
            logger.warn('`startup` configurations outside of `docker` will be ignored in docker mode.')
            logger.warn('Please place `startup` under `docker` if you want to run it in the container.')

        if not runtime_options.no_sync:
            docker_pconf.mount_from_host.update({
                f'{lmndirs.codedir}': docker_lmndirs.codedir,
                f'{lmndirs.outdir}': docker_lmndirs.outdir,
                f'{lmndirs.mountdir}': docker_lmndirs.mountdir,
                f'{lmndirs.scriptdir}': docker_lmndirs.scriptdir,
            })

        if project.mount_from_host:
            # Backward compatible
            logger.warn("`mount_from_host` under `project` is deprecated. Please move it under `docker`.")
            docker_pconf.mount_from_host.update(project.mount_from_host)

        docker_pconf.env.update(env)

        docker_runner = DockerRunner(client, docker_lmndirs)

        print_conf(mode, machine, docker_pconf.image)
        if runtime_options.sweep:
            if not runtime_options.disown:
                logger.error("You must set -d option to use sweep functionality.")
                import sys; sys.exit(1)
            sweep_ind = parse_sweep_idx(runtime_options.sweep)

            single_sweep = (len(sweep_ind) == 1)

            for sweep_idx in sweep_ind:
                _name = f'{name}-{sweep_idx}'
                logger.info(f'Launching sweep {sweep_idx}: {_name}')
                env.update({'LMN_RUN_SWEEP_IDX': sweep_idx})

                dconf = deepcopy(docker_pconf)
                dconf.name = _name
                dconf.env.update(env)

                docker_runner.exec(runtime_options.cmd,
                                   runtime_options.rel_workdir,
                                   dconf,
                                   interactive=False,
                                   kill_existing_container=runtime_options.force,
                                   quiet=not single_sweep)
        else:
            docker_runner.exec(runtime_options.cmd,
                               runtime_options.rel_workdir,
                               docker_pconf,
                               # startup=startup,
                               interactive=not runtime_options.disown,
                               kill_existing_container=runtime_options.force)


    elif mode in ['slurm', 'slurm-sing', 'sing-slurm']:
        # Slurm specific configurations
        from lmn.scheduler.slurm import SlurmConfig
        import randomname
        import random
        if 'slurm' not in machine.parsed_conf:
            raise ValueError('Configuration must have an entry for "slurm" to use slurm mode.')

        # NOTE: slurm seems to be fine with duplicated name.
        proj_name_maxlen = 15
        rand_num = random.randint(0, 100)
        job_name = f'lmn-{project.name[:proj_name_maxlen]}-{randomname.get_name()}-{rand_num}'

        # Create SlurmConfig object
        if parsed.sconf is None:
            sconf = machine.parsed_conf['slurm']
        else:
            # Try to load from the slurm conf presets
            logger.debug('parsed.sconf is specified. Loading custom preset conf.')
            sconf = preset.get('slurm-configs', {}).get(parsed.sconf, {})
            if not sconf:
                raise KeyError(f'configuration: {parsed.sconf} cannot be found or is empty in "slurm-configs".')
        slurm_conf = SlurmConfig(**{'job_name': job_name, **sconf})
        logger.debug(f'Using slurm preset: {parsed.sconf}')

        if runtime_options.force:
            logger.warn("`-f / --force` option has no effect in slurm mode")

        ssh_client = CLISSHClient(machine.remote_conf)
        slurm_runner = SlurmRunner(ssh_client, lmndirs)
        run_opt = runtime_options

        # Specify job name
        name = f'{machine.user}-lmn-{project.name}'
        if runtime_options.name is not None:
            name = f'{name}--{run_opt.name}'
        slurm_conf.job_name = name

        if mode in ['slurm-sing', 'sing-slurm']:
            from lmn.container.singularity import SingularityConfig, SingularityCommand

            # Error checking
            if machine.parsed_conf is None or 'singularity' not in machine.parsed_conf:
                raise ValueError('Entry "singularity" not found in the config file.')

            # Overwrite lmn envvars.  Hmm I don't like this...
            from lmn.cli._config_loader import DOCKER_ROOT_DIR, get_docker_lmndirs
            sing_lmndirs = get_docker_lmndirs(DOCKER_ROOT_DIR, project.name)

            # Load singularity config
            sing_conf = SingularityConfig(**{
                "pwd": str(sing_lmndirs.codedir / runtime_options.rel_workdir),
                **machine.parsed_conf['singularity']
                })
            # TODO: Use get_lmndirs function!
            # TODO: Do we need this??
            env.update({
                'LMN_PROJECT_NAME': project.name,
                'LMN_CODE_DIR': sing_lmndirs.codedir,
                'LMN_MOUNT_DIR': sing_lmndirs.mountdir,
                'LMN_OUTPUT_DIR': sing_lmndirs.outdir,
                'LMN_SCRIPT_DIR': sing_lmndirs.scriptdir,
            })

            # Backward compat
            env.update({
                'RMX_PROJECT_NAME': project.name,
                'RMX_CODE_DIR': sing_lmndirs.codedir,
                'RMX_MOUNT_DIR': sing_lmndirs.mountdir,
                'RMX_OUTPUT_DIR': sing_lmndirs.outdir,
                'RMX_SCRIPT_DIR': sing_lmndirs.scriptdir,
            })


            # Bind
            mount_from_host = {}
            if not runtime_options.no_sync:
                mount_from_host.update({
                    f'{lmndirs.codedir}': sing_lmndirs.codedir,
                    f'{lmndirs.outdir}': sing_lmndirs.outdir,
                    f'{lmndirs.mountdir}': sing_lmndirs.mountdir,
                    f'{lmndirs.scriptdir}': sing_lmndirs.scriptdir,
                })
            if project.mount_from_host:
                # Backward compatible
                logger.warn("`mount_from_host` under `project` is deprecated. Please move it under `singularity`.")
                mount_from_host.update(project.mount_from_host)

            # NOTE: Since CUDA_VISIBLE_DEVICES is often comma-separated values (i.e., CUDA_VISIBLE_DEVICES=0,1),
            # and `singularity run --env FOO=BAR,HOGE=PIYO` considers comma to be a separator for envvars,
            # It fails without special handling.
            sing_conf.env_from_host += ['CUDA_VISIBLE_DEVICES', 'SLURM_JOBID', 'SLURM_JOB_ID', 'SLURM_TASK_PID']

            # Integrate current `env`, `mount_from_host`
            sing_conf.env.update(env)
            sing_conf.mount_from_host.update(mount_from_host)

            # Finally overwrite run_opt.cmd
            run_opt.cmd = SingularityCommand.run(run_opt.cmd, sing_conf)

        timestamp = f"{time.time():.5f}".split('.')[-1]  # 5 subdigits of the timestamp
        print_conf(mode, machine, image=sing_conf.sif_file if mode in ['slurm-sing', 'sing-slurm'] else None)
        if run_opt.sweep:
            if not run_opt.disown:
                logger.error("You must set -d option to use sweep functionality.")
                import sys; sys.exit(1)

            if not parsed.contain:
                logger.error("You should set --contain option to use sweep functionality.")
                import sys; sys.exit(1)

            sweep_ind = parse_sweep_idx(run_opt.sweep)

            _slurm_conf = deepcopy(slurm_conf)
            for sweep_idx in sweep_ind:
                # NOTE: This special prefix "SINGULARITYENV_" is stripped and the rest is passed to singularity container,
                # even with --containall or --cleanenv !!
                # Example: (https://docs.sylabs.io/guides/3.1/user-guide/environment_and_metadata.html?highlight=environment%20variable)
                #     $ SINGULARITYENV_HELLO=world singularity exec centos7.img env | grep HELLO
                #     HELLO=world
                env.update({
                    'SINGULARITYENV_LMN_RUN_SWEEP_IDX': sweep_idx,
                    'APPTAINERENV_LMN_RUN_SWEEP_IDX': sweep_idx,
                })

                # Oftentimes, a user specifies $LMN_RUN_SWEEP_IDX as an argument to the command,
                # and that will be evaluated right before singularity launches
                env.update({'LMN_RUN_SWEEP_IDX': sweep_idx, 'RMX_RUN_SWEEP_IDX': sweep_idx})

                _name = f'{slurm_conf.job_name}-{timestamp}-{sweep_idx}'
                logger.info(f'Launching sweep {sweep_idx}: {_name}')
                _slurm_conf.job_name = _name
                slurm_runner.exec(run_opt.cmd, run_opt.rel_workdir, slurm_conf=_slurm_conf,
                                  startup=startup,
                                  timestamp=timestamp,
                                  interactive=False, num_sequence=run_opt.num_sequence,
                                  env=env, env_from_host=env_from_host, dry_run=run_opt.dry_run)
        else:
            slurm_runner.exec(run_opt.cmd, run_opt.rel_workdir, slurm_conf=slurm_conf,
                              startup=startup, timestamp=timestamp, interactive=not run_opt.disown, num_sequence=run_opt.num_sequence,
                              env=env, env_from_host=env_from_host, dry_run=run_opt.dry_run)


    elif mode in ['pbs', 'pbs-sing', 'sing-pbs']:
        # PBS-specific configurations
        from lmn.scheduler.pbs import PBSConfig
        import randomname
        import random
        if 'pbs' not in machine.parsed_conf:
            raise ValueError('Configuration must have an entry for "slurm" to use slurm mode.')

        # NOTE: slurm seems to be fine with duplicated name.
        proj_name_maxlen = 15
        rand_num = random.randint(0, 100)
        job_name = f'lmn-{project.name[:proj_name_maxlen]}-{randomname.get_name()}-{rand_num}'

        # Parse from slurm config options (aside from default)
        if parsed.pbsconf is not None:
            logger.debug('parsed.sconf is specified. Loading custom preset conf.')
            pbsconf = preset.get('pbs-configs', {}).get(parsed.pbsconf, {})
            if pbsconf is None:
                raise KeyError(f'configuration: {parsed.pbsconf} cannot be found in "slurm-configs".')

        else:
            pbsconf = machine.parsed_conf['pbs']
        scheduler_conf = PBSConfig(job_name, **pbsconf)

        if runtime_options.force:
            logger.warn("`-f / --force` option has no effect in slurm mode")

        # logger.info(f'slurm_conf: {machine.sconf}')
        ssh_client = CLISSHClient(machine.remote_conf)
        runner = PBSRunner(ssh_client, lmndirs)
        run_opt = runtime_options

        # Specify job name
        name = f'{machine.user}-lmn-{project.name}'
        if runtime_options.name is not None:
            name = f'{name}--{run_opt.name}'
        scheduler_conf.job_name = name

        if mode in ['pbs-sing', 'sing-pbs']:
            # Decorate run_opt.cmd for Singularity

            # sconf = SlurmConfig(job_name, **mconf['slurm'])
            if machine.parsed_conf is None or 'singularity' not in machine.parsed_conf:
                raise ValueError('Entry "singularity" not found in the config file.')

            image = machine.parsed_conf.get('singularity', {}).get('sif_file')

            # Overwrite lmn envvars.  Hmm I don't like this...
            from lmn.cli._config_loader import DOCKER_ROOT_DIR, get_docker_lmndirs
            sing_lmndirs = get_docker_lmndirs(DOCKER_ROOT_DIR, project.name)

            # TODO: Use get_lmndirs function!
            # TODO: Do we need this??
            env.update({
                'LMN_PROJECT_NAME': project.name,
                'LMN_CODE_DIR': sing_lmndirs.codedir,
                'LMN_MOUNT_DIR': sing_lmndirs.mountdir,
                'LMN_OUTPUT_DIR': sing_lmndirs.outdir,
                'LMN_SCRIPT_DIR': sing_lmndirs.scriptdir,
            })

            # Backward compat
            env.update({
                'RMX_PROJECT_NAME': project.name,
                'RMX_CODE_DIR': sing_lmndirs.codedir,
                'RMX_MOUNT_DIR': sing_lmndirs.mountdir,
                'RMX_OUTPUT_DIR': sing_lmndirs.outdir,
                'RMX_SCRIPT_DIR': sing_lmndirs.scriptdir,
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
                            bind.format(target=sing_lmndirs.mountdir, source=lmndirs.mountdir),
                            bind.format(target=sing_lmndirs.scriptdir, source=lmndirs.scriptdir)]
            options += [bind.format(target=tgt, source=src) for src, tgt in project.mount_from_host.items()]


            # NOTE: Since CUDA_VISIBLE_DEVICES is often comma-separated values (i.e., CUDA_VISIBLE_DEVICES=0,1),
            # and `singularity run --env FOO=BAR,HOGE=PIYO` considers comma to be a separator for envvars,
            # It fails without special handling.
            env_from_host = ['CUDA_VISIBLE_DEVICES', 'JOBID', 'JOB_ID_STEP_ID', 'HOSTNAME', 'NODE_IDENTIFIER', 'TASK_IDENTIFIER']

            # run_opt.cmd = sing_cmd.format(options=options, sif_file=image, cmd=escaped_cmd)
            run_opt.cmd = SingularityCommand.run(cmd, sing_conf)

        timestamp = f"{time.time():.5f}".split('.')[-1]  # 5 subdigits of the timestamp
        print_conf(mode, machine, image=image if mode in ['pbs-sing', 'sing-pbs'] else None)
        if run_opt.sweep:
            if not run_opt.disown:
                logger.error("You must set -d option to use sweep functionality.")
                import sys; sys.exit(1)

            if not parsed.contain:
                logger.error("You should set --contain option to use sweep functionality.")
                import sys; sys.exit(1)

            sweep_ind = parse_sweep_idx(run_opt.sweep)

            _scheduler_conf = deepcopy(scheduler_conf)
            for sweep_idx in sweep_ind:
                # NOTE: This special prefix "SINGULARITYENV_" is stripped and the rest is passed to singularity container,
                # even with --containall or --cleanenv !!
                # Example: (https://docs.sylabs.io/guides/3.1/user-guide/environment_and_metadata.html?highlight=environment%20variable)
                #     $ SINGULARITYENV_HELLO=world singularity exec centos7.img env | grep HELLO
                #     HELLO=world
                env.update({
                    'SINGULARITYENV_LMN_RUN_SWEEP_IDX': sweep_idx,
                    'APPTAINERENV_LMN_RUN_SWEEP_IDX': sweep_idx,
                })

                # Oftentimes, a user specifies $LMN_RUN_SWEEP_IDX as an argument to the command,
                # and that will be evaluated right before singularity launches
                env.update({'LMN_RUN_SWEEP_IDX': sweep_idx, 'RMX_RUN_SWEEP_IDX': sweep_idx})

                _name = f'{_scheduler_conf.job_name}-{timestamp}-{sweep_idx}'
                logger.info(f'Launching sweep {sweep_idx}: {_name}')
                scheduler_conf.job_name = _name

                runner.exec(run_opt.cmd, run_opt.rel_workdir, _scheduler_conf,
                            startup=startup,
                            timestamp=timestamp,
                            interactive=False,
                            num_sequence=run_opt.num_sequence,
                            env=env,
                            env_from_host=env_from_host,
                            dry_run=run_opt.dry_run)
        else:
            runner.exec(run_opt.cmd, run_opt.rel_workdir, scheduler_conf,
                        startup=startup,
                        timestamp=timestamp,
                        interactive=not run_opt.disown,
                        num_sequence=run_opt.num_sequence,
                        env=env,
                        env_from_host=env_from_host,
                        dry_run=run_opt.dry_run)

    else:
        raise ValueError(f'Unrecognized mode: {mode}')

    # Sync output files
    if not runtime_options.no_sync and not runtime_options.disown:
        _sync_output(project, machine, dry_run=parsed.dry_run)

name = 'run'
description = 'run command'
parser = _get_parser()
