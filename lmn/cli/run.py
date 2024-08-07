from __future__ import annotations
import time
from copy import deepcopy
import os
from pathlib import Path
from argparse import ArgumentParser
from argparse import Namespace
from lmn import logger
from lmn.helpers import find_project_root, parse_sweep_idx
from lmn.machine import CLISSHClient
from lmn.runner import SlurmRunner, PBSRunner
from lmn.cli.sync import _sync_output, _sync_code
from lmn.const import available_modes


from typing import TYPE_CHECKING, Literal, Optional
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
        choices=available_modes,
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


def print_conf(mode: str, machine: Machine, image: Optional[str] = None):
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
    logger.debug(f'relative working dir: {rel_workdir}')
    if isinstance(parsed.remote_command, list):
        cmd = ' '.join(parsed.remote_command)
    else:
        cmd = parsed.remote_command

    # TODO: Let's use Pydantic
    # NOTE: How is it different from `parsed`?
    # - `runtime_options` serves a similar role as `parsed`, but it can be **modified** by lmn itself
    # - anything that won't be modified should be in `parsed`
    runtime_options = Namespace(cmd=cmd,
                                rel_workdir=rel_workdir,
                                disown=parsed.disown,
                                name=parsed.name,
                                num_sequence=parsed.num_sequence,
                                no_sync=parsed.no_sync,
                                force=parsed.force)

    # Before running anything significant, validate --sweep format if specified
    if parsed.sweep:
        # This will raise an error if the format is invalid
        parse_sweep_idx(parsed.sweep)

    # - Run a pre-flight ssh with ControlMaster to establish & retain the connection
    # - The future ssh / rsync will reuse this connection
    from lmn.helpers import establish_persistent_ssh
    establish_persistent_ssh(machine.remote_conf)

    # Sync code first
    if parsed.no_sync:
        logger.warning('--no-sync option is True, local files will not be synced.')

    if not parsed.no_sync:
        if parsed.contain:
            # Generate a unique path and set it to machine.lmndir
            from lmn.helpers import get_timestamp
            _hash = get_timestamp()

            # HACK: Dirty but just overwrite machine.lmndirs with new paths
            # Add the hash to lmndirs
            rootdir = Path(machine.lmndirs.rootdir)
            new_lmn_root = rootdir.parent / (rootdir.name + f'--{_hash}')
            machine.lmndirs.codedir = new_lmn_root / 'code'
            machine.lmndirs.mountdir = new_lmn_root / 'mount'
            machine.lmndirs.outdir = new_lmn_root / 'output'
            machine.lmndirs.scriptdir = new_lmn_root / 'script'
            # machine.lmndir = Path(f'{machine.lmndir}/{_hash}')

            runtime_options.name = _hash
            logger.info(f'--contain flag is set.\n\tsetting the remote lmndir to {new_lmn_root}\n\tsetting jobs suffix to {_hash}')

        _sync_code(project, machine, parsed.dry_run)


    # If parsed.mode is not set, try to read from the config file.
    mode = parsed.mode or machine.parsed_conf.mode
    if mode is None:
        logger.warning('mode is not set. Setting it to SSH mode')
        mode = 'ssh'

    if mode == 'ssh':
        from lmn.runner import SSHRunner

        startup = ' ; '.join([e for e in [project.startup, machine.startup] if e.strip()])
        lmndirs = machine.lmndirs
        env = {**project.env, **machine.env}

        ssh_client = CLISSHClient(machine.remote_conf)
        ssh_runner = SSHRunner(ssh_client, lmndirs)
        print_conf(mode, machine)
        ssh_runner.exec(runtime_options.cmd,
                        runtime_options.rel_workdir,
                        startup=startup,
                        env=env,
                        dry_run=parsed.dry_run)

    elif mode == 'docker':
        from docker import DockerClient
        from lmn.runner import DockerRunner
        from lmn.container.docker import DockerContainerConfig

        # if 'docker' not in machine.parsed_conf:
        if machine.parsed_conf.docker is None:
            raise ValueError('Configuration must have an entry for "docker" to use docker mode.')

        startup = ' ; '.join([e for e in [project.startup, machine.startup] if e.strip()])
        lmndirs = machine.lmndirs
        env = {**project.env, **machine.env}

        base_url = "ssh://" + machine.base_uri
        # NOTE: dockerpty hangs with use_ssh_client=True
        # But I have switched the docker + interactive to python-on-whales, so maybe it's fine for now
        # If `use_ssh_client=False`, it uses paramiko internally.
        # dockerpy uses paramiko to parse `~/.ssh/config` and look up corresponding configuration
        # An annoying issue is that, when I have this entry in the config:
        # ``` ~/.ssh/config
        # Host elm
        #     HostName elm.ttic.edu
        #     User takuma
        #     IdentityFile ~/.ssh/ttic
        # ```
        # dockerpy's implementation does not recognize this identity file
        # unless base_url is set to "elm" rather than "elm.ttic.edu"
        #
        # paramiko also parses `~/.ssh/config/known_hosts` to look up hosts -> identity key mapping
        # When this works, the issue I wrote above doesn't matter (in my understanding)
        # However, somehow for the first ssh to a remote machine (after login),
        # paramiko seems to always fail to parse the file (??)
        #
        # Because of all the complications,
        # I'd prefer to use `use_ssh_client=True` that uses ssh binary rather than paramiko

        client = DockerClient(base_url=base_url, use_ssh_client=True)

        # Specify job name
        name = f'{machine.user}-lmn-{project.name}'
        if runtime_options.name is not None:
            name = f'{name}--{runtime_options.name}'

        if parsed.dry_run:
            raise ValueError('dry run is not yet supported for Docker mode')

        # from lmn.cli._config_loader import DOCKER_ROOT_DIR, get_docker_lmndirs
        # docker_lmndirs = get_docker_lmndirs(DOCKER_ROOT_DIR, project.name)
        docker_lmndirs = machine.container_lmndirs

        docker_pconf = machine.parsed_conf.docker
        docker_pconf.name = name

        if startup:
            logger.warn('`startup` configurations outside of `docker` will be ignored in docker mode.')
            logger.warn('Please place `startup` under `docker` if you want to run it in the container.')

        if not runtime_options.no_sync:
            docker_pconf.mount_from_host.update({
                f'{machine.lmndirs.codedir}': machine.container_lmndirs.codedir,
                f'{machine.lmndirs.outdir}': machine.container_lmndirs.outdir,
                f'{machine.lmndirs.mountdir}': machine.container_lmndirs.mountdir,
                f'{machine.lmndirs.scriptdir}': machine.container_lmndirs.scriptdir,
            })

        if project.mount_from_host:
            # Backward compatible
            logger.warn("`mount_from_host` under `project` is deprecated. Please move it under `docker`.")
            docker_pconf.mount_from_host.update(project.mount_from_host)

        docker_pconf.env.update(env)

        docker_runner = DockerRunner(client, docker_lmndirs)

        print_conf(mode, machine, docker_pconf.image)
        if parsed.sweep:
            if not runtime_options.disown:
                logger.error("You must set -d option to use sweep functionality.")
                import sys; sys.exit(1)
            sweep_ind = parse_sweep_idx(parsed.sweep)

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

    elif 'slurm' in mode or 'pbs' in mode:
        # validate the mode
        if mode not in ['slurm', 'slurm-sing', 'sing-slurm', 'pbs', 'pbs-sing', 'sing-pbs']:
            logger.error(f'Invalid mode: {mode}')
            import sys; sys.exit(1)

        handler_scheduler(project, machine, parsed, preset, mode, runtime_options)

    else:
        raise ValueError(f'Unrecognized mode: {mode}')

    # Sync output files
    if not runtime_options.no_sync and not runtime_options.disown:
        _sync_output(project, machine, dry_run=parsed.dry_run)


def handler_scheduler(
    project: Project,
    machine: Machine,
    parsed: Namespace,
    preset: dict,
    mode: Literal['slurm', 'slurm-sing', 'sing-slurm', 'pbs', 'pbs-sing', 'sing-pbs'],
    run_opt: Namespace,
    ):
    import random
    import randomname

    startup = ' ; '.join([e for e in [project.startup, machine.startup] if e.strip()])
    lmndirs = machine.lmndirs

    ssh_client = CLISSHClient(machine.remote_conf)

    if 'slurm' in mode:
        from lmn.scheduler.slurm import SlurmConfig

        if machine.parsed_conf.slurm is None:
            raise ValueError('Configuration must have an entry for "slurm" to use Slurm mode.')

        # Create SlurmConfig object
        if parsed.sconf is None:
            scheduler_conf = machine.parsed_conf.slurm
        else:
            # Try to load from the slurm conf presets
            logger.debug('parsed.sconf is specified. Loading custom preset conf.')
            _sconf = preset.get('slurm-configs', {}).get(parsed.sconf, {})
            if not _sconf:
                logger.error(f'Slurm config preset: "{parsed.sconf}" cannot be found in "slurm-configs" or is empty.')
                import sys; sys.exit(1)
            scheduler_conf = SlurmConfig(**_sconf)
            logger.info(f'Using Slurm preset: [{parsed.sconf}]')

        runner = SlurmRunner(ssh_client, lmndirs)

    elif 'pbs' in mode:
        from lmn.scheduler.pbs import PBSConfig

        if machine.parsed_conf.pbs is None:
            raise ValueError('Configuration must have an entry for "pbs" to use PBS mode.')

        # Create PBSConfig object
        if parsed.pbsconf is None:
            scheduler_conf = machine.parsed_conf.pbs
        else:
            # Try to load from the slurm conf presets
            logger.debug('parsed.sconf is specified. Loading custom preset conf.')
            _pconf = preset.get('pbs-configs', {}).get(parsed.pbsconf, {})
            if not _pconf:
                logger.error(f'PBS config preset: "{parsed.pbsconf}" cannot be found in "pbs-configs" or is empty.')
                import sys; sys.exit(1)
            scheduler_conf = PBSConfig(**_pconf)
            logger.info(f'Using PBS preset: [{parsed.pbsconf}]')

        runner = PBSRunner(ssh_client, lmndirs)

    else:
        logger.error(f'Unrecognized mode: {mode}')
        import sys; sys.exit(1)

    # NOTE: Slurm / PBS seem to be fine with duplicated name.
    proj_name_maxlen = 15
    rand_num = random.randint(0, 100)
    job_name = f'lmn-{project.name[:proj_name_maxlen]}-{randomname.get_name()}-{rand_num}'

    # Overwrite job name
    scheduler_conf.job_name = job_name

    if run_opt.force:
        logger.warn("`-f / --force` option has no effect in Slurm / PBS mode")

    # Specify job name
    name = f'{machine.user}-lmn-{project.name}'
    if run_opt.name is not None:
        name = f'{name}--{run_opt.name}'
    scheduler_conf.job_name = name

    if 'sing' in mode:
        from lmn.container.singularity import SingularityConfig, SingularityCommand

        # Error checking
        if machine.parsed_conf.singularity is None:
            raise ValueError('Entry "singularity" not found in the config file.')

        # Overwrite lmn envvars.  Hmm I don't like this...
        # from lmn.cli._config_loader import DOCKER_ROOT_DIR, get_docker_lmndirs
        # sing_lmndirs = get_docker_lmndirs(DOCKER_ROOT_DIR, project.name)
        sing_lmndirs = machine.container_lmndirs

        # Update `pwd` entry
        sing_conf = machine.parsed_conf.singularity
        sing_conf.pwd = str(sing_lmndirs.codedir / run_opt.rel_workdir)

        # TODO: Use get_lmndirs function!
        # TODO: Do we need this??
        sing_env = {
            'LMN_PROJECT_NAME': project.name,
            'LMN_CODE_DIR': sing_lmndirs.codedir,
            'LMN_MOUNT_DIR': sing_lmndirs.mountdir,
            'LMN_OUTPUT_DIR': sing_lmndirs.outdir,
            'LMN_SCRIPT_DIR': sing_lmndirs.scriptdir,
        }

        # Backward compat
        sing_env.update({
            'RMX_PROJECT_NAME': project.name,
            'RMX_CODE_DIR': sing_lmndirs.codedir,
            'RMX_MOUNT_DIR': sing_lmndirs.mountdir,
            'RMX_OUTPUT_DIR': sing_lmndirs.outdir,
            'RMX_SCRIPT_DIR': sing_lmndirs.scriptdir,
        })

        # Bind
        mount_from_host = {}
        if not run_opt.no_sync:
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
        sing_conf.env_from_host += ['SLURM_JOBID', 'SLURM_JOB_ID', 'SLURM_TASK_PID']

        # Integrate current `env`, `mount_from_host`
        sing_conf.env.update({**sing_env, **project.env})
        sing_conf.mount_from_host.update(mount_from_host)

        # Finally overwrite run_opt.cmd
        run_opt.cmd = SingularityCommand.run(run_opt.cmd, sing_conf)

    timestamp = f"{time.time():.5f}".split('.')[-1]  # 5 subdigits of the timestamp
    print_conf(mode, machine, image=sing_conf.sif_file if mode in ['slurm-sing', 'sing-slurm'] else None)

    env = {**project.env, **machine.env}
    if parsed.sweep:
        if not run_opt.disown:
            logger.error("You must set -d option to use sweep functionality.")
            import sys; sys.exit(1)

        if not parsed.contain:
            logger.error("You should set --contain option to use sweep functionality.")
            import sys; sys.exit(1)

        sweep_ind = parse_sweep_idx(parsed.sweep)

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

            # Add sweep_idx to the job name
            _scheduler_conf.job_name = f'{scheduler_conf.job_name}-{timestamp}-{sweep_idx}'
            logger.info(f'Launching sweep {sweep_idx}: {_scheduler_conf.job_name}')

            runner.exec(run_opt.cmd, run_opt.rel_workdir, conf=_scheduler_conf,
                        startup=startup,
                        timestamp=timestamp,
                        interactive=False, num_sequence=run_opt.num_sequence,
                        env=env, dry_run=parsed.dry_run)
    else:
        runner.exec(run_opt.cmd, run_opt.rel_workdir, conf=scheduler_conf,
                    startup=startup, timestamp=timestamp, interactive=not run_opt.disown, num_sequence=run_opt.num_sequence,
                    env=env, dry_run=parsed.dry_run)


name = 'run'
description = 'run command'
parser = _get_parser()
