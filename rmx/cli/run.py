from argparse import ArgumentParser
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
        "-m",
        "--mode",
        action="store",
        type=str,
        default=None,
        choices=["ssh", "docker", "slurm", "singularity", "sing-slurm"],
        help="What mode to run",
    )
    parser.add_argument(
        "-d",
        "--disown",
        action="store_true",
        help="Do not block to wait for the process to exit. stdout/stderr will not be shown with this option.",
    )
    parser.add_argument(
        "-X",
        "--x-forward",
        action="store_true",
        help="X11 forwarding",
    )
    parser.add_argument(
        "-n",
        "--num-sequence",
        action="store",
        type=int,
        default=1,
        help="number of sequence in Slurm sequential jobs"
    )
    # TEMP: Sweep functionality
    parser.add_argument(
        "--sweep",
        action="store",
        type=str,
        help="specify sweep range (e.g., --sweep 1-255) this is reflected to envvar $RMX_RUN_SWEEP_IDX"
                "Temporarily only available for slurm mode."
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


def handler(project, machine, runtime_options):
    print(f'handling command for {__file__}')
    print('options', runtime_options)

    # Sync code first
    _sync_code(project, machine, runtime_options)

    # TODO: Collect all envvars!!
    env = {**project.env, **machine.env}

    rmxdirs = machine.get_rmxdirs(project.name)
    startup = '&&'.join([e for e in [machine.startup, project.startup] if e.strip() == ""])
    if machine.mode == 'ssh':
        from rmx.runner import SSHRunner
        ssh_runner = SSHRunner(client, rmxdirs)
        ssh_runner.exec(runtime_options.cmd, 
                        runtime_options.rel_workdir,
                        startup=startup,
                        env=env,
                        dry_run=runtime_options.dry_run)

    elif machine.mode == 'docker':
        from docker import DockerClient, APIClient
        from rmx.runner import DockerRunner
        from rmx.config import DockerContainerConfig
        base_url = "ssh://" + machine.base_uri
        # client = DockerClient(base_url=base_url, use_ssh_client=True)
        client = DockerClient(base_url=base_url)  # dockerpty hangs with use_ssh_client=True

        if runtime_options.dry_run:
            raise ValueError('dry run is not yet supported for Docker mode')
        
        # TODO: This looks horrible. Clean up.
        # If args.image is given, look for the corresponding name in config
        # if not found, regard it as a full name of the image
        # NOTE: Currently it only parses image name !!!
        # if parsed.image:
        #     if parsed.image in config['docker-images']:
        #         image = config['docker-images'].get(parsed.image).get('name')
        #     else:
        #         image = parsed.image
        # else:
        #     image = machine_conf.get('docker').get('name')
        # if image is None:
        #     raise RuntimeError("docker image cannot be parsed. Something may be wrong with your docker configuration?")

        from docker.types import Mount
        # remote_rootdir = machine.rmxdir / project.name
        # remote_dir = remote_rootdir / 'code'
        # remote_outdir = remote_rootdir / 'output'
        # remote_mountdir = remote_rootdir / 'mount'
        docker_rmxdirs = machine.docker.get_rmxdirs(project.name)
        mounts = [Mount(target=docker_rmxdirs.codedir, source=rmxdirs.codedir, type='bind'),
                   Mount(target=docker_rmxdirs.outdir, source=rmxdirs.outdir, type='bind'),
                   Mount(target=docker_rmxdirs.mountdir, source=rmxdirs.mountdir, type='bind')]
        mounts += [Mount(target=tgt, source=src, type='bind') for src, tgt in project.mount_from_host.items()]

        docker_conf = DockerContainerConfig(
            image=machine.docker.image,
            name=f'{machine.user}-rmx-{project.name}',
            mounts=mounts,
            env=env
        )

        
        docker_runner = DockerRunner(client, docker_rmxdirs)
        docker_runner.exec(runtime_options.cmd,
                           runtime_options.rel_workdir,
                           docker_conf)
    


    elif machine.mode == "slurm":
        raise NotImplementedError('not refactored yet!')

        from rmx.config import SlurmConfig
        from rmx.machine import SlurmMachine

        # TODO: Also parse from slurm config options (aside from default)
        # Parse slurm configuration
        if parsed.conf:
            sconf = config['slurm-configs'].get(parsed.conf)
            if sconf is None:
                raise KeyError(f'configuration: {parsed.con} cannot be found in "slurm-configs".')
        else:
            sconf = machine_conf.get('slurm')

        slurm_conf = SlurmConfig(**sconf)
        logger.info(f'slurm_conf: {slurm_conf}')

        slurm_machine = SlurmMachine(ssh_client, project, slurm_conf)

        if parsed.sweep:
            assert parsed.disown, "You must set -d option to use sweep functionality."
            # Parse input
            # format #0: 8 --> 8
            # format #1: 1-10 --> range(1, 10)
            # format #2: 1,2,7 --> [1, 2, 7]
            if '-' in parsed.sweep:
                # format #1
                begin, end = [int(val) for val in parsed.sweep.split('-')]
                assert begin < end
                sweep_ind = range(begin, end)
            elif ',' in parsed.sweep:
                sweep_ind = [int(e) for e in parsed.sweep.strip().split(',')]
            elif parsed.sweep.isnumeric():
                sweep_ind = [int(parsed.sweep)]
            else:
                raise KeyError("Format for --sweep option is not recognizable. Format examples: '1-10', '8', '1,2,7'.")

            for sweep_idx in sweep_ind:
                env.update({'RMX_RUN_SWEEP_IDX': sweep_idx})
                slurm_machine.execute(parsed.remote_command, relative_workdir, startup=machine_conf.get('startup'),
                                        interactive=not parsed.disown, num_sequence=parsed.num_sequence, env=env, dry_run=parsed.dry_run, sweeping=True)
        else:
            slurm_machine.execute(parsed.remote_command, relative_workdir, startup=machine_conf.get('startup'),
                                    interactive=not parsed.disown, num_sequence=parsed.num_sequence, env=env, dry_run=parsed.dry_run)

    # Sync output files
    _sync_output(project, machine, runtime_options)

name = 'run'
description = 'run command'
parser = _get_parser()