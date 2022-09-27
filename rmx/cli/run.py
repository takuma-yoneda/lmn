from argparse import ArgumentParser
from rmx import logger
from rmx.config import SlurmConfig
from rmx.helpers import replace_rmx_envvars
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


def handler(project, machine: Machine, runtime_options):
    logger.info(f'handling command for {__file__}')
    logger.info(f'options: {runtime_options}')

    # Sync code first
    _sync_code(project, machine, runtime_options)

    # TODO: Collect all envvars!!
    env = {**project.env, **machine.env}

    rmxdirs = machine.get_rmxdirs(project.name)
    startup = '&&'.join([e for e in [machine.startup, project.startup] if e.strip() != ""])
    if machine.mode == 'ssh':
        from rmx.runner import SSHRunner
        ssh_client = SimpleSSHClient(machine.remote_conf)
        ssh_runner = SSHRunner(ssh_client, rmxdirs)
        ssh_runner.exec(runtime_options.cmd, 
                        runtime_options.rel_workdir,
                        startup=startup,
                        env=env,
                        dry_run=runtime_options.dry_run)

    elif machine.mode == 'docker':
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


        docker_runner = DockerRunner(client, docker_rmxdirs)

        if runtime_options.sweep:
            assert runtime_options.disown, "You must set -d option to use sweep functionality."
            sweep_ind = parse_sweep_idx(runtime_options.sweep)

            for sweep_idx in sweep_ind:
                env.update({'RMX_RUN_SWEEP_IDX': sweep_idx})
                docker_conf = DockerContainerConfig(
                    image=machine.docker.image,
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
                image=machine.docker.image,
                name=name,
                mounts=mounts,
                env=env
            )
            docker_runner.exec(runtime_options.cmd,
                               runtime_options.rel_workdir,
                               docker_conf,
                               interactive=not runtime_options.disown,
                               kill_existing_container=runtime_options.force)


    elif machine.mode == "slurm" or machine.mode == 'slurm-sing':
        # logger.info(f'slurm_conf: {machine.sconf}')
        ssh_client = SimpleSSHClient(machine.remote_conf)
        slurm_runner = SlurmRunner(ssh_client, rmxdirs)
        run_opt = runtime_options

        # Specify job name
        name = f'{machine.user}-rmx-{project.name}'
        if runtime_options.name is not None:
            name = f'{name}--{runtime_options.name}'
        machine.slurm_conf.job_name = name

        if machine.mode == 'slurm-sing':
            # Decorate run_opt.cmd for singularity

            # Overwrite rmx envvars.  Hmm I don't like this...
            sing_rmxdirs = machine.sing.get_rmxdirs(project.name)
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
            options += [bind.format(target=sing_rmxdirs.codedir, source=rmxdirs.codedir),
                      bind.format(target=sing_rmxdirs.outdir, source=rmxdirs.outdir),
                      bind.format(target=sing_rmxdirs.mountdir, source=rmxdirs.mountdir)]
            options += [bind.format(target=tgt, source=src) for src, tgt in project.mount_from_host.items()]

            # Overlay
            if machine.sing.overlay:
                options += [f'--overlay {machine.sing.overlay}']

            # TEMP:
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

            for sweep_idx in sweep_ind:
                env.update({'RMX_RUN_SWEEP_IDX': sweep_idx})
                slurm_runner.exec(run_opt.cmd, run_opt.rel_workdir, slurm_conf=machine.slurm_conf, 
                                  startup=startup,
                                  interactive=False, num_sequence=run_opt.num_sequence,
                                  env=env, dry_run=run_opt.dry_run)
        else:
            slurm_runner.exec(run_opt.cmd, run_opt.rel_workdir, slurm_conf=machine.slurm_conf,
                              startup=startup, interactive=not run_opt.disown, num_sequence=run_opt.num_sequence,
                              env=env, dry_run=run_opt.dry_run)

    # Sync output files
    _sync_output(project, machine, runtime_options)

name = 'run'
description = 'run command'
parser = _get_parser()
