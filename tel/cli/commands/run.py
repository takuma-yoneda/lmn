#!/usr/bin/env python3

import argparse
import pathlib
from tel.cli import AbstractCLICommand
from typing import Optional, List

Arguments = List[str]
RSYNC_DESTINATION_PATH = "/tmp/".rstrip('/')

class CLIRunCommand(AbstractCLICommand):

    KEY = 'run'

    @staticmethod
    def parser(parent: Optional[argparse.ArgumentParser] = None,
               args: Optional[Arguments] = None) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(parents=[parent])

        parser.add_argument(
            "machine",
            const=True,
            action="store",
            type=str,
            help="Machine",
        )
        parser.add_argument(
            "-m",
            "--mode",
            const=True,
            action="store",
            type=str,
            default=None,
            choices=["ssh", "docker", "slurm", "singularity", "sing-slurm"],
            help="What mode to run",
        )
        parser.add_argument(
            "-v",
            "--mount",
            default=False,
            const=True,
            action="store",
            nargs="?",
            type=str,
            help="Whether to mount the current project into the container. "
                 "Pass a comma-separated list of paths to mount multiple projects",
        )
        parser.add_argument(
            "-d",
            "--disown",
            action="store_true",
            help="Do not block to wait for the process to exit. stdout/stderr will not be shown with this option.",
        )
        parser.add_argument(
            "-L",
            "--local-forward",
            type=str,
            action="store",
            help="Local port forwarding. A general format is '1234:hostname:2345'."
                 "You can also just specify a number like '8080'. This is interpreted as '8080:localhost:8080."
                 "This cannot be used with -d/--disown option.",
        )
        parser.add_argument(
            "-R",
            "--remote-forward",
            action="store",
            help="Remote port forwarding. A general format is '1234:hostname:2345'."
                 "You can also just specify a number like '8080'. This is interpreted as '8080:localhost:8080."
                 "This cannot be used with -d/--disown option.",
        )
        parser.add_argument(
            "-X",
            "--x-forward",
            action="store_true",
            help="X11 forwarding",
        )
        parser.add_argument(
            "--entrypoint",
            action="store",
            help="Docker entrypoint",
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

    @staticmethod
    def execute(config: dict, parsed: argparse.Namespace, relative_workdir: pathlib.Path=pathlib.Path('.')) -> None:
        """Deploy the local repository and execute the command on a machine.

        1. get SSH connection to machine
        2. run rsync(project.root_dir, project.remote_rootdir)
        3. run machine.execute(parsed.remote_command)
        """
        from tel.cli.utils import rsync

        # Read from tel config file and reflect it
        # TODO: clean this up
        from tel.machine import RemoteConfig
        if parsed.machine not in config['machines']:
            raise KeyError(
                f'Machine {parsed.machine} not found in the configuration.\n'
                f'Available machines are: {config["machines"].keys()}'
            )
        machine_conf = config['machines'].get(parsed.machine)
        user, host = machine_conf['user'], machine_conf['host']
        remote_conf = RemoteConfig(user, host)


        from os.path import join as pjoin
        from tel.project import Project
        project = Project(name=parsed.name,
                          root_dir=parsed.workdir,
                          remote_dir=pjoin(machine_conf['root_dir'], parsed.name) if 'root_dir' in machine_conf else None,
                          out_dir=parsed.outdir)
        print('project:', project)


        mode = parsed.mode if parsed.mode else machine_conf.get('default_mode', 'ssh')

        from tel.machine import SSHClient
        ssh_client = SSHClient(remote_conf)

        # rsync the remote directory
        # A trick to create non-existing directory before running rsync (https://www.schwertly.com/2013/07/forcing-rsync-to-create-a-remote-path-using-rsync-path/)
        rsync_options = f"--rsync-path='mkdir -p {project.remote_rootdir} && mkdir -p {project.remote_outdir} && rsync'"
        rsync(source_dir=project.root_dir, target_dir=ssh_client.uri(project.remote_rootdir), options=rsync_options)

        if mode == "ssh":
            from tel.machine import SSHMachine
            ssh_machine = SSHMachine(ssh_client, project)
            ssh_machine.execute(parsed.remote_command, relative_workdir, disown=parsed.disown, shell=True, x_forward=parsed.x_forward)

        elif mode == "docker":
            from docker import DockerClient
            from tel.config import DockerContainerConfig
            from tel.machine import DockerMachine
            base_url = "ssh://" + remote_conf.base_uri
            docker_client = DockerClient(base_url=base_url, use_ssh_client=True)


            # TODO: This looks horrible. Clean up.
            # If args.image is given, look for the corresponding name in config
            # if not found, regard it as a full name of the image
            # NOTE: Currently it only parses image name !!!
            if parsed.image:
                if parsed.image in config['docker-images']:
                    image = config['docker-images'].get(parsed.image).get('name')
                else:
                    image = parsed.image
            else:
                image = machine_conf.get('docker').get('name')
            if image is None:
                raise RuntimeError("docker image cannot be parsed. Something may be wrong with your docker configuration?")

            docker_conf = DockerContainerConfig(image, f'{user}-{project.name}')
            print('docker_conf:', docker_conf)

            docker_machine = DockerMachine(docker_client, project, docker_conf=docker_conf)
            docker_machine.execute(parsed.remote_command, shell=True, use_gpus=True, x_forward=parsed.x_forward)

        elif mode == "slurm":
            from tel.config import SlurmConfig
            from tel.machine import SlurmMachine

            # TODO: Also parse from slurm config options (aside from default)
            # Parse slurm configuration
            slurm_conf = SlurmConfig(**machine_conf.get('slurm'))
            print('slurm_conf', slurm_conf)

            slurm_machine = SlurmMachine(ssh_client, project, slurm_conf)
            slurm_machine.execute(parsed.remote_command, relative_workdir, interactive=not parsed.disown, n_sequence=parsed.n_sequence)

        elif mode == "singularity":
            raise NotImplementedError()
        elif mode == "sing-slurm":
            raise NotImplementedError()
        else:
            raise KeyError('mode: {parsed.mode} is not available.')

        # Rsync remote outdir with the local outdir.
        if project.out_dir:
            from tel.cli.utils import rsync
            # Check if there's any output file (the first line is always 'total [num-files]')
            result = ssh_client.run(f'ls -l {project.remote_outdir} | grep -v "^total" | wc -l', hide=True)
            num_output_files = int(result.stdout)
            print(f'{num_output_files} files are in the output directory')
            if num_output_files:
                rsync(source_dir=ssh_client.uri(project.remote_outdir), target_dir=project.out_dir)




        # TODO: later; Set up port forwarding
        # Check confilicts in parsed arguments
        # assert not ((parsed.disown and parsed.local_forward) or (parsed.disown and parsed.remote_forward)), '-d/--disown option cannot be used with port forwarding option.'
        # conn = machine.remote_conf.get_connection()

        # if parsed.local_forward:
        #     if ':' in parsed.local_forward:
        #         local_port, remote_host, remote_port = parsed.local_forward.split(':')
        #     else:
        #         local_port, remote_host, remote_port = parsed.local_forward, 'localhost', parsed.local_forward

        #     port_forward = lambda : conn.forward_local(local_port, remote_port, remote_host)
        #     # TODO: I should write a new context manager that internally either use local/remote forward or none.
        #     # You can actually copy & paste from the source. The lines of code is not that big.
