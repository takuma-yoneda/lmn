#!/usr/bin/env python3
import os
from os.path import expandvars
import docker
from rmx import logger

class DockerContainerConfig:
    def __init__(self, image, name, remove=True, network='host', ipc_mode='host', mounts=None,
                 detach=True, tty=True, environment=None, device_requests=None, runtime='docker') -> None:
        self.image = image
        self.name = name
        self.remove = remove
        self.network = network
        self.ipc_mode = ipc_mode
        self.mounts = [] if mounts is None else mounts
        self.detach = detach
        self.tty = tty
        self.environment = dict() if environment is None else environment
        self.device_requests = [] if device_requests is None else device_requests
        self.runtime = runtime

    def __repr__(self):
        properties = {key: val for key, val in vars(self).items()}
        return repr(f'[DockerContainerConfig]\n{properties}')

    def use_gpus(self):
        """Add gpus to device_requests

        https://github.com/docker/docker-py/issues/2395#issuecomment-907243275
        """
        gpu = docker.types.DeviceRequest(
            count=-1,
            capabilities=[['gpu'], ['nvidia'], ['compute'], ['compat32'], ['graphics'], ['utility'], ['video'], ['display']]
        )

        # Does "in" work here???
        if gpu not in self.device_requests:
            self.device_requests.append(gpu)

    def add_mount(self, target, source, type='bind', read_only=False):
        """Add a mount point"""
        from docker.types import Mount
        self.mounts.append(Mount(target=target, source=source, type=type, read_only=read_only))

    def use_x_forward(self, target_home=expandvars('$HOME')):
        """Set up X11 forwarding

        target_home: path to the home directory (on the remote server)
        Corresponds to https://github.com/afdaniele/x-docker
        """
        import shutil
        import subprocess

        # If installed with brew, it's stored in /opt/X11/bin/xhost, and it's not automatically appended to PATH
        xhost_command = '/opt/X11/bin/xhost' if os.path.isfile('/opt/X11/bin/xhost') else 'xhost'
        if not shutil.which(xhost_command):
            raise OSError('xhost not found. If you are on macOS, you need to install XQuartz.')

        # If using XQuartz, the display variables looks
        # something like: /private/tmp/com.apple.launchd.jY5AhC1lFM/org.xquartz:0
        # which doesn't work (??)
        if 'xquartz' in os.environ.get('DISPLAY', ''):
            from rmx.cli.utils import run_cmd
            ip = run_cmd('ifconfig en0 | grep inet | awk \'$1=="inet" {print $2}\'', get_output=True, shell=True)
            display = f'{ip}:0'
        else:
            display = os.environ.get('DISPLAY', ':0'),

        # TODO: In addition to this, there needs to be a X forwarding from the remote to local
        # paramiko should support that.

        logger.info(f'display: {display}')
        self.environment.update({
            'DISPLAY': display,
            'NVIDIA_VISIBLE_DEVICES': 'all',  # Only if you have nvidia gpus
            'NVIDIA_DRIVER_CAPABILITIES': 'all',  # Only if you have nvidia gpus
            'QT_X11_NO_MITSHM': '1'  # support QT
        })
        # self.runtime = 'nvidia'  # You may need it if docker version is old. This is deprecated now.

        # run `xhost +local:root`
        subprocess.check_call([xhost_command, "+local:root"])

        # mount x-server socket
        self.add_mount(target='/tmp/.X11-unix', source='/tmp/.X11-unix')

        # TODO: Somehow needs to obtain $HOME in the remote server!
        # some sort of hook that is executed right before docker.run should work.
        # NOTE: This assumes the user in docker container is "root"
        self.add_mount(target='/root/.Xauthority', source=f'{target_home}/.Xauthority')


class SlurmConfig:
    """Let's keep it minimal. Conceptually it's better to store SlurmConfig here,
    but that would make it harder to read.
    """
    def __init__(self, partition='cpu', constraint=None, cpus_per_task=1, time=None,  output=None, error=None, dependency=None, exclude=None, **kwargs) -> None:
        self.partition = partition
        self.constraint = constraint
        self.exclude = exclude
        self.cpus_per_task = cpus_per_task
        self.time = time
        self.dependency = dependency
        self.output = output
        self.error = error


class SingularityConfig:
    pass
