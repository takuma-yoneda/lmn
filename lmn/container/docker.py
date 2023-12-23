from pydantic import BaseModel, Field
from typing import Optional, List, Union


def get_gpu_device():
    """Add gpus to device_requests, just for dockerpy

    https://github.com/docker/docker-py/issues/2395#issuecomment-907243275
    """
    from docker.types import DeviceRequest
    gpu = DeviceRequest(
        count=-1,
        capabilities=[['gpu'], ['nvidia'], ['compute'], ['compat32'], ['graphics'], ['utility'], ['video'], ['display']]
    )

    return gpu


class DockerContainerConfig(BaseModel):
    image: str
    name: Optional[str] = None
    env: dict = Field(alias="environment", default={})
    mount_from_host: dict = {}
    remove: bool = True
    network: str = 'bridge'
    ipc_mode: str = 'private'
    startup: Union[str, List[str], None] = None
    tty: bool = True
    gpus: str = 'all'
    user_id: int = 0
    group_id: int = 0
    runtime: str = 'docker'