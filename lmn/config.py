from __future__ import annotations
from pydantic import BaseModel
from typing import Optional, List, Union

from lmn.container import DockerContainerConfig, SingularityConfig
from lmn.scheduler import SlurmConfig, PBSConfig


class ProjectConfig(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    environment: dict = {}
    outdir: Optional[str] = None
    mount_from_host: dict = {}
    exclude: List[str] = []
    startup: Union[str, List[str]] = ''


class MachineConfig(BaseModel):
    user: str
    host: str
    root_dir: Optional[str] = None
    environment: dict = {}
    mount_from_host: dict = {}  # Deprecated; Moved to container config (Docker and Singularity)
    startup: Union[str, List[str]] = ''
    mode: str = 'ssh'

    # LMN Directories
    lmndirs: Optional[OptionalLMNDirectories] = None
    container_lmndirs: Optional[OptionalLMNDirectories] = None

    # Container config
    docker: Optional[DockerContainerConfig] = None
    singularity: Optional[SingularityConfig] = None

    # Scheduler config
    slurm: Optional[SlurmConfig] = None
    pbs: Optional[PBSConfig] = None


class LMNDirectories(BaseModel):
    codedir: str
    mountdir: str
    outdir: str
    scriptdir: str
    rootdir: str


class OptionalLMNDirectories(BaseModel):
    codedir: Optional[str] = None
    mountdir: Optional[str] = None
    outdir: Optional[str] = None
    scriptdir: Optional[str] = None
    rootdir: Optional[str] = None
