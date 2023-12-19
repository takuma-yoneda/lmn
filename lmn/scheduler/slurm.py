#!/usr/bin/env python3
from __future__ import annotations
from pydantic import BaseModel
from typing import Optional

# TODO: Implement SlurmCommand by myself
from simple_slurm_command import SlurmCommand


class SlurmConfig(BaseModel):
    """Let's keep it minimal. Conceptually it's better to store SlurmConfig here,
    but that would make it harder to read.
    """
    job_name: str = 'default-job-name'  # To be filled
    partition: str = 'cpu'
    constraint: Optional[str] = None
    cpus_per_task: int = 1
    time: Optional[str] = None
    output: Optional[str] = None
    error: Optional[str] = None
    dependency: Optional[str] = None
    nodelist: Optional[str] = None
    exclude: Optional[str] = None
    shell: str = 'bash'