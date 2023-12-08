#!/usr/bin/env python3

# TODO: Implement SlurmCommand by myself
from simple_slurm_command import SlurmCommand


class SlurmConfig:
    """Let's keep it minimal. Conceptually it's better to store SlurmConfig here,
    but that would make it harder to read.
    """
    def __init__(self, job_name: str, partition='cpu', constraint=None, cpus_per_task=1, time=None,
                 output=None, error=None, dependency=None, nodelist=None, exclude=None, shell='bash', **kwargs) -> None:
        self.job_name = job_name
        self.partition = partition
        self.constraint = constraint
        self.exclude = exclude
        self.cpus_per_task = cpus_per_task
        self.time = time
        self.dependency = dependency
        self.output = output
        self.error = error
        self.nodelist = nodelist
        self.shell = shell