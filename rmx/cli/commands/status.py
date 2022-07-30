#!/usr/bin/env python3

import argparse
import pathlib
from rmx.cli import AbstractCLICommand
from typing import Optional, List
from rmx import logger

Arguments = List[str]
RSYNC_DESTINATION_PATH = "/tmp/".rstrip('/')

class CLIStatusCommand(AbstractCLICommand):

    KEY = 'status'

    @staticmethod
    def parser(parent: Optional[argparse.ArgumentParser] = None,
               args: Optional[Arguments] = None) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(parents=[parent])

        parser.add_argument(
            "machine",
            action="store",
            type=str,
            help="Machine",
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
        return parser

    @staticmethod
    def execute(config: dict, parsed: argparse.Namespace, relative_workdir: pathlib.Path=pathlib.Path('.')) -> None:
        """Currenlty only support slurm
        1. execute `squeue -u $USER`
        2. parse

        sacct command:
        Thanks to
        - https://slurm.schedmd.com/sacct.html
        - https://rc.byu.edu/wiki/?id=Using+sacct
        sacct --starttime $(date -d '40 hours ago' +%D-%R) --endtime now --format JobID,JobName%-100,NodeList,Elapsed,State,ExitCode,MaxRSS --parsable2
        """
        from rmx.cli.utils import rsync

        # Read from rmx config file and reflect it
        # TODO: clean this up
        from rmx.machine import RemoteConfig
        if parsed.machine not in config['machines']:
            raise KeyError(
                f'Machine "{parsed.machine}" not found in the configuration. '
                f'Available machines are: {" ".join(config["machines"].keys())}'
            )
        machine_conf = config['machines'].get(parsed.machine)
        user, host = machine_conf['user'], machine_conf['host']
        remote_conf = RemoteConfig(user, host)

        # NOTE: Order to check 'mode'
        # 1. If specified in cli --> use that mode
        # 2. If default_mode is set (config file) --> use that mode
        # 3. Use ssh mode
        mode = parsed.mode if parsed.mode else machine_conf.get('default_mode', 'ssh')

        from rmx.machine import SSHClient
        ssh_client = SSHClient(remote_conf)

        # Curerntly only slurm is supported
        if mode == "slurm":
            from rmx.helpers import sacct_cmd, parse_sacct
            result = ssh_client.run(sacct_cmd, hide=True)
            sacct_entries = parse_sacct(result.stdout)
            # logger.info(f'entries:\n{sacct_entries}')  # TEMP

        elif mode == "singularity":
            raise NotImplementedError()
        elif mode == "sing-slurm":
            raise NotImplementedError()
        else:
            raise KeyError('mode: {parsed.mode} is not available.')


        # Convert sacct_entries (List) to a dictionary of {jobid: entry}
        sacct_dict = {sacct_entry['JobID']: sacct_entry for sacct_entry in sacct_entries}


        # TODO: Replace this with LaunchLogManager.read
        import os
        import json
        from os.path import expandvars
        launch_logfile = expandvars('$HOME/.rmx/launched.json')
        assert os.path.isfile(launch_logfile)
        with open(launch_logfile, 'r') as f:
            data = json.load(f)

        # Compatibility
        # Always use old --> new order
        data = data[::-1]

        # Construct {envvar-at-submission: [(jobid, state), (jobid, state), ...]}
        unified_entries = {}
        for entry in data:
            if 'jobid' not in entry:
                continue

            jobid = entry['jobid']
            if jobid not in sacct_dict:
                continue

            envvar = frozenset(entry['envvar'].items())
            # user_cmd = entry['envvar']['RMX_USER_COMMAND']
            if envvar in unified_entries:
                unified_entries[envvar] += [(jobid, sacct_dict[jobid]['State'])]
            else:
                unified_entries[envvar] = [(jobid, sacct_dict[jobid]['State'])]


        # Merge the list of (jobid, state) pairs to create
        # {envvar-at-submission: state}
        # a. the last state is CANCELLED by xxx --> use prev jobid, state pair
        # b. otherwise, just use the last state (COMPLETED, FAILED, TIMEOUT, PENDING, RUNNING)
        def reduce(job_status_pairs):
            _, status = job_status_pairs[-1]
            if status.startswith('CANCELLED'):
                if len(job_status_pairs) == 1:
                    # The job has never run properly yet.
                    return 'NOT_STARTED'
                return reduce(job_status_pairs[:-1])
            return status

        unified_entries = {envvar: reduce(job_status_pairs) for envvar, job_status_pairs in unified_entries.items()}

        # Needs relaunch?:
        # 1. PENDING or RUNNING --> No need to relaunch
        # 2. FAILED or TIMEOUT or NOT_STARTED--> Needs relaunch
        from rmx.helpers import defreeze_dict
        logger.info('The following needs relaunch:')
        usrcmd2swp = {}
        for envvar, status in unified_entries.items():
            if status in ['PENDING', 'RUNNING', 'COMPLETED']:
                continue

            envvar = defreeze_dict(envvar)
            usrcmd = envvar['RMX_USER_COMMAND']
            swpidx = envvar['RMX_RUN_SWEEP_IDX']
            if usrcmd in usrcmd2swp:
                usrcmd2swp[usrcmd] += [swpidx]
            else:
                usrcmd2swp[usrcmd] = [swpidx]

            # envvar = defreeze_dict(envvar)
            # logger.info(f'{envvar["RMX_USER_COMMAND"]}\t{envvar.get("RMX_RUN_SWEEP_IDX")}\t{status}')
        for usrcmd, swp in usrcmd2swp.items():
            logger.info(f'user command: {usrcmd}\n' + ",".join(map(str, swp)))


        # Merge by user command
