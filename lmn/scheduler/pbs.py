#!/usr/bin/env python3
from typing import List, Optional
from copy import deepcopy
from pydantic import BaseModel


class PBSConfig(BaseModel):
    job_name: str = 'default-job-name'  # To be filled
    account: str = 'SuperBERT'  # Account string
    queue: str = 'debug'  # debug, small, medium, large, etc. (Check `qstat -q`)
    shell: str = '/usr/bin/env bash'
    filesystems: str = 'home:grand'  # Request access to /home and /grand directories
    select: int = 1  # Request 1 node
    place: str = 'free'  # scatter, pack (default): specify how to distribute allocations (I believe it only matters for multi-node allocation ?)
    walltime: str = '1:00:00'  # 1 hour for debug queue, 72 hours for preemptable queue

    @property
    def resource_list(self):
        return [
            f'place={self.place}', f'walltime={self.walltime}',
            f'filesystems={self.filesystems}', f'select={self.select}'
        ]


class PBSCommand:
    @staticmethod
    def _valid_key(key: str) -> str:
        '''Long arguments (for slurmCommand) constructed with '-' have been internally
        represented with '_' (for Python). Correct for this in the output.
        '''
        return key.replace('_', '-')

    @staticmethod
    def make_options(pbs_config: dict) -> List[str]:
        """Return a list of arguments.
        Example: ['-I', '-l filesystems=home:grand', '-l select=1', ...]
        """
        needs_special_handling = ['l', 'I']
        pbs_config = deepcopy(pbs_config)

        # # Process space
        # pbs_config = {
        #     k: f"'{v}'" if isinstance(v, str) and " " in v else v
        #     for k, v in pbs_config.items()
        # }

        # We need a special handling on `-l` option, since it is allowed to appear multiple times in a command.
        args = [
            f"-{PBSCommand._valid_key(k)} {v}"
            for k, v in pbs_config.items()
            if k not in needs_special_handling and v is not None
        ]

        # Expand `-l` option (list)
        if 'l' in pbs_config:
            assert isinstance(pbs_config['l'], (list, tuple))
            args += [f'-l {v}' for v in pbs_config['l']]

        # Handle `-I` option (bool)
        if 'I' in pbs_config:
            assert isinstance(pbs_config['I'], bool)
            if pbs_config['I']:
                args += ['-I']

        return args

    @staticmethod
    def qsub_from_dict(run_cmd: str, pbs_config: dict, qsub_cmd: str = 'qsub', interactive: bool = False,
                       convert: bool = True):

        args = PBSCommand.make_options(pbs_config)
        if interactive:
            # NOTE:
            # ### Issues in interactive mode ###
            # It's tricky to execute a script in an interactive mode on PBS.
            # Whenever we use `-I` (interactive) option on qsub like:
            # `qsub <options> -I <script>`,
            # the script is simply ignored (actually, only PBS directives, the lines starting with `#PBS`, are respected).
            # ### Workaround ###
            # To overcome this issue, we can use `--` option with `bash -c` like:
            # `qsub <options> -I -- /usr/bin/env bash -c '<command>'`
            # or
            # `qsub <options> -I -- /bin/bash -c '<command>'`
            # This way, we can execute a script and a command in an interactive mode.
            # If we use a script, just make sure that `bash -c` is included in your script file.
            # ### Another workaround ###
            # I found that we can also use `-S` option to execute a script (this is originally for specifying a shell).
            # `qsub <options> -I -S <script>`
            cmd = ' '.join((qsub_cmd, *args, '--', run_cmd))
        else:
            batch_args = (f'#PBS {arg}' for arg in args)
            cmd = '\n'.join((
                qsub_cmd + ' << EOF',
                '\n'.join(('#!/usr/bin/env bash', '', *batch_args, '')),
                run_cmd.replace('$', '\\$') if convert else run_cmd,
                'EOF',
            ))

        return cmd

    @staticmethod
    def qsub(run_cmd: str, pbs_config: PBSConfig, qsub_cmd: str = 'qsub', interactive: bool = False,
             convert: bool = True):

        pbs_dict = {
            # a: # date and time
            'A': pbs_config.account,  # Account string
            # c
            # C
            # 'e': pbs_config.stderr,  # Path to be used for the job's standard error stream
            # 'f': pbs_config.force_foreground,  # Prevents qsub from spawning a background process.
            # h
            'I': interactive,  # Job is to be run interactively
            # 'j': # Specifies whether and how to join the job's standard error and standard output streams.
            # 'J': pbs_config.array,  # Makes this job an array job. format: `-J <range> [%<max subjobs>]`
            # 'k': # Specifies whether and which of the standard output and standard error streams is left behind on the execution host, or written to their final destination. (Default: ; neither is retained)
            'l': pbs_config.resource_list,  # Allows the user to request resources and specify job placement.
            # m
            # M
            'N': pbs_config.job_name,
            # 'o': pbs_config.stdout,  # Path to be used for the job's standard output stream.
            # 'p': # Priority of the job.  Sets job's Priority attribute to priority. (Range: [-1024, 1023], Default: Zero)
            # P
            'q': pbs_config.queue,  # Where the job is sent upon submission.
            # 'r':  # Declares whether the job is rerunnable. format: `-r <y|n>`
            # R  # Specifies whether standard output and/or standard error files are automatically removed (deleted) upon job completion.
            # 'S': pbs_config.shell,
            # u
            # 'v': ...,  # Lists environment variables and shell functions to be exported to the job. <- Let's rather use export explicitly.
            # V  # All environment variables and shell functions in the user's login environment where qsub is run are exported to the job.
            # 'W': ...,  # The -W option allows specification of some job attributes.  Some job attributes must be specified using this option.
            # z  # Job identifier is not written to standard output.
        }
        return PBSCommand.qsub_from_dict(run_cmd, pbs_dict, qsub_cmd, interactive, convert)
