#!/usr/bin/env python3
from __future__ import annotations
import os
from pathlib import Path
from typing import Iterator

def is_system_root(directory: Path):
    return directory == directory.parent

def is_home_dir(directory: Path):
    return directory.resolve() == Path.home().resolve()

def yield_parents(directory: Path, max_depth=None) -> Iterator[Path]:
    """generator returning the the current directory and ancestors one after another"""
    max_depth = 999 if max_depth is None else max_depth
    for _ in range(max_depth):
        if is_system_root(directory):
            break
        directory = directory.parent
        yield directory


def merge_nested_dict(a, b, path=None, conflict='use_b'):
    """Merge dictionary b into a (can be nested)

    Correspondence: https://stackoverflow.com/a/7205107/7057866
    """
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_nested_dict(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass # same leaf value
            else:
                if conflict == 'use_b':
                    a[key] = b[key]
                elif conflict == 'use_a':
                    pass
                else:
                    raise ValueError('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a


def parse_config(project_root, global_conf_paths=['${HOME}/.rmx.config', '${HOME}/.config/rmx']):
    """ Parse rmx config (json file)

    It looks for config file in this order:
    1. {project_root}/.rmx.config
    2. $HOME/.rmx.config
    3. $HOME/.config/rmx
    """
    import pyjson5 as json

    from os.path import expandvars, isfile
    from rmx import logger

    def _maybe_load(path):
        if isfile(path):
            with open(path, 'r') as f:
                return json.load(f)
        return {}

    # TODO: Hmmm... a better way to write this config search algorithm?
    path_found = False
    for path in global_conf_paths:
        path = expandvars(path)
        if isfile(path):
            path_found = True
            break

    if path_found:
        with open(path, 'r') as f:
            global_conf = json.load(f)
    else:
        logger.warn('rmx global config file cannot be located.')
        global_conf = {}

    local_conf = _maybe_load(f'{project_root}/.rmx.config')

    global_conf = remove_recursively(global_conf, key='__help')
    local_conf = remove_recursively(local_conf, key='__help')

    merged_conf = merge_nested_dict(global_conf, local_conf)
    return merged_conf

def remove_recursively(config_dict, key='__help'):
    """remove entry with the specified key recursively."""
    for k in list(config_dict.keys()):
        if k == key:
            del config_dict[k]
            continue
        if isinstance(config_dict[k], dict):
            config_dict[k] = remove_recursively(config_dict[k], key=key)

    return config_dict


def find_project_root():
    """Find a project root (which is rsync-ed with the remote server).

    It first goes up in the directory tree to find ".git" or ".rmx.config" file.
    If not found, print warning and just use the current directory
    """
    from rmx import logger
    def is_proj_root(directory: Path):
        if (directory / '.git').is_dir():
            return True
        if (directory / '.rmx.config').is_file():
            return True
        return False

    current_dir = Path(os.getcwd()).resolve()
    if is_proj_root(current_dir):
        return current_dir

    for directory in yield_parents(current_dir):
        if is_proj_root(directory):
            return directory

    logger.warn('.git directory or .rmx file not found in the ancestor directories.\n'
                'Setting project root to current directory')

    assert not is_system_root(current_dir), "project root detected is the system root '/' you never want to rsync your entire disk."
    assert not is_home_dir(current_dir), "project root detected is home directory. You never want to rsync the entire home directory to a remote machine."
    return current_dir



from datetime import datetime
def get_timestamp() -> str:
    return datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S.%f')

def read_timestamp(time_str: str) -> datetime:
    return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')


def wrap_shebang(command, shell='bash'):
    return f"#!/usr/bin/env {shell}\n{command}"


sacct_cmd = "sacct --starttime $(date -d '40 hours ago' +%D-%R) --endtime now --format JobID,JobName%-100,NodeList,Elapsed,State,ExitCode --parsable2"


def parse_sacct(sacct_output):
    lines = sacct_output.strip().split('\n')
    keys = lines[0].split('|')
    entries = [{key: entry for key, entry in zip(keys, line.split('|'))} for line in lines[1:]]
    # NOTE: For a batch job, sacct shows two entries for one job submission:
    # {'JobID': '8231686', 'JobName': 'rmx-iti-coped-chief-97', ... 'MaxRSS': ''},
    # {'JobID': '8231686.batch', 'JobName': 'batch', ... 'MaxRSS': '64844148K'}
    # We should merge these into one.
    # TEMP: Forget about MaxRSS, and just remove all entries with *.batch
    entries = [entry for entry in entries if not entry['JobID'].endswith('.batch')]
    return entries


def posixpath2str(obj):
    import pathlib
    if isinstance(obj, list):
        return [posixpath2str(e) for e in obj]
    elif isinstance(obj, dict):
        return {key: posixpath2str(val) for key, val in obj.items()}
    elif isinstance(obj, pathlib.Path):
        return str(obj)
    else:
        return obj


def defreeze_dict(frozen_dict: frozenset):
    return {key: val for key, val in frozen_dict}


def replace_rmx_envvars(query: str, rmxenvs: dict):
    """
    Replace RMX_* envvars (i.e., ${RMX_CODE_DIR}, ${RMX_OUTPUT_DIR}, ${RMX_MOUNT_DIR}) with actual path
    Args
      query (string): input string to process
      rmxenvs (dict): map from RMX_* envvars to actual paths
    """
    import re
    rmx_envvars = ['RMX_CODE_DIR', 'RMX_OUTPUT_DIR', 'RMX_MOUNT_DIR']
    for original in rmx_envvars:
        # Match "${original}" or "$original"
        regex = r'{}'.format('(\$\{' + original + '\}' + f'|\$' + original + ')')
        target = rmxenvs[original]
        query = re.sub(regex, str(target), str(query))
    return query


# def replace_rmx_envvars(env: dict):
#     import re
#     # Replace RMX_* envvars: (${RMX_CODE_DIR}, ${RMX_OUTPUT_DIR}, ${RMX_MOUNT_DIR})
#     # This cannot happen automatically on remote server side, since we set these envvars exactly at the same time as other envvars.
#     rmx_envvars = ['RMX_CODE_DIR', 'RMX_OUTPUT_DIR', 'RMX_MOUNT_DIR']
#     for original in rmx_envvars:
#         # Match "${original}" or "$original"
#         regex = r'{}'.format('(\$\{' + original + '\}' + f'|\$' + original + ')')
#         target = str(env[original])
#         env = {key: re.sub(regex, target, str(val)) for key, val in env.items()}
#     return env


from os.path import expandvars
class LaunchLogManager:
    def __init__(self, path=expandvars('$HOME/.rmx/launched.jsonl')) -> None:
        self.path = path

    def _refresh(self):
        import pyjson5 as json
        from datetime import datetime
        # Read entries from the top (oldest) to bottom (newest)
        with open(self.path, 'r') as f:
            entries = f.read().strip().split('\n')
        cutoff_idx = -1
        for idx, entry in enumerate(entries):
            timestamp = json.loads(entry).get('timestamp')
            dt = (datetime.now() - read_timestamp(timestamp)).seconds
            if dt <= 60 * 60 * 30:
                cutoff_idx = idx
                break
        refreshed_entries = entries[cutoff_idx:]

        # Write in jsonl format
        # NOTE: Make sure to add \n in the end!!
        with open(self.path, 'w') as f:
            f.write('\n'.join(refreshed_entries) + '\n')

    def log(self, entry):
        import json
        # Prepare jsonification
        entry = posixpath2str(entry)
        with open(self.path, 'a') as f:
            f.write(json.dumps(entry) + '\n')

    def read(self):
        pass
