#!/usr/bin/env python3
import os
from pathlib import Path
from typing import Iterator

def is_system_root(directory: Path):
    return directory == directory.parent

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


def parse_config(project_root):
    """ Parse tel config (json file)

    It looks for config file in this order:
    1. incrementally goes up in the directory tree (up to TEL_MAX_DEPTH) and find .tel
    2. $HOME/.tel.config
    3. $HOME/.config/tel
    """
    import json
    from os.path import expandvars, isfile

    def _maybe_load(path):
        if isfile(path):
            with open(path, 'r') as f:
                return json.load(f)
        return {}

    # TODO: Hmmm... a better way to write this config search algorithm?
    global_conf_paths = ['${HOME}/.tel.config', '${HOME}/.config/tel']
    for path in global_conf_paths:
        path = expandvars(path)
        if isfile(path):
            break
    with open(path, 'r') as f:
        global_conf = json.load(f)

    local_conf = _maybe_load(f'{project_root}/.tel.config')

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

    It first goes up in the directory tree to find ".git" or ".tel" file.
    If not found, print warning and just use the current directory
    """
    def is_proj_root(directory: Path):
        if (directory / '.git').is_dir():
            return True
        if (directory / '.tel').is_file():
            return True
        return False

    current_dir = Path(os.getcwd()).resolve()
    if is_proj_root(current_dir):
        return current_dir

    for directory in yield_parents(current_dir):
        if is_proj_root(directory):
            return directory

    print('.git directory or .tel file not found in the ancestor directories.\n'
          'Setting project root to current directory')

    assert not is_system_root(current_dir), "project root detected is the system root '/' you never want to rsync your entire disk."
    return current_dir
