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


def parse_config():
    """ Parse tel config (json file)

    It looks for config file in this order:
    1. incrementally goes up in the directory tree (up to TEL_MAX_DEPTH) and find .tel
    2. $HOME/.tel.config
    3. $HOME/.config/tel
    """

    # TODO: Hmmm... a better way to write this config search algorithm?
    import json
    from os.path import expandvars, isfile
    candidate_paths = ['${HOME}/.tel.config', '${HOME}/.config/tel']
    for path in candidate_paths:
        path = expandvars(path)
        if isfile(path):
            break

    with open(path, 'r') as f:
        config = json.load(f)

    config = remove_recursively(config, key='__help')
    return config

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
