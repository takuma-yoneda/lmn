#!/usr/bin/env python3

global_config_paths = ['${HOME}/.config/lmn.json5']
global_config_paths += ['${HOME}/.config/rmx']  # Backward compat

local_config_fnames = ['.lmn.json5']
local_config_fnames += ['.rmx.config']  # Backward compat
