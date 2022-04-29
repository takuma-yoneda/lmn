# Configurations

## Global configuration
LMD looks for a configuration file in this order:
1. `{project_root}.lmd.config`
2. `$HOME/.lmd.config`
3. `$HOME/.config/lmd`

NOTE: project root is determined by checking if a directory contains `.git` or `.lmd.config`. If not found in the current directory, LMD recursively traverses its parents to search for it.

## Local configuration
LMD looks for `{project_root}/.lmd.config`.  
local and global configurations are merged.

