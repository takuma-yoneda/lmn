# Configurations

## Global configuration
lmn looks for a configuration file in this order:
1. `{project_root}/.lmn.config`
2. `$HOME/.lmn.config`
3. `$HOME/.config/lmn`

NOTE: project root is determined by checking if a directory contains `.git` or `.lmn.config`. If those are not found in the current directory, lmn recursively traverses its parents to search for it.
And if they are now found in any ancestor directories, lmn assumes the current directory to be the project root.

## Local configuration
lmn looks for `{project_root}/.lmn.config`.  
Local and global configurations are merged. Local configuration has a priority.

