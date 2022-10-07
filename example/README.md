# Configurations

## Global configuration
rmx looks for a configuration file in this order:
1. `{project_root}/.rmx.config`
2. `$HOME/.rmx.config`
3. `$HOME/.config/rmx`

NOTE: project root is determined by checking if a directory contains `.git` or `.rmx.config`. If those are not found in the current directory, rmx recursively traverses its parents to search for it.
And if they are now found in any ancestor directories, rmx assumes the current directory to be the project root.

## Local configuration
RMX looks for `{project_root}/.rmx.config`.  
Local and global configurations are merged. Local configuration has a priority.

