# Configurations

## Global configuration
lmn looks for a configuration file in this order:
1. `{project_root}/.lmn.json5`
2. `$HOME/.lmn.json5`
3. `$HOME/.config/lmn`

2 and 3 are considered *global* configuration. The local configuration has priority to the global one.

> Project root is determined by checking if a directory contains `.git` or `.lmn.json5`.  
> If those are not found in the current directory, `lmn` recursively traverses its parents to search for it.
