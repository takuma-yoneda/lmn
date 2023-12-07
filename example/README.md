# Configurations

## Global and local configurations
`lmn` looks for configuration files in this order:
1. `{project_root}/.lmn.json5`
2. `$HOME/.lmn.json5`
3. `$HOME/.config/lmn`

2 or 3 are considered *global* configuration. `lmn` merges local configuration with a global one (the local config has priority for conflicted entries).
It is recommended to store basic configurations (e.g., remote machines) in the global config, and keep the local config light.

## Preset for slurm configurations
You can store presets for slurm job specification in a config file:
```json5
{
    "machines": {
        ...
    },
    "slurm-configs": {
        "very-short": {
            "partition": "contrib-gpu",
            "cpus_per_task": 1,
            "time": "0:10:00",
            "output": "slurm-%j.out.log",
            "error": "slurm-%j.error.log",
        },
        "a6000": {
            "partition": "contrib-gpu",
            "cpus_per_task": 1,
            "time": "4:00:00",
            "constraint": "a6000",
            "output": "slurm-%j.out.log",
            "error": "slurm-%j.error.log",
        },
        "cpu": {
            "partition": "cpu",
            "cpus_per_task": 1,
            "time": "4:00:00",
            "constraint": "avx",
            "output": "slurm-%j.out.log",
            "error": "slurm-%j.error.log",
        },
}
```
When you want to use a preset, you can simply use `--sconf` option
```bash
$ lmn run tticslurm --sconf a6000 -- python train.py
```

## Preset for PBS configurations
Coming Soon...

## Other notes

**Project root**
> Project root is determined by checking if a directory contains `.git` or `.lmn.json5`.  
> If those are not found in the current directory, `lmn` recursively traverses its parents to search for it.

**SSH connection**
> `lmn` establishes a ssh connection with ControlMaster (ControlPath is set to `~/.ssh/lmn-ssh-socket-{hostname}`)
> any following ssh connections reuse the established one.