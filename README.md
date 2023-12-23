<div align="center">

# 🍋 LMN - A minimal launcher
<!-- <a href="https://github.com/takuma-yoneda/lmn/actions/workflows/python-publish-pypi.yml"> -->
<!--     <img src="https://github.com/takuma-yoneda/lmn/actions/workflows/python-publish-pypi.yml/badge.svg" alt="Publish to PyPI" /> -->
<!-- </a> -->

A lightweight tool to run your local project on a remote machine, with a single command.

`lmn` can set up a container environment (Docker or Singularity) and can work with job schedulers (Slurm or PBS).

<!-- `lmn` is a lightweight launcher. `lmn` allows you to seamlessly launch scripts across multiple remote machines. -->
<!-- A lightweight tool to rsync and execute local scripts in a remote machine. -->

<a href="https://github.com/takuma-yoneda/lmn/actions/workflows/python-run-tests.yml">
    <img src="https://github.com/takuma-yoneda/lmn/actions/workflows/python-run-tests.yml/badge.svg" alt="Test" />
</a>
<!-- <a href="https://github.com/takuma-yoneda/lmn">
    <img src="https://tokei.rs/b1/github/takuma-yoneda/lmn" alt="Total lines" />
</a> -->
<a href="https://pypi.org/project/lmn/">
    <img src="https://img.shields.io/pypi/v/lmn?logo=python&logoColor=%23cccccc" alt="PyPI" />
</a>
</div>

# 🧑‍💻 `lmn` in action:
In your project directory, you can simply run
```bash
$ lmn run <remote-machine> -- python generate_fancy_images.py
```
It does:
1. **rsync your local project directory** with the remote machine <!-- (the root of the project dir is identified by the location of `.git` or `.lmn.json5`) -->
2. **ssh into the remote machine**, (optionally) set up the specified env (Docker or Singularity)
3. Inside of the environment, **`lmn` runs `python generate_fancy_images.py`**
4. If files are created (such as images), **`lmn` rsync the generated files back to the local project directory**


# 🚀 Quickstart
### ▶️&nbsp;&nbsp;Installation
You can install `lmn` via pip:
```bash
$ pip install lmn
```
### ▶️&nbsp;&nbsp;Configuration
All you need is to place a single configuration file at `project_dir/.lmn.json5` (on your local machine).  
The config file looks like:
<details>
<summary>Example Configuration</summary>
    
```json5
{
    "project": {
        "name": "my_project",
        // What not to rsync with the remote machine:
        "exclude": [".git", ".venv", "wandb", "__pycache__"],
        // Project-specific environment variables:
        "environment": {
            "MUJOCO_GL": "egl"
        }
    },
    "machines": {
        "elm": {
            // Host information
            "host": "elm.ttic.edu",
            "user": "takuma",
            // Rsync target directory (on the remote host)
            "root_dir": "/scratch/takuma/lmn",
            // Mode: ["ssh", "docker", "slurm", "pbs", "slurm-sing", "pbs-sing"]
            "mode": "docker",
            // Docker configurations
            "docker": {
                "image": "ripl/my_transformer:latest",
                "network": "host",
                // Mount configurations (host -> container)
                "mount_from_host": {
                    "/ripl/user/takuma/project/": "/project",
                    "/dev/shm": "/dev/shm",
                },
            },
            // Host-specific environment variables
            "environment": {
                "PROJECT_DIR": "/project",
            },
        },
        "tticslurm": {
            "host": "slurm.ttic.edu",
            "user": "takuma",
            "mode": "slurm-sing",  // Running a Singularity container on a cluster with Slurm job scheduler
            "root_dir": "/share/data/ripl-takuma/lmn",
            // Slurm job configurations
            "slurm": {
                "partition": "contrib-gpu",
                "cpus_per_task": 1,
                "time": "04:00:00",
                "output": "slurm-%j.out.log",
                "error": "slurm-%j.error.log",
                "exclude": "gpu0,gpu18",
            },
            // Singularity configurations
            "singularity": {
                "sif_file": "/share/data/ripl-takuma/singularity/my_transformer.sif",
                "writable_tmpfs": true,
                "startup": "ldconfig /.singularity.d/libs",  // Command to run after starting up the container
                "mount_from_host": {
                    "/share/data/ripl-takuma/project/": "/project",
                },
            },
            "environment": {
                "PROJECT_DIR": "/project",
            }
        }
    }
}
```
</details>

More example configurations can be found in [the example directory](/example).

### ▶️&nbsp;&nbsp;Command examples
Make sure that you're in the project directory first.
```bash
# Launch an interactive shell in the docker container (on elm):
$ lmn run elm -- bash

# Run a job in the docker container (on elm):
$ lmn run elm -- python train.py

# Run a script on the host (on elm):
$ lmn run elm --mode ssh -- python hello.py

# Run a command quickly on the host without syncing any files ("bare"-run; on elm)
$ lmn brun elm -- hostname

# Check GPU usage on elm (This is equivalent to `lmn brun elm -- nvidia-smi`)
$ lmn nv elm

# Launch an interactive shell in the Singularity container via Slurm scheduler (on tticslurm)
$ lmn run tticslurm -- bash

# Run a job in the Singularity container via Slurm scheduler (on tticslurm)
$ lmn run tticslurm -- python train.py

# Submit a batch job that runs in the Singularity container via Slurm scheduler (on tticslurm)
$ lmn run tticslurm -d -- python train.py

# Launching a sweep (batch jobs) that runs in the Singularity container via Slurm scheduler (on tticslurm)
# This submits 10 batch jobs where `$LMN_RUN_SWEEP_IDX` is set from 0 to 9.
$ lmn run tticslurm --sweep 0-10 -d -- python train.py -l '$LMN_RUN_SWEEP_IDX'

# Run a script on the login node (on tticslurm)
$ lmn run tticslurm --mode ssh -- squeue -u takuma

# Get help
$ lmn --help

# Get help on `lmn run`
$ lmn run --help
```

<details>
<summary>More about `--sweep` format</summary>
    
- `--sweep 0-10`: ten jobs with `LMN_RUN_SWEEP_IDX=0`, `1` through `9`
  - Internally `lmn` simply runs `range(0, 10)`
- `--sweep 7`: a single job with `LMN_RUN_SWEEP_IDX=7`
- `--sweep 3,5,8`:  three jobs with `LMN_RUN_SWEEP_IDX=3` and `5` and `8`
</details>

<!-- # Paramiko fails in ssh-authentication?
- Make sure you can ssh manually
- Make sure to run `ssh-add <your-ssh-key>` even if you can log in manually -->

<!-- # Best practice
## Singularity
- Never install / store anything under the home directory when you build the image
- Singularity mounts host's home directory as default
- Even if you specify `--contain`, it will create an empty home directory...
-->


<!-- ## Project structure
- project-root
  - docker
    - Dockerfile
    - Makefile
  - donottransport
    - whatever large files you don't need on remote side
    
## Slurm
- If you share a directory
  - add `umask 002` in your `~/.bashrc` to allow group write permission as default -->

# Comparison with other packages
`lmn` is inspired by the following great packages: [geyang/jaynes](https://github.com/geyang/jaynes), [justinjfu/doodad](https://github.com/justinjfu/doodad) and [afdaniele/cpk](https://github.com/afdaniele/cpk).
- `jaynes` and `doodad` focus on launching a lot of jobs in non-interactive mode
  - ✅ support ssh, docker, slurm and AWS / GCP (but not PBS scheduler)
  - 😢 do not support Singularity
  - 😢 cannot launch interactive jobs
  - 😢 only work with Python project, and require (although small) modifications to the project codebase
- `cpk` focuses on (though not limited to) ROS application and running programs in docker containers
  - ✅ supports X forwarding and more stuff that are helpful to run ROS applications on the container
  - ✅ provides more functionalities such as creating and deploying ssh keys on remote machines
  - 😢 does not support clusters with schedulers (Slurm or PBS), nor does it support Singularity


# Tasks
- [ ] Use Pydantic for configurations
- [ ] add ssh-release subcommand (to drop the persistent ssh connection)
- [ ] Remove project.name from the global config, and get project name from the project root directory name
