# Example Project
## Local config file
`.lmd.config` file in this directory looks like this:

``` json
{
    "project": {
        "name": "awesome-project",
        "environment": {
            "YOUR_ENV_VAR": "whatever value"
        },
        "mount": [
            "${HOME}/.secret/some-credential"
        ]
    },
    "machines": {
        "tticslurm": {
            "environment": {
                "YOUR_ENV_VAR": "custom envvar specific to a machine"
            }
        }
    }
}

```

## Execution

When you run `lmd run <remote-machine> <command-to-execute>`, LMD will
1. Copy the contents of `your-awesome-project` to `<remote-machine>:/tmp/your-awesome-project/code/`
2. Copy the credential file `some-credential` to `<remote-machine>:/tmp/your-awesome-project/mount/`
4. Create `<remote-machine>:/tmp/your-awesome-project/output` directory to store output
3. Set envvar `YOUR_ENV_VAR` to `whatever value` in the remote shell
5. Execute command in the remote shell

**In action**

```console
you@local$ lmd run birch --mode ssh --verbose 'hostname && pwd && env'
lmd - Remote code execution for ML researchers - v0.0.1
birch
/tmp/project/code
YOUR_ENV_VAR=whatever value
LMD_CODE_DIR=/tmp/project/code
LMD_MOUNT_DIR=/tmp/project/mount
LMD_OUTPUT_DIR=/tmp/project/output
...
```


