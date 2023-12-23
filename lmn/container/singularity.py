from pydantic import BaseModel, Field
from typing import Optional, List, Union
from lmn import logger


class SingularityConfig(BaseModel):
    sif_file: str
    env: dict = Field(alias="environment", default={})
    env_from_host: List[str] = []
    mount_from_host: dict = {}
    overlay: Optional[str] = None
    startup: Union[str, List[str], None] = None
    pwd: Optional[str] = None

    runtime: str = "singularity"
    writable_tmpfs: bool = True
    nv: bool = True
    containall: bool = True

    # HACK: I don't know how to appropriately set alias with Pydantic...
    @property
    def image(self):
        print("Image property is deprecated. Use sif_file instead.")
        return self.sif_file


class SingularityCommand:
    @staticmethod
    def _valid_key(key: str) -> str:
        '''Long arguments (for slurmCommand) constructed with '-' have been internally
        represented with '_' (for Python). Correct for this in the output.
        '''
        return key.replace('_', '-')

    @staticmethod
    def run(cmd: str, singularity_config: SingularityConfig) -> str:
        c = singularity_config
        options = SingularityCommand.make_options(c)

        if c.startup:
            if isinstance(c.startup, list):
                startup = " ; ".join(c.startup)
            cmd = f"{startup} ; {cmd}"
        # Escape special chars and quotes (https://stackoverflow.com/a/18886646/19913466)
        logger.debug(f"cmd before escape: {cmd}")
        cmd = cmd.encode("unicode-escape").replace(b'"', b'\\"').decode("utf-8")
        logger.debug(f"cmd after escape: {cmd}")

        # NOTE: Without --containall, nvidia-smi command fails with "couldn't find libnvidia-ml.so library in your system."
        # NOTE: Without bash -c '{cmd}', if you put PYTHONPATH=/foo/bar, it fails with no such file or directory 'PYTHONPATH=/foo/bar'
        # TODO: Will the envvars be taken over to the internal shell (by this extra bash command)?
        return f'{c.runtime} run {" ".join(options)} {c.sif_file} bash -c -- "{cmd}"'

    @staticmethod
    def make_options(singularity_config: SingularityConfig) -> List[str]:
        c = singularity_config
        _valid_key = SingularityCommand._valid_key

        # Handle binary flags
        binary_flags = ["nv", "containall", "writable_tmpfs"]
        options = [f"--{_valid_key(flag)}" for flag in binary_flags if getattr(c, flag, False)]

        # Handle arg-val patterns (e.g., `--overlay /blah/blah.fs`)
        arg_vals = ["overlay", "pwd"]
        options += [
            f"--{_valid_key(arg)} {getattr(c, arg)}" for arg in arg_vals if getattr(c, arg, False)
        ]

        # Handle environment variables
        # TODO: Better to use --env-file option and read from a file
        # Escaping quotes and commas will be much easier in that way.
        if c.env:
            options += [
                "--env " + ",".join(f'{key}="{val}"' for key, val in c.env.items())
            ]

        if c.env_from_host:
            options += [
                "--env " + ",".join(f'{envvar}=${envvar}' for envvar in c.env_from_host)
            ]

        # Handle binds:
        bind = "-B {source}:{target}"
        options += [
            bind.format(source=source, target=target)
            for source, target in c.mount_from_host.items()
        ]

        return options


if __name__ == "__main__":
    sing_conf = SingularityConfig(
        sif_file="hoge.sif",
        overlay="foo/bar.fs",
        nv=True,
        containall=True,
        environment={"FOO": "BAR", "HOGE": "PIYO"},
        mount_from_host={"/foo/bar": "/foo/bar"},
    )
    sing_command = SingularityCommand.run("test-command", sing_conf)
    print(sing_command)
