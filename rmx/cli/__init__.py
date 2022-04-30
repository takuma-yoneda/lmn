#!/usr/bin/env python3
import argparse
import logging
import os
from abc import abstractmethod, ABC
from typing import Optional, Type

from rmx.types import Arguments


class AbstractCLICommand(ABC):

    KEY = None

    @classmethod
    def name(cls) -> str:
        return cls.KEY

    @staticmethod
    def common_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(add_help=False)
        # parser.add_argument(
        #     "-C",
        #     "--workdir",
        #     default=os.getcwd(),
        #     help="Directory containing the CPK project"
        # )
        parser.add_argument(
            "--image",
            default=None,
            help="specify docker image"
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="dry run"
        )
        parser.add_argument(
            "-o",
            "--outdir",
            default=None,
            help="output directory where generated files will be stored."
        )
        parser.add_argument(
            "--verbose",
            default=False,
            action="store_true",
            help="Be verbose"
        )
        parser.add_argument(
            "--debug",
            default=False,
            action="store_true",
            help="Enable debug mode"
        )
        return parser

    @classmethod
    def get_parser(cls, args: Arguments) -> argparse.ArgumentParser:
        common_parser = cls.common_parser()
        command_parser = cls.parser(common_parser, args)
        command_parser.prog = f'rmx {cls.KEY}'
        return command_parser

    @staticmethod
    @abstractmethod
    def parser(parent: Optional[argparse.ArgumentParser] = None,
               args: Optional[Arguments] = None) -> argparse.ArgumentParser:
        pass

    @staticmethod
    @abstractmethod
    def execute(config: dict, parsed: argparse.Namespace, relative_workdir: str) -> bool:
        pass


class RMXCLI:

    @staticmethod
    def parse_arguments(command: Type[AbstractCLICommand], args: Arguments) -> argparse.Namespace:
        parser = command.get_parser(args)
        parsed = parser.parse_args(args)
        # sanitize workdir
        parsed.workdir = os.path.abspath(parsed.workdir)
        # enable debug
        # if parsed.debug:
        #     cpklogger.setLevel(logging.DEBUG)
        # ---
        return parsed


__all__ = [
    "AbstractCLICommand",
    "RMXCLI",
    # "cpklogger"
]
