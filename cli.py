from typing import NamedTuple, Optional, Tuple, TextIO, TYPE_CHECKING
from dataclasses import dataclass, field

import socket

import sys
IS_NEW_PYTHON: bool = sys.version_info >= (3, 8)
if IS_NEW_PYTHON:
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias


if IS_NEW_PYTHON:
    Command: TypeAlias = Tuple[str, list[str]]
    CommandList: TypeAlias = list[Command]
    ArgList: TypeAlias = list[str]
elif not TYPE_CHECKING:
    Command: TypeAlias = Tuple
    CommandList: TypeAlias = list
    ArgList: TypeAlias = list

if IS_NEW_PYTHON:
    ParsedCommand = NamedTuple('ParsedCommand', [
        ('cmd_name', str),
        ('args', list[str]),
    ])
elif not TYPE_CHECKING:
    ParsedCommand = NamedTuple('ParsedCommand', [
        ('cmd_name', str),
        ('args', list),
    ])

class ClientCli:
    CMDS: CommandList = [
        ('publish', ['topic', 'data']),
        ('subscribe', ['topic']),
        ('unsubscribe', ['topic']),
        ('topics', []),
        ('refresh', []),
        ('exit', []),
        ('help', []),
    ]

    @staticmethod
    def help(file: TextIO) -> None:
        Private.help(ClientCli.CMDS, file)

    @staticmethod
    def command(
            input: TextIO,
            file: TextIO,
            ) -> Optional[ParsedCommand]:
        cmd_i = Private.command(ClientCli.CMDS, input, file)
        if cmd_i is None:
            ClientCli.help(file)
            return None
        else:
            args: list[str] = \
                Private.args(ClientCli.CMDS[cmd_i], input, file)
            return ParsedCommand(ClientCli.CMDS[cmd_i][0], args)

class Private:
    @staticmethod
    def trim_left(s: str) -> str:
        for i, c in enumerate(s):
            if not c.isspace():
                return s[i:]
        return ''

    @staticmethod
    def trim_right(s: str) -> str:
        for i, c in reversed(list(enumerate(s))):
            if not c.isspace():
                return s[:i+1]
        return ''

    @staticmethod
    def trim(s: str) -> str:
        return Private.trim_right(Private.trim_left(s))

    @staticmethod
    def help(cmds: CommandList, file: TextIO) -> None:
        if len(cmds) > 0:
            print(
                'Available commands:',
                file=file,
            )
            for cmd in cmds:
                end: str = ':' if len(cmd[1]) > 0 else ''
                print(
                    f'    {cmd[0]}{end}',
                    file=file, end=''
                )
                for arg in cmd[1]:
                    print(
                        f' <{arg}>',
                        file=file, end=''
                    )
                print(file=file, end='\n')
            print(
                '',
                'Example:',
                f'    {cmds[0][0]}',
                file=file, sep='\n'
            )
            for arg in cmds[0][1]:
                print(
                    f'    {cmds[0][0]}: {arg}> my {arg}',
                    file=file
                )
            print(file=file)
        else:
            print(
                'No commands',
                file=file
            )

    @staticmethod
    def command(
                cmds: CommandList,
                input: TextIO,
                file: TextIO,
                ) -> Optional[int]:
        cmd_str: str = input.readline()
        cmd_str_trimed: str = Private.trim(cmd_str)
        for i, cmd in enumerate(cmds):
            if cmd_str_trimed == cmd[0]:
                return i
        else:
            if len(cmd_str_trimed) > 0:
                print(
                    f"Command not understood: '{cmd_str_trimed}'",
                    file=file
                )
            return None

    @staticmethod
    def args(full_cmd: Command, input: TextIO, file: TextIO) -> ArgList:
        cmd: str = full_cmd[0]
        cmd_args: ArgList = full_cmd[1]
        ret_list: ArgList = []
        for cmd_arg in cmd_args:
            loop: bool = True
            while loop:
                print(f"{cmd}: {cmd_arg}> ", file=file, end='')
                file.flush()
                arg: str = input.readline()
                arg_trimed: str = Private.trim(arg)
                if len(arg_trimed) > 0:
                    ret_list.append(arg_trimed)
                    loop = False
        return ret_list
