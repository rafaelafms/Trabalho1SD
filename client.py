from typing import Optional, TextIO, Any
from dataclasses import dataclass, field

import rpyc # type: ignore

import sys

from interface import UserId, Content
from cli import ClientCli, ParsedCommand

HOST = 'localhost'
PORT = 5000

def log(s: str) -> None:
    print(s, file=sys.stderr)

@dataclass(eq=False, kw_only=True, slots=True)
class ConnMan:
    host: str
    port: int
    conn: rpyc.Connection = field(init=False)

    def __enter__(self) -> rpyc.Connection:
        self.conn = rpyc.connect(self.host, self.port)
        log(f"connected to service: {'conn.root.get_service_name()'}")
        return self.conn

    def __exit__(self, *args: Any) -> None:
        self.conn.close()

def create_conn(
        host: str,
        port: int,
        ) -> ConnMan:
    return ConnMan(host=host, port=port)

def handle_command(
        id: UserId,
        parsed: ParsedCommand,
        conn: rpyc.Connection,
        output: TextIO,
        ) -> bool:
    if parsed.cmd_name == 'publish':
        assert len(parsed.args) == 2
        ok = \
            conn.root.publish(id, parsed.args[0], parsed.args[1])
        if ok:
            print('=> Ok!',
                file=output)
        else:
            print('=> Not ok!',
                file=output)
    elif parsed.cmd_name == 'subscribe':
        assert len(parsed.args) == 1
        ok = \
            conn.root.subscribe_to(id, parsed.args[0])
        if ok:
            print('=> Ok!',
                file=output)
        else:
            print('=> Not ok!',
                file=output)
    elif parsed.cmd_name == 'unsubscribe':
        assert len(parsed.args) == 1
        ok = \
            conn.root.unsubscribe(id, parsed.args[0])
        if ok:
            print('=> Ok!',
                file=output)
        else:
            print('=> Not ok!',
                file=output)
    elif parsed.cmd_name == 'topics':
        assert len(parsed.args) == 0
        topics = \
            conn.root.list_topics()
        print(f'=> topics: {topics}',
            file=output)
    elif parsed.cmd_name == 'exit':
        return True
    elif parsed.cmd_name == 'help':
        ClientCli.help(output)
    else:
        log(f"cmd: '{parsed.cmd_name}'")
        for i, arg in enumerate(parsed.args):
            log(f"arg {i}: '{arg}'")
        assert False, f"Unhandled command: '{parsed.cmd_name}'"
    return False

mail: list[Content] = []

def callback(contents: list[Content]) -> None:
    # print('callback:', contents)
    global mail
    for content in contents:
        mail.append(content)

def main() -> int:
    infile = sys.stdin
    outfile = sys.stdout
    with create_conn(HOST, PORT) as conn:
        should_stop: bool = False
        id: UserId = 'client'
        loginok: bool = \
            conn.root.login(id, callback)
        if loginok:
            print('=> Login ok!',
                file=outfile)
        else:
            print('=> Login not ok!',
                file=outfile)
            return 1
        ClientCli.help(outfile)
        while not should_stop:
            parsed: Optional[ParsedCommand] = \
                ClientCli.command(infile, outfile)
            if parsed is None:
                # Nothing to do
                pass
            else:
                should_stop = handle_command(id, parsed, conn, outfile)
            while len(mail) > 0:
                log(f"len(mail) = {len(mail)}")
                content: Content = mail.pop(0)
                print(f"New message: {content}",
                    file=outfile)
    return 0

if __name__ == "__main__":
    sys.exit(main())
