from __future__ import annotations

from typing import Optional, Tuple, TypeAlias
from dataclasses import dataclass, field

import rpyc # type: ignore

import threading
import time
import sys

from interface import UserId, Topic, Content, FnNotify, BrokerService

PORT = 5000

def log(s: str) -> None:
    print(s, file=sys.stderr)

IndexNext: TypeAlias = int

@dataclass(slots=True)
class SubsState:
    callback: Optional[FnNotify] = None
    subscribed: dict[Topic, IndexNext] = field(default_factory=dict)

Subscribers: TypeAlias = dict[UserId, SubsState]

Ip: TypeAlias = int

from typing import Any
def magia(*a: Any) -> Any:
    assert False, "Magia should not be called"

@dataclass(kw_only=True, slots=True)
class ME(BrokerService):
    all_topics: list[Topic] = field(default_factory=list)
    all_contents: list[Content] = field(default_factory=list)
    all_subs: Subscribers = field(default_factory=dict)
    all_loggedin: dict[Ip, UserId] = field(default_factory=dict)
    notify_queue: list[Topic] = field(default_factory=list)

    def create_topic(self, id: UserId, topic: Topic) -> Topic:
        if topic not in self.all_topics:
            self.all_topics.append(topic)
        return topic

    def on_connect(self, conn: rpyc.Connection) -> None:
        log('Someone connected')

    def on_disconnect(self, conn: rpyc.Connection) -> None:
        log('Someone disconnected')
        ip = 3
            # magia(conn)
        if ip in self.all_loggedin.keys():
            id = self.all_loggedin[ip]
            assert id in self.all_subs.keys()
            self.all_subs[id].callback = None
            self.all_loggedin.pop(ip)
        else:
            pass

    def exposed_login(self, id: UserId, callback: FnNotify) -> bool:
        if id in self.all_loggedin.values():
            return False
        else:
            ip = 3
                # magia(self)
            assert ip not in self.all_loggedin.keys()
            self.all_loggedin[ip] = id
            if id not in self.all_subs:
                self.all_subs[id] = SubsState(
                    callback = callback
                )
            else:
                assert self.all_subs[id].callback is None
                self.all_subs[id].callback = callback
                for topic in self.all_topics:
                    self.notify_one(self.all_subs[id], topic)
            return True

    def exposed_list_topics(self) -> list[Topic]:
        return self.all_topics

    def exposed_publish(self, author: UserId, topic: Topic, data: str) -> bool:
        if topic not in self.all_topics:
            return False
        else:
            content: Content = Content(
                author = author,
                topic = topic,
                data = data,
            )
            self.all_contents.append(content)
            self.notify_all(content.topic)
            # self.notify_queue.append(content.topic)
            return True

    def exposed_subscribe_to(self, id: UserId, topic: Topic) -> bool:
        log(f"subscribe_to: '{id}' '{topic}'")
        if topic not in self.all_topics:
            log(f"subscribe_to: return False")
            return False
        else:
            assert id in self.all_subs.keys()
            subs_state: SubsState = self.all_subs[id]
            next_index: int = len(self.all_contents)
            log(f"subscribe_to: before {subs_state.subscribed}")
            subs_state.subscribed[topic] = next_index
            log(f"subscribe_to: after {subs_state.subscribed}")
            return True

    def exposed_unsubscribe(self, id: UserId, topic: Topic) -> bool:
        subscribed =  self.all_subs[id].subscribed
        if topic in subscribed.keys():
            subscribed.pop(topic)
        else:
            pass
        return True

    def notify_all(self, topic: Topic) -> None:
        log(f"notify all: '{topic}'")
        for subs_state in self.all_subs.values():
            self.notify_one(subs_state, topic)

    def notify_one(self, subs_state: SubsState, topic: Topic) -> None:
        log(f"notify one: {subs_state.subscribed} '{topic}'")
        if subs_state.callback is None:
            # Nothing to do
            pass
        else:
            if topic in subs_state.subscribed.keys():
                list_to_send: list[Content] = []
                index = subs_state.subscribed[topic]
                for content in self.all_contents[index:]:
                    if content.topic == topic:
                        list_to_send.append(content)
                subs_state.callback(list_to_send)
                subs_state.subscribed[topic] = len(self.all_contents)

def notify(server_data: ME) -> None:
    queue: list[Topic] = server_data.notify_queue
    while True:
        while len(queue) > 0:
            topic: Topic = queue.pop(0)
            log(f"notify '{topic}'")
            server_data.notify_all(topic)
        log('notify is sleeping')
        time.sleep(1)

def main() -> int:
    from rpyc.utils.server import ThreadedServer # type: ignore
    server_data = ME(
        all_topics = [
            'monitoria',
            'ic',
            'estagio',
            'extensao',
            'hashi',
        ],
    )
    srv = ThreadedServer(
        server_data,
        port = PORT
    )
    server_thread: threading.Thread = \
        threading.Thread(
            target=srv.start,
        )
    notify_thread: threading.Thread = \
        threading.Thread(
            target=notify,
            args=(server_data,),
        )
    server_thread.start()
    # notify_thread.start()
    server_thread.join()
    # notify_thread.join()
    return 0

if __name__ == "__main__":
    retcode: int = main()
    sys.exit(retcode)

# TODO LIST:
# notify "assincrono"
# "select" para o RPyC
# Magia do ip

# all_contents: limit de tamanho
# hashi: mirror
