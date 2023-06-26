from __future__ import annotations

from typing import Optional, TypeAlias
from dataclasses import dataclass, field

import rpyc # type: ignore

import threading
import sys

from interface import UserId, Topic, Content, FnNotify, BrokerService

PORT = 5000

GLOBAL_LOG: bool = True

def log(s: str) -> None:
    if GLOBAL_LOG:
        print(s, file=sys.stderr)

def log_contents(
        contents: list[Content],
        ) -> None:
    if not GLOBAL_LOG:
        return
    for i, content in enumerate(contents):
        log(f"#{i}: {content}")

IndexNext: TypeAlias = int

@dataclass(slots=True)
class SubsState:
    callback: Optional[FnNotify] = None
    subscribed: dict[Topic, IndexNext] = field(default_factory=dict)

Subscribers: TypeAlias = dict[UserId, SubsState]

BufferType: TypeAlias = list
Buffer = list

class ME(BrokerService):
    all_topics: list[Topic] = list()
    all_contents: BufferType[Content] = Buffer()
    all_subs: Subscribers = dict()
    logged_in: dict[rpyc.Connection, UserId] = dict()

    def create_topic(self, id: UserId, topic: Topic) -> Topic:
        if topic not in self.all_topics:
            self.all_topics.append(topic)
        return topic

    def on_connect(self, conn: rpyc.Connection) -> None:
        log('on_connect: Someone connected')
        log(f"> conn: {conn}")
        self._conn = conn

    def on_disconnect(self, conn: rpyc.Connection) -> None:
        log('on_disconnect: Someone disconnected')
        log(f"> conn: {self._conn}")
        assert self._conn == conn
        if self._conn in ME.logged_in.keys():
            ME.logged_in.pop(self._conn)
        else:
            # Nothing to do
            pass

    def exposed_login(self, id: UserId, callback: FnNotify) -> bool:
        log(f"login: '{id}'")
        log(f"> conn: {self._conn}")
        if id in ME.logged_in.values():
            log(f"> client already connected: '{id}'")
            return False
        else:
            ME.logged_in[self._conn] = id
            if id not in ME.all_subs.keys():
                log(f"> new client: '{id}'")
                ME.all_subs[id] = SubsState(
                    callback = callback
                )
            else:
                assert ME.all_subs[id].callback is None
                ME.all_subs[id].callback = callback
                self.notify_conn()
            return True

    def exposed_list_topics(self) -> list[Topic]:
        self.notify_conn()
        return ME.all_topics

    def exposed_publish(self, author: UserId, topic: Topic, data: str) -> bool:
        log(f"publish: '{author}' '{topic}' '{data}'")
        log(f"> conn: {self._conn}")
        if topic not in ME.all_topics:
            log(f"> Topic not valid '{topic}' by '{author}'")
            self.notify_conn()
            return False
        else:
            content: Content = Content(
                author = author,
                topic = topic,
                data = data,
            )
            ME.all_contents.append(content)
            log(f"> Publishing {content}")
            log_contents(ME.all_contents)
            self.notify_conn()
            return True

    def exposed_subscribe_to(self, id: UserId, topic: Topic) -> bool:
        log(f"subscribe_to: '{id}' '{topic}'")
        log(f"> conn: {self._conn}")
        if topic not in ME.all_topics:
            log(f"> subscribe_to: return False")
            self.notify_conn()
            return False
        else:
            assert id in ME.all_subs.keys()
            subs_state: SubsState = ME.all_subs[id]
            next_index: int = len(ME.all_contents)
            log(f"> subscribe_to: before {subs_state.subscribed}")
            subs_state.subscribed[topic] = next_index
            log(f"> subscribe_to: after {subs_state.subscribed}")
            self.notify_conn()
            return True

    def exposed_unsubscribe_to(self, id: UserId, topic: Topic) -> bool:
        log(f"unsubscribe: '{id}' '{topic}'")
        log(f"> conn: {self._conn}")
        subscribed =  ME.all_subs[id].subscribed
        if topic in subscribed.keys():
            log(f"> unsubscribing to '{topic}'")
            subscribed.pop(topic)
        else:
            log(f"> unsubscribe invalid")
        self.notify_conn()
        return True

    def notify_conn(self) -> None:
        id = ME.logged_in[self._conn]
        subs_state = ME.all_subs[id]
        for topic in subs_state.subscribed.keys():
            ME.notify_one(subs_state, topic)

    @staticmethod
    def notify_one(subs_state: SubsState, topic: Topic) -> None:
        log(f"notify one: {subs_state.subscribed} '{topic}'")
        if subs_state.callback is None:
            # Nothing to do
            pass
        else:
            if topic in subs_state.subscribed.keys():
                list_to_send: list[Content] = []
                index = subs_state.subscribed[topic]
                slice = ME.all_contents[index:]
                assert isinstance(slice, list)
                for content in slice:
                    if content.topic == topic:
                        log(f"> append: {content}")
                        list_to_send.append(content)
                if len(list_to_send) > 0:
                    subs_state.callback(list_to_send)
                    subs_state.subscribed[topic] = len(ME.all_contents)

def main() -> int:
    from rpyc.utils.server import ThreadedServer # type: ignore
    ME.all_topics = [
        'monitoria',
        'ic',
        'estagio',
        'extensao',
        'hashi',
    ]
    srv = ThreadedServer(
        ME,
        port = PORT
    )
    server_thread: threading.Thread = \
        threading.Thread(
            target=srv.start,
        )
    server_thread.start()
    server_thread.join()
    return 0

if __name__ == "__main__":
    retcode: int = main()
    sys.exit(retcode)
