from __future__ import annotations

from typing import Generic, Optional, Tuple, TypeAlias, TypeVar, Union
from dataclasses import dataclass, field

import rpyc # type: ignore

import threading
import time
import sys

from interface import UserId, Topic, Content, FnNotify, BrokerService

PORT = 5000

GLOBAL_LOG: bool = True

def log(s: str) -> None:
    if GLOBAL_LOG:
        print(s, file=sys.stderr)

def log_contents(
        contents: Union[list[Content], LimitedBuffer[Content]],
        ) -> None:
    if not GLOBAL_LOG:
        return
    if isinstance(contents, list):
        for i, content in enumerate(contents):
            log(f"#{i}: {content}")
    elif isinstance(contents, LimitedBuffer):
        for slot in contents.list_start(0):
            log(f"#{slot.id}: {slot.item}")
    else:
        assert False, "unreachable"

IndexNext: TypeAlias = int

@dataclass(slots=True)
class SubsState:
    callback: Optional[FnNotify] = None
    subscribed: dict[Topic, IndexNext] = field(default_factory=dict)

Subscribers: TypeAlias = dict[UserId, SubsState]

Ip: TypeAlias = int

T = TypeVar('T')
U = TypeVar('U')

@dataclass(kw_only=True, slots=True)
class LimitedBuffer(Generic[T]):
    MAX_LEN = 3

    @dataclass(frozen=True, kw_only=True, slots=True)
    class Item(Generic[U]):
        id: int
        item: U

    buf: list[Item[T]] = field(default_factory=list)
    next_id: int = 0
    last_idx: int = 0

    def convert_index(self, i: int) -> int:
        assert 0 <= i
        last_item: LimitedBuffer.Item[T] = self.buf[self.last_idx]
        assert last_item.id <= i and i < self.next_id
        last_id: int = self.buf[self.last_idx].id
        idx: int = (i - last_id + self.last_idx) % LimitedBuffer.MAX_LEN
        return idx

    def __getitem__(self, i: Union[int, slice]) -> Union[None, T, list[T]]:
        if isinstance(i, int):
            assert 0 <= i
            last_item: LimitedBuffer.Item[T] = self.buf[self.last_idx]
            if last_item.id <= i and i < self.next_id:
                idx: int = self.convert_index(i)
                return self.buf[idx].item
            else:
                return None
        elif isinstance(i, slice):
            assert i.stop is None
            assert i.step is None
            start: int = i.start or 0
            return [
                x.item
                for x in self.list_start(start)
            ]
        else:
            assert False, f"Unhandled type {type(i)}"

    def __len__(self) -> int:
        return self.next_id

    def append(self, object: T) -> None:
        item: LimitedBuffer.Item[T] = LimitedBuffer.Item(
            id = self.next_id,
            item = object,
        )
        if LimitedBuffer.MAX_LEN <= self.next_id:
            self.buf[self.last_idx] = item
            self.last_idx = (self.last_idx + 1) % LimitedBuffer.MAX_LEN
        else:
            self.buf.append(item)
        self.next_id += 1

    def list_start(self, start: int) -> list[LimitedBuffer.Item[T]]:
        assert 0 <= start
        last_item: LimitedBuffer.Item[T] = self.buf[self.last_idx]
        start_small: Optional[int] = None
        if start < last_item.id:
            start_small = last_item.id
        elif last_item.id <= start and start < self.next_id:
            start_small = self.convert_index(start)
        else:
            start_small = LimitedBuffer.MAX_LEN
        assert start_small is not None
        list_small: list[LimitedBuffer.Item[T]] = self.buf[start_small:]
        list_big: list[LimitedBuffer.Item[T]] = self.buf[0:self.last_idx]
        return list_small + list_big

# BufferType: TypeAlias = LimitedBuffer
# Buffer = LimitedBuffer
BufferType: TypeAlias = list
Buffer = list

from typing import Any
def magia(*a: Any) -> Any:
    assert False, "Magia should not be called"

class ME(BrokerService):
    all_topics: list[Topic] = list()
    all_contents: BufferType[Content] = Buffer()
    all_subs: Subscribers = dict()
    logged_in: dict[rpyc.Connection, UserId] = dict()
    notify_queue: list[Topic] = list()

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
                user_callback = ME.all_subs[id].callback
                if user_callback is None:
                    log(f"> '{id}' already logged out")
                else:
                    if ME.test_callback(user_callback):
                        log(f"> '{id}' is connected")
                        return False
                    else:
                        log(f"> '{id}' just disconnected")
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
            # ME.notify_all(content.topic)
            # ME.notify_one(ME.all_subs[author], content.topic)
            # self.notify_queue.append(content.topic)
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

    @staticmethod
    def notify_all(topic: Topic) -> None:
        log(f"notify all: '{topic}'")
        for subs_state in ME.all_subs.values():
            ME.notify_one(subs_state, topic)

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
                    if ME.test_callback_with(subs_state.callback, list_to_send):
                        subs_state.subscribed[topic] = len(ME.all_contents)
                    else:
                        log(f"> someone was disconnected")
                        subs_state.callback = None

    @staticmethod
    def test_callback(callback: FnNotify) -> bool:
        return ME.test_callback_with(callback, [])

    @staticmethod
    def test_callback_with(callback: FnNotify, list_send: list[Content]) -> bool:
        log('test_callback')
        ok: bool = False
        try:
            log('> test_callback: try')
            callback(list_send)
            log(f"> test_callback: callback({list_send})")
            ok = True
        except Exception as e:
            log(f"> test_callback: except {e}")
        return ok

def notify() -> None:
    while True:
        while len(ME.notify_queue) > 0:
            topic: Topic = ME.notify_queue.pop(0)
            log(f"thread notify '{topic}'")
            ME.notify_all(topic)
            log(f"thread notify: is sleeping")
        time.sleep(1)

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
    notify_thread: threading.Thread = \
        threading.Thread(
            target=notify,
            args=(),
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

# hashi: mirror
