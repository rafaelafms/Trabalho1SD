from __future__ import annotations

from typing import Optional, Tuple, TypeAlias
from dataclasses import dataclass, field

import rpyc # type: ignore

from interface import UserId, Topic, Content, FnNotify, BrokerService

IndexNext: TypeAlias = int

@dataclass(slots=True)
class SubsState:
    callback: Optional[FnNotify] = None
    subscribed: dict[Topic, IndexNext] = field(default_factory=dict)

Subscribers: TypeAlias = dict[UserId, SubsState]

from typing import Any
def magia(*a: Any) -> Any:
    assert False, "magia should not be called"

class ME(BrokerService):
    all_topics: list[Topic]
    all_contents: list[Content]
    all_subs: Subscribers

    def create_topic(self, id: UserId, topic: Topic) -> Topic:
        if topic not in self.all_topics:
            self.all_topics.append(topic)
        return topic

    def on_disconnect(self) -> bool:
        assert False, "NOT IMPLEMENTED"

    def exposed_login(self, id: UserId, callback: FnNotify) -> bool:
        assert False, "NOT IMPLEMENTED"

    def exposed_list_topics(self) -> list[Topic]:
        return self.all_topics

    def exposed_publish(self, author: UserId, topic: Topic, data: str) -> bool:
        content: Content = Content(
            author = author,
            topic = topic,
            data = data,
        )
        self.all_contents.append(content)
        # Note: bloqueia até notificar todo mundo
        self.notify_all(content.topic)
        return True

    def exposed_subscribe_to(self, id: UserId, topic: Topic) -> bool:
        if topic not in self.all_topics:
            return False
        else:
            assert id in self.all_subs.keys()
            subs_state: SubsState = self.all_subs[id]
            next_index: int = len(self.all_topics)
            subs_state.subscribed[topic] = next_index
            return True

    def exposed_unsubscribe(self) -> bool:
        assert False, "NOT IMPLEMENTED"

    def notify_all(self, topic: Topic) -> None:
        for subs_state in self.all_subs.values():
            self.notify_one(subs_state, topic)

    def notify_one(self, subs_state: SubsState, topic: Topic) -> None:
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

# TODO LIST:
# notify "assincrono"
# "select" para o RPyC
# interface usuário

# all_contents: limit de tamanho
# hashi: mirror
