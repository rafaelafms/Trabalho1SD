from __future__ import annotations

from typing import Callable, TypeAlias
from dataclasses import dataclass

import rpyc # type: ignore

UserId: TypeAlias = str

Topic: TypeAlias = str

# Isso é para ser tipo uma struct
# Frozen diz que os campos são read-only
@dataclass(frozen=True, kw_only=True, slots=True)
class Content:
    author: UserId
    topic: Topic
    data: str

FnNotify: TypeAlias = Callable[[list[Content]], None]

class BrokerService(rpyc.Service): # type: ignore

    # Não é exposed porque só o "admin" tem acesso
    def create_topic(self, id: UserId, topicname: str) -> Topic:
        assert False, "TO BE IMPLEMENTED"

    # Handshake

    def exposed_login(self, id: UserId) -> Bool:
        assert False, "TO BE IMPLEMENTED"

    # Query operations

    def exposed_list_topics(self) -> list[Topic]:
        assert False, "TO BE IMPLEMENTED"

    # Publisher operations

    def exposed_publish(self, id: UserId, topic: Topic, data: str) -> Bool:
        assert False, "TO BE IMPLEMENTED"

    # Subscriber operations

    def exposed_subscribe_to(self, id: UserId, topic: Topic, callback: FnNotify) -> Bool:
        assert False, "TO BE IMPLEMENTED"

    def exposed_unsubscribe_to(self, id: UserId, topic: Topic) -> Bool:
        assert False, "TO BE IMPLEMENTED"
