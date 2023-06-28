from __future__ import annotations

# Se não funcionar no lab rode:
# $ pip install --user typing_extensions
import sys
IS_NEW_PYTHON: bool = sys.version_info >= (3, 8)
if IS_NEW_PYTHON:
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

from typing import Callable, TYPE_CHECKING
from dataclasses import dataclass

import rpyc # type: ignore

UserId: TypeAlias = str

Topic: TypeAlias = str

# Isso é para ser tipo uma struct
# Frozen diz que os campos são read-only
if IS_NEW_PYTHON:
    @dataclass(frozen=True, kw_only=True, slots=True)
    class Content:
        author: UserId
        topic: Topic
        data: str
elif not TYPE_CHECKING:
    @dataclass(frozen=True)
    class Content:
        author: UserId
        topic: Topic
        data: str

if IS_NEW_PYTHON:
    FnNotify: TypeAlias = Callable[[list[Content]], None]
elif not TYPE_CHECKING:
    FnNotify: TypeAlias = Callable

class BrokerService(rpyc.Service): # type: ignore

    # Não é exposed porque só o "admin" tem acesso
    def create_topic(self, id: UserId, topicname: str) -> Topic:
        assert False, "TO BE IMPLEMENTED"

    # Handshake

    def exposed_login(self, id: UserId, callback: FnNotify) -> bool:
        assert False, "TO BE IMPLEMENTED"

    # Query operations

    def exposed_list_topics(self) -> list[Topic]:
        assert False, "TO BE IMPLEMENTED"

    # Publisher operations

    def exposed_publish(self, id: UserId, topic: Topic, data: str) -> bool:
        """
        Função responde se Anúncio conseguiu ser publicado
        """
        assert False, "TO BE IMPLEMENTED"

    # Subscriber operations

    def exposed_subscribe_to(self, id: UserId, topic: Topic) -> bool:
        """
        Função responde se `id` está inscrito no `topic`
        """
        assert False, "TO BE IMPLEMENTED"

    def exposed_unsubscribe_to(self, id: UserId, topic: Topic) -> bool:
        """
        Função responde se `id` não está inscrito no `topic`
        """
        assert False, "TO BE IMPLEMENTED"
