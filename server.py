

import rpyc # typing: ignore

IndexNext: TypeAlias = int
SubsState: TypeAlias = list[Tuple[Topic, IndexNext, FnNotify]]

Subscribers: TypeAlias = dict[UserId, SubsState]

class ME:
    all_topics: list[Topic]
    all_contents: list[Content]
    all_subs: Subscribers

    def login() -> bool:
        pass

    def create_topic(self, topic: Topic) -> None:
        if topic not in self.all_topics:
            self.all_topics.append(topic)

    def list_topics(self) -> list[Topic]:
        return self.all_topics

    def publish(self, author: UserId, topic: Topic, data: str) -> bool:
        content: Content = Content(
            author = author,
            topic = topic,
            data = data,
        )
        self.all_contents.append(content)
        # avisar os subscribers
        assert False, 'avisar os subscribers'

    def subscribe(self, id: UserId, topic: Topic, callback: FnNotify) -> bool:
        if topic not in self.all_topics:
            return None # TODO: eh True ou False
        else:
            assert id in self.all_subs.keys()
            subs_state: SubsState = self.all_subs[id]
            next_index: int = len(self.all_topics)
            asdf = (topic, next_index, callback)
            if magia(topic in subs_state):
                index: int = magia(topic, subs_state)
                subs_state[index] = asdf
            else:
                subs_state.append(asdf)
        pass

    def unsubscribe() -> bool:
        pass

