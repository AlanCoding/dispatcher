from abc import abstractmethod


class BaseBroker:

    @abstractmethod
    async def connect(self): ...

    @abstractmethod
    async def aprocess_notify(self, connected_callback=None): ...

    @abstractmethod
    async def apublish_message(self, channel, payload=None) -> None: ...

    @abstractmethod
    async def aclose(self) -> None: ...

    @abstractmethod
    def get_connection(self): ...

    @abstractmethod
    def publish_message(self, queue, message): ...

    @abstractmethod
    def close(self): ...
