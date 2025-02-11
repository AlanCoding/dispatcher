from abc import abstractmethod
from typing import Any, Iterable, Optional


class BaseBroker:
    _config = None

    def __init__(
        self,
        config: Optional[dict] = None,
        async_connection_factory: Optional[str] = None,
        sync_connection_factory: Optional[str] = None,
        channels: Iterable[str] = ('dispatcher_default',),
    ) -> None:
        if not config or async_connection_factory:
            raise RuntimeError('Must specify either config or async_connection_factory')

        self._config = config
        self._async_connection_factory = async_connection_factory
        self._sync_connection_factory = sync_connection_factory
        self._connection: Optional[Any] = None
        self.channels = channels

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
