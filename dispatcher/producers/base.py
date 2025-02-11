import asyncio
from typing import Optional


class ProducerEvents:
    def __init__(self) -> None:
        self.ready_event = asyncio.Event()


class BaseProducer:
    events: Optional[ProducerEvents] = None

    def _create_events(self) -> ProducerEvents:
        return ProducerEvents()

    async def start_producing(self, dispatcher) -> None: ...

    async def shutdown(self): ...
