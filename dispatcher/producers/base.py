import asyncio


class ProducerEvents:
    def __init__(self):
        self.ready_event = asyncio.Event()


class BaseProducer:

    async def start_producing(self, dispatcher) -> None: ...

    async def shutdown(self): ...
