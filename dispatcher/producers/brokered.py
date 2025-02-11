import asyncio
import importlib
import logging
from types import ModuleType
from typing import Optional

from dispatcher.brokers.base import BaseBroker
from dispatcher.producers.base import BaseProducer

logger = logging.getLogger(__name__)


def get_broker_module(broker_name) -> ModuleType:
    return importlib.import_module(f'dispatcher.brokers.{broker_name}')


class BrokeredProducer(BaseProducer):
    def __init__(self, broker: BaseBroker, close_on_exit: bool = True) -> None:
        self.events = self._create_events()
        self.production_task: Optional[asyncio.Task] = None
        self.broker = broker
        self.close_on_exit = close_on_exit
        self.dispatcher = None

    @classmethod
    def get_async_broker(cls, broker_name, broker_config) -> BaseBroker:
        broker_module = get_broker_module(broker_name)
        broker_cls = getattr(broker_module, 'AsyncBroker')
        return broker_cls(**broker_config)

    @classmethod
    def get_sync_broker(cls, broker_name, broker_config) -> BaseBroker:
        broker_module = get_broker_module(broker_name)
        broker_cls = getattr(broker_module, 'SyncBroker')
        return broker_cls(**broker_config)

    async def start_producing(self, dispatcher) -> None:
        self.production_task = asyncio.create_task(self.produce_forever(dispatcher), name=f'{self.broker}_production')
        # TODO: implement connection retry logic
        self.production_task.add_done_callback(dispatcher.fatal_error_callback)

    def all_tasks(self) -> list[asyncio.Task]:
        if self.production_task:
            return [self.production_task]
        return []

    async def connected_callback(self) -> None:
        if self.events:
            self.events.ready_event.set()
        if self.dispatcher:
            await self.dispatcher.connected_callback(self)

    async def produce_forever(self, dispatcher) -> None:
        self.dispatcher = dispatcher
        async for channel, payload in self.broker.aprocess_notify(connected_callback=self.connected_callback):
            await dispatcher.process_message(payload, broker=self, channel=channel)

    async def notify(self, channel: str, payload: Optional[str] = None) -> None:
        await self.broker.apublish_message(channel, payload=payload)

    async def shutdown(self) -> None:
        if self.production_task:
            self.production_task.cancel()
            try:
                await self.production_task
            except asyncio.CancelledError:
                logger.info(f'Successfully canceled production from {self.broker}')
            except Exception:
                # traceback logged in fatal callback
                if not hasattr(self.production_task, '_dispatcher_tb_logged'):
                    logger.exception(f'Broker {self.broker} shutdown saw an unexpected exception from production task')
            self.production_task = None
        if self.close_on_exit:
            logger.debug(f'Closing {self.broker} connection')
            await self.broker.aclose()
