import importlib
from types import ModuleType

from dispatcher.brokers.base import BaseBroker


def get_broker_module(broker_name) -> ModuleType:
    return importlib.import_module(f'dispatcher.brokers.{broker_name}')


def get_async_broker(broker_name, broker_config) -> BaseBroker:
    broker_module = get_broker_module(broker_name)
    broker_cls = getattr(broker_module, 'AsyncBroker')
    return broker_cls(**broker_config)


def get_sync_broker(broker_name, broker_config) -> BaseBroker:
    broker_module = get_broker_module(broker_name)
    broker_cls = getattr(broker_module, 'SyncBroker')
    return broker_cls(**broker_config)
