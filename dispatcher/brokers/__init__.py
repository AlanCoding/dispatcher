import importlib
from typing import Optional
from types import ModuleType

from dispatcher.brokers.base import BaseBroker
from dispatcher.config import settings


def get_broker_module(broker_name) -> ModuleType:
    return importlib.import_module(f'dispatcher.brokers.{broker_name}')


def get_async_broker(broker_name, broker_config) -> BaseBroker:
    broker_module = get_broker_module(broker_name)
    return broker_module.AsyncBroker(**broker_config)


def get_sync_broker(broker_name, broker_config) -> BaseBroker:
    broker_module = get_broker_module(broker_name)
    return broker_module.SyncBroker(**broker_config)


def get_sync_publisher_from_settings(publish_broker: Optional[str] = None) -> BaseBroker:
    if publish_broker:
        pass
    elif len(settings.brokers) == 1:
        publish_broker = list(settings.brokers.keys())[0]
    elif 'default_broker' in settings.publish:
        publish_broker = settings.publish['default_broker']
    else:
        raise RuntimeError(f'Could not determine which broker to publish with between options {list(settings.brokers.keys())}')

    return get_sync_broker(publish_broker, settings.brokers[publish_broker])


def get_async_publisher_from_settings() -> BaseBroker:
    publish_broker = settings.publish['default_broker']
    return get_async_broker(publish_broker, settings.brokers[publish_broker])
