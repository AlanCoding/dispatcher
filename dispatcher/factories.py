import inspect
from copy import deepcopy
from typing import Iterable, Optional, Type, get_args, get_origin

from dispatcher import producers
from dispatcher.brokers import get_broker
from dispatcher.brokers.base import BaseBroker
from dispatcher.config import LazySettings
from dispatcher.config import settings as global_settings
from dispatcher.control import Control
from dispatcher.main import DispatcherMain
from dispatcher.pool import WorkerPool
from dispatcher import process

"""
Creates objects from settings,
This is kept separate from the settings and the class definitions themselves,
which is to avoid import dependencies.
"""

# ---- Service objects ----


def process_manager_from_settings(settings: LazySettings = global_settings):
    cls_name = settings.service.get('process_manager_cls', 'ForkServer')
    process_manager_cls = getattr(process, cls_name)
    return process_manager_cls()


def pool_from_settings(settings: LazySettings = global_settings):
    kwargs = settings.service.get('pool_kwargs', {}).copy()
    kwargs['settings'] = settings
    kwargs['process_manager'] = process_manager_from_settings(settings=settings)
    return WorkerPool(**kwargs)


def brokers_from_settings(settings: LazySettings = global_settings) -> Iterable[BaseBroker]:
    return [get_broker(broker_name, broker_kwargs) for broker_name, broker_kwargs in settings.brokers.items()]


def producers_from_settings(settings: LazySettings = global_settings) -> Iterable[producers.BaseProducer]:
    producer_objects = []
    for broker in brokers_from_settings(settings=settings):
        producer = producers.BrokeredProducer(broker=broker)
        producer_objects.append(producer)

    for producer_cls, producer_kwargs in settings.producers.items():
        producer_objects.append(getattr(producers, producer_cls)(**producer_kwargs))

    return producer_objects


def from_settings(settings: LazySettings = global_settings) -> DispatcherMain:
    """
    Returns the main dispatcher object, used for running the background task service.
    You could initialize this yourself, but using the shared settings allows for consistency
    between the service, publisher, and any other interacting processes.
    """
    producers = producers_from_settings(settings=settings)
    pool = pool_from_settings(settings=settings)
    return DispatcherMain(producers, pool)


# ---- Publisher objects ----


def _get_publisher_broker_name(publish_broker: Optional[str] = None, settings: LazySettings = global_settings) -> str:
    if publish_broker:
        return publish_broker
    elif len(settings.brokers) == 1:
        return list(settings.brokers.keys())[0]
    elif 'default_broker' in settings.publish:
        return settings.publish['default_broker']
    else:
        raise RuntimeError(f'Could not determine which broker to publish with between options {list(settings.brokers.keys())}')


def get_publisher_from_settings(publish_broker: Optional[str] = None, settings: LazySettings = global_settings, **overrides) -> BaseBroker:
    """
    An asynchronous publisher is the ideal choice for submitting control-and-reply actions.
    This returns an asyncio broker of the default publisher type.

    If channels are specified, these completely replace the channel list from settings.
    For control-and-reply, this will contain only the reply_to channel, to not receive
    unrelated traffic.
    """
    publish_broker = _get_publisher_broker_name(publish_broker=publish_broker, settings=settings)
    return get_broker(publish_broker, settings.brokers[publish_broker], **overrides)


def get_control_from_settings(publish_broker: Optional[str] = None, settings: LazySettings = global_settings, **overrides):
    publish_broker = _get_publisher_broker_name(publish_broker=publish_broker, settings=settings)
    broker_options = settings.brokers[publish_broker].copy()
    broker_options.update(overrides)
    return Control(publish_broker, broker_options)


# ---- Schema generation ----

SERIALIZED_TYPES = (int, str, dict, type(None), tuple, list)


def is_valid_annotation(annotation):
    if get_origin(annotation):
        for arg in get_args(annotation):
            if not is_valid_annotation(arg):
                return False
    else:
        if annotation not in SERIALIZED_TYPES:
            return False
    return True


def schema_for_cls(cls: Type) -> dict[str, str]:
    signature = inspect.signature(cls.__init__)
    parameters = signature.parameters
    spec = {}
    for k, p in parameters.items():
        if is_valid_annotation(p.annotation):
            spec[k] = str(p.annotation)
    return spec


def generate_settings_schema(settings: LazySettings = global_settings) -> dict:
    ret = deepcopy(settings.serialize())

    ret['service']['pool_kwargs'] = schema_for_cls(WorkerPool)

    for broker_name, broker_kwargs in settings.brokers.items():
        broker = get_broker(broker_name, broker_kwargs)
        ret['brokers'][broker_name] = schema_for_cls(type(broker))

    for producer_cls, producer_kwargs in settings.producers.items():
        ret['producers'][producer_cls] = schema_for_cls(getattr(producers, producer_cls))

    ret['publish'] = {'default_broker': 'str'}

    return ret
