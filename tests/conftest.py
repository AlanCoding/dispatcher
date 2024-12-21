import asyncio

from typing import Callable, AsyncIterator

import pytest

import pytest_asyncio

from dispatcher.main import DispatcherMain
from dispatcher.control import Control

from dispatcher.brokers.pg_notify import publish_message


# List of channels to listen on
CHANNELS = ['test_channel', 'test_channel2', 'test_channel3']

# Database connection details
CONNECTION_STRING = "dbname=dispatch_db user=dispatch password=dispatching host=localhost port=55777"

BASIC_CONFIG = {"producers": {"brokers": {"pg_notify": {"conninfo": CONNECTION_STRING}, "channels": CHANNELS}}, "pool": {"max_workers": 3}}


@pytest.fixture
def pg_dispatcher() -> DispatcherMain:
    return DispatcherMain(BASIC_CONFIG)


@pytest_asyncio.fixture()
async def apg_dispatcher(request) -> AsyncIterator[DispatcherMain]:
    try:
        dispatcher = DispatcherMain(BASIC_CONFIG)

        await dispatcher.connect_signals()
        await dispatcher.start_working()
        await dispatcher.wait_for_producers_ready()

        yield dispatcher
    finally:
        await dispatcher.shutdown()
        await dispatcher.cancel_tasks()


@pytest.fixture
def pg_message() -> Callable:
    def _rf(message, channel='test_channel'):
        publish_message(channel, message, config={"conninfo": CONNECTION_STRING})
    return _rf


@pytest.fixture
def pg_control() -> Control:
    return Control('test_channel', config={'conninfo': CONNECTION_STRING})
