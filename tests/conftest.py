from typing import Callable

import pytest

from dispatcher.main import DispatcherMain
from dispatcher.control import Control

from dispatcher.brokers.pg_notify import publish_message


# List of channels to listen on
CHANNELS = ['test_channel', 'test_channel2', 'test_channel3']

# Database connection details
CONNECTION_STRING = "dbname=dispatch_db user=dispatch password=dispatching host=localhost port=55777"


@pytest.fixture
def pg_dispatcher() -> DispatcherMain:
    return DispatcherMain({"producers": {"brokers": {"pg_notify": {"conninfo": CONNECTION_STRING}, "channels": CHANNELS}}, "pool": {"max_workers": 3}})


@pytest.fixture
def pg_message() -> Callable:
    def _rf(message, channel='test_channel'):
        publish_message(channel, message, config={"conninfo": CONNECTION_STRING})
    return _rf


@pytest.fixture
def pg_control() -> Control:
    return Control('test_channel', config={'conninfo': CONNECTION_STRING})
