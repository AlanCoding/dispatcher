import pytest

from dispatcherd.brokers.pg_notify import Broker


def test_invalid_characters(conn_config):
    broker = Broker(config=conn_config)
    with pytest.raises(Exception):
        broker.publish_message(channel='foo-bar', message='message')

