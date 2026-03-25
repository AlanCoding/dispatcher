from unittest import mock

import pytest

from dispatcherd.config import temporary_settings
from dispatcherd.publish import task


def test_apply_async_with_no_queue(registry, conn_config):
    @task(registry=registry)
    def test_method():
        return

    dmethod = registry.get_from_callable(test_method)

    # These settings do not specify a default channel, that is the main point
    with temporary_settings({'version': 2, 'brokers': {'pg_notify': {'config': conn_config}}}):

        # Can not run a method if we do not have a queue
        with pytest.raises(ValueError):
            dmethod.apply_async()

        # But providing a queue at time of submission works
        with mock.patch('dispatcherd.brokers.pg_notify.Broker.publish_message') as mock_publish_method:
            dmethod.apply_async(queue='fooqueue')
        mock_publish_method.assert_called_once_with(channel='fooqueue', message=mock.ANY)

    mock_publish_method.assert_called_once()


def test_delay_uses_task_queue(registry, conn_config):
    """Task decorated with queue= should publish to that queue when using .delay()"""

    @task(queue='task_specific_queue', registry=registry)
    def test_method():
        return

    dmethod = registry.get_from_callable(test_method)

    with temporary_settings({'version': 2, 'brokers': {'pg_notify': {'config': conn_config}}}):
        with mock.patch('dispatcherd.brokers.pg_notify.Broker.publish_message') as mock_publish_method:
            dmethod.delay()

        mock_publish_method.assert_called_once_with(channel='task_specific_queue', message=mock.ANY)


def test_apply_async_explicit_queue_overrides_task_queue(registry, conn_config):
    """Explicit queue in apply_async() should override the task's decorated queue"""

    @task(queue='task_specific_queue', registry=registry)
    def test_method():
        return

    dmethod = registry.get_from_callable(test_method)

    with temporary_settings({'version': 2, 'brokers': {'pg_notify': {'config': conn_config}}}):
        with mock.patch('dispatcherd.brokers.pg_notify.Broker.publish_message') as mock_publish_method:
            dmethod.apply_async(queue='override_queue')

        mock_publish_method.assert_called_once_with(channel='override_queue', message=mock.ANY)
