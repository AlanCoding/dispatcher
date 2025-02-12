import pytest

from dispatcher.config import DispatcherSettings, LazySettings


def test_settings_reference_unconfigured():
    settings = LazySettings()
    with pytest.raises(Exception) as exc:
        settings.brokers
    assert 'Dispatcher not configured' in str(exc)


def test_configured_settings():
    settings = LazySettings()
    settings._wrapped = DispatcherSettings({'brokers': {'pg_notify': {'config': {}}}})
    'pg_notify' in settings.brokers
