import os
from contextlib import contextmanager
from typing import Optional

import yaml

from dispatcher.publish import DispatcherDecorator
from dispatcher.registry import DispatcherMethodRegistry, registry
from dispatcher.utils import resolve_callable


class DispatcherSettings:
    def __init__(self, config: dict, task_registry: DispatcherMethodRegistry = registry) -> None:
        self.brokers: list = config.get('brokers', [])
        self.producers: list = config.get('producers', [])
        self.service: dict = config.get('service', {'max_workers': 3})
        self.callbacks: dict = config.get('callbacks', {})
        self.tasks: dict = config.get('tasks', {})
        self.settings: dict = config.get('settings', {})

        for task_name, options in self.tasks.items():
            method = resolve_callable(task_name)
            if method:
                DispatcherDecorator(task_registry, **options)(method)
            else:
                raise RuntimeError(f'Could not locate task given in config: {task_name}')


def settings_from_file(path: str) -> DispatcherSettings:
    with open(path, 'r') as f:
        config_content = f.read()

    config = yaml.safe_load(config_content)
    return DispatcherSettings(config)


def settings_from_env() -> DispatcherSettings:
    if file_path := os.getenv('DISPATCHER_CONFIG_FILE'):
        return settings_from_file(file_path)
    raise RuntimeError('Dispatcher not configured, set DISPATCHER_CONFIG_FILE or call dispatcher.config.setup')


empty = object()


class LazySettings:
    def __init__(self) -> None:
        self._wrapped: Optional[DispatcherSettings] = None

    def __getattr__(self, name):
        if self._wrapped is empty:
            self._setup()
        return getattr(self._wrapped, name)

    def _setup(self) -> None:
        self._wrapped = settings_from_env()


settings = LazySettings()


def setup(config: Optional[dict] = None, file_path: Optional[str] = None):
    if config:
        settings._wrapped = DispatcherSettings(config)
    elif file_path:
        settings._wrapped = settings_from_file(file_path)
    settings._wrapped = settings_from_env()


@contextmanager
def temporary_settings(config):
    prior_settings = settings._wrapped
    try:
        settings._wrapped = DispatcherSettings(config)
        yield settings
    finally:
        settings._wrapped = prior_settings
