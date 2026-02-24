from typing import Callable

import pytest

from dispatcherd.service.asyncio_tasks import SharedAsyncObjects
from dispatcherd.service.pool import WorkerPool
from dispatcherd.service.process import ProcessManager


@pytest.fixture
def pool_factory(test_settings) -> Callable[..., WorkerPool]:
    def _factory(**kwargs_overrides) -> WorkerPool:
        pm = ProcessManager(settings=test_settings)
        kwargs = dict(process_manager=pm, min_workers=5, max_workers=5, shared=SharedAsyncObjects())
        kwargs.update(kwargs_overrides)
        pool = WorkerPool(**kwargs)
        return pool

    return _factory
