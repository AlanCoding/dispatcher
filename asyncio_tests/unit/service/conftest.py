import queue
from typing import Callable

import pytest

from dispatcherd.service.asyncio_tasks import SharedAsyncObjects
from dispatcherd.service.pool import WorkerPool
from dispatcherd.service.process import ProcessManager


def fill_and_check_scale_down(pool: WorkerPool) -> bool:
    """Test helper: fill the usage tracker with current pool state and check if scale-down is warranted."""
    worker_ct = len([w for w in pool.workers if w.counts_for_capacity])
    demand_ct = pool.active_task_ct()
    pool.usage_tracker.fill_unknown_usage(worker_ct, demand_ct)
    return pool.usage_tracker.should_scale_down(worker_ct, demand_ct)


class FakeProcess:
    """A stand-in for ProcessProxy that requires no multiprocessing resources.

    Provides the same interface as ProcessProxy (pid, is_alive, start, join,
    kill, exitcode, message_queue) but backed by a plain queue.Queue and
    simple flags.  No subprocess is created or forked.
    """

    def __init__(self) -> None:
        self.message_queue: queue.Queue = queue.Queue()
        self._started = False

    def start(self) -> None:
        self._started = True

    def join(self, timeout: int | None = None) -> None:
        pass

    @property
    def pid(self) -> int | None:
        return None

    def exitcode(self) -> int | None:
        return 0

    def is_alive(self) -> bool:
        return False

    def kill(self) -> None:
        pass

    def terminate(self) -> None:
        pass


class FakeProcessManager:
    """A stand-in for ProcessManager that creates FakeProcess instances.

    No multiprocessing queues or real subprocesses are allocated.
    """

    def create_process(self, **kwargs) -> FakeProcess:
        return FakeProcess()

    def has_shutdown(self) -> bool:
        return False


class FakeWorkerPool(WorkerPool):
    """A WorkerPool that puts newly created workers straight into 'ready' status.

    Workers are real PoolWorker instances backed by FakeProcess objects (via
    FakeProcessManager), so all properties and bookkeeping methods work
    unchanged.  The only difference is that up() sets the worker to 'ready'
    immediately, skipping the initialized -> spawned -> starting -> ready
    lifecycle that requires a real subprocess.
    """

    async def up(self) -> int:
        worker_id = await super().up()
        self.workers.get_by_id(worker_id).status = 'ready'
        return worker_id


@pytest.fixture
def pool_factory(test_settings) -> Callable[..., WorkerPool]:
    """Create a WorkerPool backed by a real ProcessManager.

    Workers created via pool.up() are real PoolWorker instances wrapping
    ProcessProxy objects.  No subprocess is forked until worker.start() is
    called (by manage_new_workers), but the ProcessManager does allocate
    multiprocessing queues.

    Use this when the test needs:
    - Real ProcessManager queues (read_results_forever, start_working, shutdown)
    - Worker lifecycle states (initialized -> spawned -> starting -> ready)
    - Subprocess error handling (worker.start() failures)

    For tests that only exercise pool-level scaling, scheduling, or status
    logic, prefer fake_pool_factory instead.
    """

    def _factory(**kwargs_overrides) -> WorkerPool:
        pm = ProcessManager(settings=test_settings)
        kwargs = dict(process_manager=pm, min_workers=5, max_workers=5, shared=SharedAsyncObjects())
        kwargs.update(kwargs_overrides)
        pool = WorkerPool(**kwargs)
        return pool

    return _factory


@pytest.fixture
def fake_pool_factory() -> Callable[..., FakeWorkerPool]:
    """Create a FakeWorkerPool backed by FakeProcessManager.

    Workers created via pool.up() are real PoolWorker instances backed by
    FakeProcess objects, immediately set to 'ready' status.  No subprocess
    is created or forked, and no multiprocessing queues are allocated.

    Use this for tests that exercise pool-level logic such as:
    - Scale-up / scale-down decisions
    - Task dispatching and scheduling
    - Status data reporting

    Use the real pool_factory when you need:
    - Real ProcessManager queues (read_results_forever, start_working, shutdown)
    - Worker lifecycle states (initialized -> spawned -> starting -> ready)
    - Subprocess error handling (worker.start() failures)
    """

    def _factory(**kwargs_overrides) -> FakeWorkerPool:
        pm = FakeProcessManager()
        kwargs = dict(process_manager=pm, min_workers=5, max_workers=5, shared=SharedAsyncObjects())
        kwargs.update(kwargs_overrides)
        pool = FakeWorkerPool(**kwargs)
        return pool

    return _factory
