# Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
import time
from typing import Callable

import pytest

from dispatcherd.service.asyncio_tasks import SharedAsyncObjects
from dispatcherd.service.pool import _SCALE_DOWN_BLOCKED, WorkerPool
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


@pytest.mark.asyncio
async def test_scale_down_when_never_needed_this_many_workers(pool_factory):
    """Core regression: if last_used_by_ct has entries for counts 1-2 but NOT 3,
    should_scale_down() must return True and scale_workers() must produce a stopping worker."""
    pool = pool_factory(min_workers=1, max_workers=10)

    # Create 3 ready idle workers
    for _ in range(3):
        worker_id = await pool.up()
        pool.workers.get_by_id(worker_id).status = 'ready'

    # Only have entries for counts 1 and 2, NOT 3
    pool.last_used_by_ct = {
        1: time.monotonic() - 120.0,
        2: time.monotonic() - 120.0,
    }

    assert pool.should_scale_down() is True
    await pool.scale_workers()
    statuses = [w.status for w in pool.workers]
    assert 'stopping' in statuses


@pytest.mark.asyncio
async def test_scale_down_blocked_by_sentinel(pool_factory):
    """3 ready idle workers with last_used_by_ct[3] set to the sentinel.
    should_scale_down() must return False."""
    pool = pool_factory(min_workers=1, max_workers=10)

    for _ in range(3):
        worker_id = await pool.up()
        pool.workers.get_by_id(worker_id).status = 'ready'

    pool.last_used_by_ct[3] = _SCALE_DOWN_BLOCKED

    assert pool.should_scale_down() is False


@pytest.mark.asyncio
async def test_scale_down_cascade_through_gaps(pool_factory):
    """10 ready idle workers, last_used_by_ct only has entries for counts 1-5.
    Repeatedly calling scale_workers() should scale all the way down to min_workers=1."""
    pool = pool_factory(min_workers=1, max_workers=10)

    for _ in range(10):
        worker_id = await pool.up()
        pool.workers.get_by_id(worker_id).status = 'ready'

    # Only populate entries for counts 1-5
    pool.last_used_by_ct = {i: time.monotonic() - 120.0 for i in range(1, 6)}

    # Repeatedly scale down, retiring stopped workers between calls
    for _ in range(20):  # enough iterations to converge
        await pool.scale_workers()
        # Mark stopping workers as retired so they no longer count for capacity
        for w in pool.workers:
            if w.status == 'stopping':
                w.status = 'retired'
                w.retired_at = time.monotonic()

    capacity_workers = [w for w in pool.workers if w.counts_for_capacity]
    assert len(capacity_workers) == 1


@pytest.mark.asyncio
async def test_scale_down_with_conflicting_stale_data(pool_factory):
    """5 ready idle workers exercising all three states of last_used_by_ct[5]:
    sentinel (False), old timestamp (True), recent timestamp (False), absent key (True)."""
    pool = pool_factory(min_workers=1, max_workers=10)

    for _ in range(5):
        worker_id = await pool.up()
        pool.workers.get_by_id(worker_id).status = 'ready'

    # State 1: sentinel blocks scale-down
    pool.last_used_by_ct[5] = _SCALE_DOWN_BLOCKED
    assert pool.should_scale_down() is False

    # State 2: old timestamp allows scale-down
    pool.last_used_by_ct[5] = time.monotonic() - 120.0
    assert pool.should_scale_down() is True

    # State 3: recent timestamp blocks scale-down
    pool.last_used_by_ct[5] = time.monotonic()
    assert pool.should_scale_down() is False

    # State 4: absent key allows scale-down
    del pool.last_used_by_ct[5]
    assert pool.should_scale_down() is True


@pytest.mark.asyncio
async def test_status_data_includes_last_used_diagnostics(pool_factory):
    """Populate last_used_by_ct with 6 entries mixing sentinels and timestamps.
    Assert get_status_data() includes count and top5 with correct formatting."""
    pool = pool_factory(min_workers=1, max_workers=10)

    now = time.monotonic()
    pool.last_used_by_ct = {
        1: now - 100.0,
        2: now - 50.0,
        3: _SCALE_DOWN_BLOCKED,
        4: now - 10.0,
        5: _SCALE_DOWN_BLOCKED,
        6: now - 1.0,
    }

    data = pool.get_status_data()
    assert data["worker_ct"] == 0  # no workers created, just testing status data formatting
    assert data["last_used_by_ct_count"] == 6

    top5 = data["last_used_by_ct_top5"]
    # Top 5 highest keys: 6, 5, 4, 3, 2
    assert set(top5.keys()) == {2, 3, 4, 5, 6}

    # Sentinel entries show "blocked"
    assert top5[3] == "blocked"
    assert top5[5] == "blocked"

    # Timestamp entries show seconds-ago as floats
    assert isinstance(top5[2], float)
    assert isinstance(top5[4], float)
    assert isinstance(top5[6], float)
