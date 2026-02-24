# Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
# See also: tests/unit/service/test_usage_tracker.py for synchronous unit tests of WorkerUsageTracker
import time

import pytest

from dispatcherd.service.pool import WorkerUsageTracker


@pytest.mark.asyncio
async def test_scale_down_when_never_needed_this_many_workers(pool_factory):
    """Core regression: if _last_used_by_ct has entries for counts 1-2 but NOT 3,
    should_scale_down() must return True and scale_workers() must produce a stopping worker."""
    pool = pool_factory(min_workers=1, max_workers=10)

    # Create 3 ready idle workers
    for _ in range(3):
        worker_id = await pool.up()
        pool.workers.get_by_id(worker_id).status = 'ready'

    # Only have entries for counts 1 and 2, NOT 3
    pool.usage_tracker._last_used_by_ct = {
        1: time.monotonic() - 120.0,
        2: time.monotonic() - 120.0,
    }

    assert pool.should_scale_down() is True
    await pool.scale_workers()
    statuses = [w.status for w in pool.workers]
    assert 'stopping' in statuses


@pytest.mark.asyncio
async def test_scale_down_blocked_by_sentinel(pool_factory):
    """3 ready idle workers with _last_used_by_ct[3] set to the sentinel.
    should_scale_down() must return False."""
    pool = pool_factory(min_workers=1, max_workers=10)

    for _ in range(3):
        worker_id = await pool.up()
        pool.workers.get_by_id(worker_id).status = 'ready'

    pool.usage_tracker.record_task_start(3)

    assert pool.should_scale_down() is False


@pytest.mark.asyncio
async def test_scale_down_cascade_through_gaps(pool_factory):
    """10 ready idle workers, _last_used_by_ct only has entries for counts 1-5.
    Repeatedly calling scale_workers() should scale all the way down to min_workers=1."""
    pool = pool_factory(min_workers=1, max_workers=10)

    for _ in range(10):
        worker_id = await pool.up()
        pool.workers.get_by_id(worker_id).status = 'ready'

    # Only populate entries for counts 1-5
    pool.usage_tracker._last_used_by_ct = {i: time.monotonic() - 120.0 for i in range(1, 6)}

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
    """5 ready idle workers exercising all three states of _last_used_by_ct[5]:
    sentinel (False), old timestamp (True), recent timestamp (False), absent key (True)."""
    pool = pool_factory(min_workers=1, max_workers=10)

    for _ in range(5):
        worker_id = await pool.up()
        pool.workers.get_by_id(worker_id).status = 'ready'

    # State 1: sentinel blocks scale-down
    pool.usage_tracker.record_task_start(5)
    assert pool.should_scale_down() is False

    # State 2: old timestamp allows scale-down
    pool.usage_tracker._last_used_by_ct[5] = time.monotonic() - 120.0
    assert pool.should_scale_down() is True

    # State 3: recent timestamp blocks scale-down
    pool.usage_tracker._last_used_by_ct[5] = time.monotonic()
    assert pool.should_scale_down() is False

    # State 4: absent key allows scale-down
    del pool.usage_tracker._last_used_by_ct[5]
    assert pool.should_scale_down() is True


@pytest.mark.asyncio
async def test_status_data_includes_last_used_diagnostics(pool_factory):
    """Populate _last_used_by_ct with 6 entries mixing sentinels and timestamps.
    Assert get_status_data() includes count, top5, and near-worker-ct with correct formatting."""
    pool = pool_factory(min_workers=1, max_workers=10)

    # Create 4 ready workers so worker_ct=4
    for _ in range(4):
        worker_id = await pool.up()
        pool.workers.get_by_id(worker_id).status = 'ready'

    now = time.monotonic()
    pool.usage_tracker._last_used_by_ct = {
        1: now - 100.0,
        2: now - 50.0,
        3: WorkerUsageTracker._SCALE_DOWN_BLOCKED,
        4: now - 10.0,
        5: WorkerUsageTracker._SCALE_DOWN_BLOCKED,
        6: now - 1.0,
    }

    data = pool.get_status_data()
    assert data["worker_ct"] == 4
    assert data["usage"]["count"] == 6

    top5 = data["usage"]["top5"]
    # Top 5 highest keys: 6, 5, 4, 3, 2
    assert set(top5.keys()) == {2, 3, 4, 5, 6}

    # Sentinel entries show "blocked"
    assert top5[3] == "blocked"
    assert top5[5] == "blocked"

    # Timestamp entries show seconds-ago as floats
    assert isinstance(top5[2], float)
    assert isinstance(top5[4], float)
    assert isinstance(top5[6], float)

    # Near-worker-ct shows keys [2..6] centered on worker_ct=4
    near = data["usage"]["near_worker_ct"]
    assert set(near.keys()) == {2, 3, 4, 5, 6}
    assert isinstance(near[2], float)
    assert near[3] == "blocked"
    assert isinstance(near[4], float)
    assert near[5] == "blocked"
    assert isinstance(near[6], float)


@pytest.mark.asyncio
async def test_status_data_near_worker_ct_shows_absent_keys(pool_factory):
    """When _last_used_by_ct has no entries near the current worker count,
    the near-worker-ct summary should show 'absent' for missing keys."""
    pool = pool_factory(min_workers=1, max_workers=10)

    # Create 8 ready workers so worker_ct=8
    for _ in range(8):
        worker_id = await pool.up()
        pool.workers.get_by_id(worker_id).status = 'ready'

    # Only populate entries far from worker_ct=8
    now = time.monotonic()
    pool.usage_tracker._last_used_by_ct = {1: now - 100.0, 2: now - 50.0}

    data = pool.get_status_data()
    near = data["usage"]["near_worker_ct"]
    # Keys [6..10] centered on worker_ct=8, none present in _last_used_by_ct
    assert set(near.keys()) == {6, 7, 8, 9, 10}
    for k in near:
        assert near[k] == "absent"
