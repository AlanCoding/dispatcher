# Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
# See also: tests/unit/service/test_usage_tracker.py for synchronous unit tests of WorkerUsageTracker
import time
from unittest.mock import patch

import pytest


@pytest.mark.asyncio
async def test_scale_down_when_never_needed_this_many_workers(fake_pool_factory):
    """Core regression: if usage tracker has entries for counts 1-2 but NOT 3,
    should_scale_down() must fill the gap and allow scale-down after scaledown_wait."""
    pool = fake_pool_factory(min_workers=1, max_workers=10)

    # Create 3 ready idle workers
    for _ in range(3):
        worker_id = await pool.up()
        pool.workers.get_by_id(worker_id).status = 'ready'

    base = 1000.0
    # Only have entries for counts 1 and 2, NOT 3
    with patch('time.monotonic', return_value=base):
        pool.usage_tracker.record_task_finish(1)
        pool.usage_tracker.record_task_finish(2)

    # First call detects the gap at count 3, fills it as idle — too recent to scale down
    with patch('time.monotonic', return_value=base + 100.0):
        assert pool.should_scale_down() is False

    # After scaledown_wait, the filled entry is old enough — scale down proceeds
    with patch('time.monotonic', return_value=base + 200.0):
        assert pool.should_scale_down() is True
        await pool.scale_workers()
    statuses = [w.status for w in pool.workers]
    assert 'stopping' in statuses


@pytest.mark.asyncio
async def test_scale_down_blocked_by_sentinel(fake_pool_factory):
    """3 ready idle workers with usage tracker recording active work at count 3.
    should_scale_down() must return False."""
    pool = fake_pool_factory(min_workers=1, max_workers=10)

    for _ in range(3):
        worker_id = await pool.up()
        pool.workers.get_by_id(worker_id).status = 'ready'

    pool.usage_tracker.record_task_start(3)

    assert pool.should_scale_down() is False


@pytest.mark.asyncio
async def test_scale_down_cascade_through_gaps(fake_pool_factory):
    """10 ready idle workers, usage tracker only has entries for counts 1-5.
    After filling gaps and waiting scaledown_wait, repeatedly calling
    scale_workers() should scale all the way down to min_workers=1."""
    pool = fake_pool_factory(min_workers=1, max_workers=10)

    for _ in range(10):
        worker_id = await pool.up()
        pool.workers.get_by_id(worker_id).status = 'ready'

    base = 1000.0
    # Only populate entries for counts 1-5
    with patch('time.monotonic', return_value=base):
        for i in range(1, 6):
            pool.usage_tracker.record_task_finish(i)

    # Trigger fill for absent keys 6-10
    with patch('time.monotonic', return_value=base + 100.0):
        pool.should_scale_down()

    # After scaledown_wait, both original entries (base) and filled entries
    # (base+100) are old enough — cascade scales all the way down
    with patch('time.monotonic', return_value=base + 200.0):
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
async def test_scale_down_with_conflicting_stale_data(fake_pool_factory):
    """5 ready idle workers exercising all three states of usage tracking:
    sentinel (False), old timestamp (True), recent timestamp (False), absent key (True)."""
    pool = fake_pool_factory(min_workers=1, max_workers=10)

    for _ in range(5):
        worker_id = await pool.up()
        pool.workers.get_by_id(worker_id).status = 'ready'

    # State 1: sentinel blocks scale-down
    pool.usage_tracker.record_task_start(5)
    assert pool.should_scale_down() is False

    # State 2: old timestamp allows scale-down
    with patch('time.monotonic', return_value=time.monotonic() - 120.0):
        pool.usage_tracker.record_task_finish(5)
    assert pool.should_scale_down() is True

    # State 3: recent timestamp blocks scale-down
    pool.usage_tracker.record_task_finish(5)
    assert pool.should_scale_down() is False

    # State 4: absent key — fill first, then scale-down after scaledown_wait
    worker_id = await pool.up()
    pool.workers.get_by_id(worker_id).status = 'ready'
    # Now worker_ct=6, no entry for 6 exists
    base = 1000.0
    with patch('time.monotonic', return_value=base):
        assert pool.should_scale_down() is False  # triggers fill, too recent
    with patch('time.monotonic', return_value=base + 120.0):
        assert pool.should_scale_down() is True  # past scaledown_wait


@pytest.mark.asyncio
async def test_status_data_includes_last_used_diagnostics(fake_pool_factory):
    """Populate usage tracker with 6 entries mixing sentinels and timestamps.
    Assert get_status_data() includes count, top5, and near-worker-ct with correct formatting."""
    pool = fake_pool_factory(min_workers=1, max_workers=10)

    # Create 4 ready workers so worker_ct=4
    for _ in range(4):
        worker_id = await pool.up()
        pool.workers.get_by_id(worker_id).status = 'ready'

    base = 1000.0
    with patch('time.monotonic', return_value=base):
        pool.usage_tracker.record_task_finish(1)
    with patch('time.monotonic', return_value=base + 50.0):
        pool.usage_tracker.record_task_finish(2)
    pool.usage_tracker.record_task_start(3)
    with patch('time.monotonic', return_value=base + 90.0):
        pool.usage_tracker.record_task_finish(4)
    pool.usage_tracker.record_task_start(5)
    with patch('time.monotonic', return_value=base + 99.0):
        pool.usage_tracker.record_task_finish(6)

    with patch('time.monotonic', return_value=base + 100.0):
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
async def test_status_data_near_worker_ct_shows_absent_keys(fake_pool_factory):
    """When usage tracker has no entries near the current worker count,
    the near-worker-ct summary should show 'absent' for missing keys."""
    pool = fake_pool_factory(min_workers=1, max_workers=10)

    # Create 8 ready workers so worker_ct=8
    for _ in range(8):
        worker_id = await pool.up()
        pool.workers.get_by_id(worker_id).status = 'ready'

    # Only populate entries far from worker_ct=8
    pool.usage_tracker.record_task_finish(1)
    pool.usage_tracker.record_task_finish(2)

    data = pool.get_status_data()
    near = data["usage"]["near_worker_ct"]
    # Keys [6..10] centered on worker_ct=8, none present in usage tracker
    assert set(near.keys()) == {6, 7, 8, 9, 10}
    for k in near:
        assert near[k] == "absent"
