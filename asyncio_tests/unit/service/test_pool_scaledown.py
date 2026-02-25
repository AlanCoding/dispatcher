# Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
# See also: tests/unit/service/test_usage_tracker.py for synchronous unit tests of WorkerUsageTracker
from unittest.mock import patch

import pytest


@pytest.mark.asyncio
async def test_scale_down_idle_workers(fake_pool_factory):
    """3 idle workers, no running tasks.  First tick fills the tracker,
    second tick (after scaledown_wait) allows scale-down."""
    pool = fake_pool_factory(min_workers=1, max_workers=10)

    for _ in range(3):
        worker_id = await pool.up()
        assert pool.workers.get_by_id(worker_id).status == 'ready'

    base = 1000.0
    # First tick: fills keys 1-3 as idle timestamps — too recent to scale down
    with patch('time.monotonic', return_value=base):
        assert pool.should_scale_down() is False

    # Second tick after scaledown_wait: timestamps are old enough
    with patch('time.monotonic', return_value=base + 100.0):
        assert pool.should_scale_down() is True
        await pool.scale_workers()
    statuses = [w.status for w in pool.workers]
    assert 'stopping' in statuses


@pytest.mark.asyncio
async def test_scale_down_blocked_by_active_work(fake_pool_factory):
    """3 workers with 3 running tasks — should_scale_down() must return False."""
    pool = fake_pool_factory(min_workers=1, max_workers=10)

    for _ in range(3):
        worker_id = await pool.up()
        worker = pool.workers.get_by_id(worker_id)
        assert worker.status == 'ready'
        await worker.start_task({'task': 'busy'})

    assert pool.should_scale_down() is False


@pytest.mark.asyncio
async def test_task_finish_enables_timely_scale_down(fake_pool_factory):
    """record_task_finish records a timestamp on the hot path, so scale-down
    can proceed at the earliest possible moment without waiting for the next
    periodic fill tick."""
    pool = fake_pool_factory(min_workers=1, max_workers=10)

    for _ in range(3):
        worker_id = await pool.up()
        assert pool.workers.get_by_id(worker_id).status == 'ready'

    base = 1000.0
    # Simulate a task finishing — sets timestamp at running_ct
    with patch('time.monotonic', return_value=base):
        pool.usage_tracker.record_task_finish(3)

    # Periodic fill hasn't run yet, but key 3 already has a timestamp
    # After scaledown_wait, scale-down proceeds without needing a prior fill tick
    with patch('time.monotonic', return_value=base + 100.0):
        assert pool.should_scale_down() is True


@pytest.mark.asyncio
async def test_scale_down_cascade_to_min_workers(fake_pool_factory):
    """10 idle workers.  After two ticks, repeatedly calling scale_workers()
    should scale all the way down to min_workers=1."""
    pool = fake_pool_factory(min_workers=1, max_workers=10)

    for _ in range(10):
        worker_id = await pool.up()
        assert pool.workers.get_by_id(worker_id).status == 'ready'

    base = 1000.0
    # First tick: fill all keys as idle
    with patch('time.monotonic', return_value=base):
        pool.should_scale_down()

    # After scaledown_wait, a single scale_workers call retires all surplus
    with patch('time.monotonic', return_value=base + 100.0):
        await pool.scale_workers()

    stopping = [w for w in pool.workers if w.status == 'stopping']
    capacity_workers = [w for w in pool.workers if w.counts_for_capacity]
    assert len(stopping) == 9
    assert len(capacity_workers) == 1


@pytest.mark.asyncio
async def test_scale_down_respects_active_load(fake_pool_factory):
    """5 workers, 3 running tasks.  Should scale down surplus but never
    below the active load."""
    pool = fake_pool_factory(min_workers=1, max_workers=10)

    workers = []
    for _ in range(5):
        worker_id = await pool.up()
        workers.append(pool.workers.get_by_id(worker_id))
        assert workers[-1].status == 'ready'

    # Give 3 workers tasks
    for i in range(3):
        await workers[i].start_task({'task': f'busy-{i}'})

    base = 1000.0
    # First tick: keys 1-3 blocked, keys 4-5 idle timestamps
    with patch('time.monotonic', return_value=base):
        assert pool.should_scale_down() is False

    # After scaledown_wait: surplus keys aged out, scale down from 5
    with patch('time.monotonic', return_value=base + 100.0):
        assert pool.should_scale_down() is True
        await pool.scale_workers()

    statuses = [w.status for w in pool.workers]
    assert statuses.count('stopping') == 2
    assert statuses.count('ready') == 3


@pytest.mark.asyncio
async def test_blocked_entries_clear_when_load_drops(fake_pool_factory):
    """When demand drops, previously blocked entries should convert to
    timestamps and eventually allow scale-down."""
    pool = fake_pool_factory(min_workers=1, max_workers=10)

    workers = []
    for _ in range(5):
        worker_id = await pool.up()
        workers.append(pool.workers.get_by_id(worker_id))
        assert workers[-1].status == 'ready'

    # All 5 busy
    for w in workers:
        await w.start_task({'task': 'busy'})

    base = 1000.0
    with patch('time.monotonic', return_value=base):
        assert pool.should_scale_down() is False  # all blocked

    # Tasks finish — clear current_task to simulate completion
    for w in workers:
        w.current_task = None

    # Next tick: running_ct=0, previously blocked keys become timestamps
    with patch('time.monotonic', return_value=base + 1.0):
        assert pool.should_scale_down() is False  # timestamps too recent

    # After scaledown_wait
    with patch('time.monotonic', return_value=base + 100.0):
        assert pool.should_scale_down() is True


@pytest.mark.asyncio
async def test_status_data_includes_usage_diagnostics(fake_pool_factory):
    """Populate tracker via fill_unknown_usage with a mix of blocked and idle entries.
    Assert get_status_data() includes count, top5, and near-worker-ct."""
    pool = fake_pool_factory(min_workers=1, max_workers=10)

    # Create 4 ready workers, give 2 of them tasks
    workers = []
    for _ in range(4):
        worker_id = await pool.up()
        workers.append(pool.workers.get_by_id(worker_id))
        assert workers[-1].status == 'ready'

    await workers[0].start_task({'task': 'busy-1'})
    await workers[1].start_task({'task': 'busy-2'})

    base = 1000.0
    # Fill: running_ct=2, so keys 1-2 blocked, keys 3-4 idle
    with patch('time.monotonic', return_value=base):
        pool.should_scale_down()

    with patch('time.monotonic', return_value=base + 10.0):
        data = pool.get_status_data()

    assert data["worker_ct"] == 4
    assert data["running_ct"] == 2
    assert data["usage"]["count"] == 4

    # Near worker_ct=4: keys [2..6]
    near = data["usage"]["near_worker_ct"]
    assert set(near.keys()) == {2, 3, 4, 5, 6}
    assert near[2] == "blocked"
    assert isinstance(near[3], float)
    assert isinstance(near[4], float)
    assert near[5] == "absent"
    assert near[6] == "absent"


@pytest.mark.asyncio
async def test_status_data_near_worker_ct_shows_absent_keys(fake_pool_factory):
    """When usage tracker has no entries near the current worker count,
    the near-worker-ct summary should show 'absent' for missing keys."""
    pool = fake_pool_factory(min_workers=1, max_workers=10)

    # Create 8 ready workers so worker_ct=8
    for _ in range(8):
        worker_id = await pool.up()
        assert pool.workers.get_by_id(worker_id).status == 'ready'

    # Fill only keys 1-2 (via fill with worker_ct=2)
    pool.usage_tracker.fill_unknown_usage(worker_ct=2, running_ct=0)

    data = pool.get_status_data()
    near = data["usage"]["near_worker_ct"]
    # Keys [6..10] centered on worker_ct=8, none present in usage tracker
    assert set(near.keys()) == {6, 7, 8, 9, 10}
    for k in near:
        assert near[k] == "absent"


@pytest.mark.asyncio
async def test_scaledown_reserve_keeps_surplus(fake_pool_factory):
    """6 workers, 2 busy, scaledown_reserve=2, min_workers=1.
    After scaledown_wait, scale_workers should stop 2 workers
    (down to 4 = 2 demand + 2 reserve), not all the way to 1."""
    pool = fake_pool_factory(min_workers=1, max_workers=10, scaledown_reserve=2)

    workers = []
    for _ in range(6):
        worker_id = await pool.up()
        workers.append(pool.workers.get_by_id(worker_id))
        assert workers[-1].status == 'ready'

    # Give 2 workers tasks
    for i in range(2):
        await workers[i].start_task({'task': f'busy-{i}'})

    base = 1000.0
    # First tick: keys 1-2 blocked, keys 3-6 idle timestamps
    with patch('time.monotonic', return_value=base):
        assert pool.should_scale_down() is False

    # After scaledown_wait: surplus above reserve aged out
    with patch('time.monotonic', return_value=base + 100.0):
        assert pool.should_scale_down() is True
        await pool.scale_workers()

    statuses = [w.status for w in pool.workers]
    assert statuses.count('stopping') == 2  # scaled from 6 to 4
    assert statuses.count('ready') == 4  # 2 busy + 2 reserve
