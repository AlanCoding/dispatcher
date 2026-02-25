# Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
# See also: asyncio_tests/unit/service/test_pool_scaledown.py for async integration tests
from unittest.mock import patch

from dispatcherd.service.pool import WorkerUsageTracker


class TestRecordTaskFinish:
    def test_recent_finish_blocks_scale_down(self):
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        tracker.record_task_finish(3)
        assert tracker.should_scale_down(3) is False

    def test_old_finish_allows_scale_down(self):
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        base = 1000.0
        with patch('time.monotonic', return_value=base):
            tracker.record_task_finish(3)
        with patch('time.monotonic', return_value=base + 120.0):
            assert tracker.should_scale_down(3) is True

    def test_finish_overwrites_blocked_entry(self):
        """record_task_finish replaces a blocked sentinel with a timestamp."""
        tracker = WorkerUsageTracker(scaledown_wait=10.0)
        tracker.fill_unknown_usage(worker_ct=3, running_ct=3)
        assert tracker.should_scale_down(3) is False
        # Task finishes — replaces blocked with timestamp
        base = 1000.0
        with patch('time.monotonic', return_value=base):
            tracker.record_task_finish(3)
        # Still blocked by recency
        with patch('time.monotonic', return_value=base):
            assert tracker.should_scale_down(3) is False
        # After scaledown_wait
        with patch('time.monotonic', return_value=base + 11.0):
            assert tracker.should_scale_down(3) is True

    def test_finish_enables_earlier_scale_down_than_periodic_fill(self):
        """record_task_finish sets the timestamp immediately, so scale-down
        can happen sooner than waiting for the next periodic fill."""
        tracker = WorkerUsageTracker(scaledown_wait=10.0)
        base = 1000.0
        # Task finishes at time=base, setting timestamp for key 5
        with patch('time.monotonic', return_value=base):
            tracker.record_task_finish(5)
        # Periodic fill at base+5 — should NOT overwrite the older timestamp
        with patch('time.monotonic', return_value=base + 5.0):
            tracker.fill_unknown_usage(worker_ct=5, running_ct=0)
        # At base+11, the record_task_finish timestamp is old enough
        with patch('time.monotonic', return_value=base + 11.0):
            assert tracker.should_scale_down(5) is True


class TestFillAndScaleDown:
    def test_absent_key_allows_scale_down(self):
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        assert tracker.should_scale_down(5) is True

    def test_active_load_blocks_scale_down(self):
        """Keys at or below running_ct are blocked."""
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        tracker.fill_unknown_usage(worker_ct=3, running_ct=3)
        assert tracker.should_scale_down(3) is False

    def test_idle_surplus_blocks_until_scaledown_wait(self):
        """Keys above running_ct get a fresh timestamp — blocked until scaledown_wait passes."""
        tracker = WorkerUsageTracker(scaledown_wait=10.0)
        base = 1000.0
        with patch('time.monotonic', return_value=base):
            tracker.fill_unknown_usage(worker_ct=3, running_ct=0)
        with patch('time.monotonic', return_value=base):
            assert tracker.should_scale_down(3) is False
        with patch('time.monotonic', return_value=base + 11.0):
            assert tracker.should_scale_down(3) is True

    def test_scaledown_wait_boundary(self):
        tracker = WorkerUsageTracker(scaledown_wait=10.0)
        base = 1000.0
        with patch('time.monotonic', return_value=base):
            tracker.fill_unknown_usage(worker_ct=3, running_ct=0)
        # Just under the wait threshold — should block
        with patch('time.monotonic', return_value=base + 9.0):
            assert tracker.should_scale_down(3) is False
        # Just over the wait threshold — should allow
        with patch('time.monotonic', return_value=base + 11.0):
            assert tracker.should_scale_down(3) is True

    def test_blocked_entries_refresh_when_still_active(self):
        """Repeated fills with same running_ct keep entries blocked."""
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        tracker.fill_unknown_usage(worker_ct=3, running_ct=3)
        assert tracker.should_scale_down(3) is False
        # Second fill, still loaded
        tracker.fill_unknown_usage(worker_ct=3, running_ct=3)
        assert tracker.should_scale_down(3) is False

    def test_blocked_entries_convert_to_timestamp_when_idle(self):
        """When running_ct drops, previously blocked entries above it
        are replaced with a timestamp and start aging."""
        tracker = WorkerUsageTracker(scaledown_wait=10.0)
        base = 1000.0
        # First fill: all 3 keys blocked
        tracker.fill_unknown_usage(worker_ct=3, running_ct=3)
        assert tracker.should_scale_down(3) is False

        # Second fill: running_ct dropped, key 3 was blocked but now idle
        with patch('time.monotonic', return_value=base):
            tracker.fill_unknown_usage(worker_ct=3, running_ct=1)
        # Key 3 now has a timestamp, but too recent
        with patch('time.monotonic', return_value=base):
            assert tracker.should_scale_down(3) is False
        # After scaledown_wait, key 3 allows scale-down
        with patch('time.monotonic', return_value=base + 11.0):
            assert tracker.should_scale_down(3) is True
        # Key 1 is still blocked (at running_ct)
        with patch('time.monotonic', return_value=base + 11.0):
            assert tracker.should_scale_down(1) is False

    def test_existing_timestamps_not_overwritten_by_fill(self):
        """Timestamps above running_ct are preserved so they keep aging."""
        tracker = WorkerUsageTracker(scaledown_wait=10.0)
        base = 1000.0
        with patch('time.monotonic', return_value=base):
            tracker.fill_unknown_usage(worker_ct=3, running_ct=0)
        # Second fill much later — timestamps should NOT be refreshed
        with patch('time.monotonic', return_value=base + 100.0):
            tracker.fill_unknown_usage(worker_ct=3, running_ct=0)
        # Original timestamps are preserved, so they're old enough
        with patch('time.monotonic', return_value=base + 11.0):
            assert tracker.should_scale_down(3) is True

    def test_different_worker_counts_independent(self):
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        tracker.fill_unknown_usage(worker_ct=2, running_ct=2)
        assert tracker.should_scale_down(2) is False
        assert tracker.should_scale_down(3) is True

    def test_scaledown_reserve_blocks_within_buffer(self):
        """With scaledown_reserve=2, worker_ct 6 with running_ct 4 should not scale down (6 <= 4+2)."""
        tracker = WorkerUsageTracker(scaledown_wait=10.0, scaledown_reserve=2)
        base = 1000.0
        with patch('time.monotonic', return_value=base):
            tracker.fill_unknown_usage(worker_ct=6, running_ct=4)
        # After scaledown_wait, worker_ct=6 is within reserve (6 <= 4+2)
        with patch('time.monotonic', return_value=base + 11.0):
            assert tracker.should_scale_down(6, running_ct=4) is False
        # Absent key still returns True (absent overrides reserve)
        with patch('time.monotonic', return_value=base + 11.0):
            assert tracker.should_scale_down(7, running_ct=4) is True

    def test_scaledown_reserve_allows_beyond_buffer(self):
        """With scaledown_reserve=2, worker_ct above running_ct+reserve can scale down."""
        tracker = WorkerUsageTracker(scaledown_wait=10.0, scaledown_reserve=2)
        base = 1000.0
        with patch('time.monotonic', return_value=base):
            tracker.fill_unknown_usage(worker_ct=8, running_ct=4)
        with patch('time.monotonic', return_value=base + 11.0):
            assert tracker.should_scale_down(8, running_ct=4) is True  # 8 > 4+2
            assert tracker.should_scale_down(7, running_ct=4) is True  # 7 > 4+2
            assert tracker.should_scale_down(6, running_ct=4) is False  # 6 <= 4+2

    def test_scaledown_reserve_zero_default(self):
        """With default scaledown_reserve=0, behavior matches existing tests."""
        tracker = WorkerUsageTracker(scaledown_wait=10.0)
        assert tracker.scaledown_reserve == 0
        base = 1000.0
        with patch('time.monotonic', return_value=base):
            tracker.fill_unknown_usage(worker_ct=3, running_ct=0)
        with patch('time.monotonic', return_value=base + 11.0):
            assert tracker.should_scale_down(3) is True

    def test_stale_blocked_entries_above_worker_ct_downgraded(self):
        """Keys above worker_ct left over from a previously higher worker count
        should be downgraded from blocked to a timestamp so they age out."""
        tracker = WorkerUsageTracker(scaledown_wait=10.0)
        base = 1000.0
        # Pool was at 5 workers, all busy
        with patch('time.monotonic', return_value=base):
            tracker.fill_unknown_usage(worker_ct=5, running_ct=5)
        # All 5 keys blocked
        for k in range(1, 6):
            assert tracker.should_scale_down(k) is False

        # Pool scaled down to 2 workers, none busy
        with patch('time.monotonic', return_value=base + 1.0):
            tracker.fill_unknown_usage(worker_ct=2, running_ct=0)
        # Keys 3-5 were above worker_ct and stale blocked, now timestamps
        with patch('time.monotonic', return_value=base + 1.0):
            for k in range(3, 6):
                assert tracker._last_used_by_ct[k] is not tracker._SCALE_DOWN_BLOCKED


class TestStatusData:
    def test_empty_tracker(self):
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        data = tracker.get_status_data(worker_ct=3)
        assert data["count"] == 0
        assert data["top5"] == {}
        assert set(data["near_worker_ct"].keys()) == {1, 2, 3, 4, 5}
        for v in data["near_worker_ct"].values():
            assert v == "absent"

    def test_mixed_entries(self):
        """Fill with running_ct=3 to get a mix of blocked and timestamp entries."""
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        base = 1000.0
        with patch('time.monotonic', return_value=base):
            tracker.fill_unknown_usage(worker_ct=6, running_ct=3)

        with patch('time.monotonic', return_value=base + 100.0):
            data = tracker.get_status_data(worker_ct=4)

        assert data["count"] == 6
        # Top 5 highest keys: 6, 5, 4, 3, 2
        assert set(data["top5"].keys()) == {2, 3, 4, 5, 6}
        assert data["top5"][2] == "blocked"
        assert data["top5"][3] == "blocked"
        assert isinstance(data["top5"][4], float)
        assert isinstance(data["top5"][5], float)
        assert isinstance(data["top5"][6], float)
        # Near worker_ct=4: keys [2..6]
        near = data["near_worker_ct"]
        assert set(near.keys()) == {2, 3, 4, 5, 6}
        assert near[2] == "blocked"
        assert near[3] == "blocked"
        assert isinstance(near[4], float)
        assert isinstance(near[5], float)
        assert isinstance(near[6], float)

    def test_near_worker_ct_absent_keys(self):
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        tracker.fill_unknown_usage(worker_ct=2, running_ct=0)
        data = tracker.get_status_data(worker_ct=8)
        near = data["near_worker_ct"]
        assert set(near.keys()) == {6, 7, 8, 9, 10}
        for v in near.values():
            assert v == "absent"

    def test_top5_returns_highest_keys(self):
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        tracker.fill_unknown_usage(worker_ct=8, running_ct=0)
        data = tracker.get_status_data(worker_ct=5)
        assert set(data["top5"].keys()) == {4, 5, 6, 7, 8}
