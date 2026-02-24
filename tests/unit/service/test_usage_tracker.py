# Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
# See also: asyncio_tests/unit/service/test_pool_scaledown.py for async integration tests
from unittest.mock import patch

from dispatcherd.service.pool import WorkerUsageTracker


class TestRecordAndScaleDown:
    def test_absent_key_allows_scale_down(self):
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        assert tracker.should_scale_down(5) is True

    def test_task_start_blocks_scale_down(self):
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        tracker.record_task_start(3)
        assert tracker.should_scale_down(3) is False

    def test_task_finish_recent_blocks_scale_down(self):
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        tracker.record_task_finish(3)
        assert tracker.should_scale_down(3) is False

    def test_task_finish_old_allows_scale_down(self):
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        base = 1000.0
        with patch('time.monotonic', return_value=base):
            tracker.record_task_finish(3)
        with patch('time.monotonic', return_value=base + 120.0):
            assert tracker.should_scale_down(3) is True

    def test_scaledown_wait_boundary(self):
        tracker = WorkerUsageTracker(scaledown_wait=10.0)
        base = 1000.0
        with patch('time.monotonic', return_value=base):
            tracker.record_task_finish(3)
        # Just under the wait threshold — should block
        with patch('time.monotonic', return_value=base + 9.0):
            assert tracker.should_scale_down(3) is False
        # Just over the wait threshold — should allow
        with patch('time.monotonic', return_value=base + 11.0):
            assert tracker.should_scale_down(3) is True

    def test_start_then_finish_transitions(self):
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        tracker.record_task_start(3)
        assert tracker.should_scale_down(3) is False
        # Finish replaces sentinel with fresh timestamp, still blocks due to recency
        tracker.record_task_finish(3)
        assert tracker.should_scale_down(3) is False

    def test_different_worker_counts_independent(self):
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        tracker.record_task_start(2)
        assert tracker.should_scale_down(2) is False
        assert tracker.should_scale_down(3) is True


class TestFillUnknownUsage:
    def test_gaps_below_running_ct_filled_as_blocked(self):
        """Missing keys at or below the current load are filled as blocked."""
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        tracker.record_task_start(1)
        tracker.record_task_start(3)
        # Key 2 is a gap — fill with running_ct=3
        tracker.fill_unknown_usage(worker_ct=3, running_ct=3)
        assert tracker.should_scale_down(2) is False  # filled as blocked

    def test_gaps_above_running_ct_filled_as_idle(self):
        """Missing keys above the current load are filled with a timestamp."""
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        tracker.record_task_start(1)
        # Keys 2-5 are absent — fill with running_ct=1
        tracker.fill_unknown_usage(worker_ct=5, running_ct=1)
        # Keys 2-5 were filled as idle (timestamp = now), too recent to scale down
        assert tracker.should_scale_down(2) is False
        assert tracker.should_scale_down(5) is False

    def test_does_not_overwrite_existing_entries(self):
        """Existing entries are preserved during fill."""
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        tracker.record_task_start(2)
        base = 1000.0
        with patch('time.monotonic', return_value=base):
            tracker.record_task_finish(3)
        # Fill — key 2 (blocked) and key 3 (timestamp) should be unchanged
        with patch('time.monotonic', return_value=base + 120.0):
            tracker.fill_unknown_usage(worker_ct=5, running_ct=1)
        assert tracker.should_scale_down(2) is False  # still blocked
        with patch('time.monotonic', return_value=base + 120.0):
            assert tracker.should_scale_down(3) is True  # still old timestamp

    def test_filled_idle_entries_allow_eventual_scale_down(self):
        """Idle-filled entries allow scale-down after scaledown_wait passes."""
        tracker = WorkerUsageTracker(scaledown_wait=10.0)
        base = 1000.0
        with patch('time.monotonic', return_value=base):
            tracker.fill_unknown_usage(worker_ct=3, running_ct=0)
        # Immediately after fill — too recent
        with patch('time.monotonic', return_value=base):
            assert tracker.should_scale_down(1) is False
        # After scaledown_wait — allowed
        with patch('time.monotonic', return_value=base + 11.0):
            assert tracker.should_scale_down(1) is True
            assert tracker.should_scale_down(2) is True
            assert tracker.should_scale_down(3) is True

    def test_is_tracked(self):
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        assert tracker.is_tracked(3) is False
        tracker.record_task_start(3)
        assert tracker.is_tracked(3) is True


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
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        base = 1000.0
        with patch('time.monotonic', return_value=base):
            tracker.record_task_finish(1)
        with patch('time.monotonic', return_value=base + 50.0):
            tracker.record_task_finish(2)
        tracker.record_task_start(3)
        with patch('time.monotonic', return_value=base + 90.0):
            tracker.record_task_finish(4)
        tracker.record_task_start(5)
        with patch('time.monotonic', return_value=base + 99.0):
            tracker.record_task_finish(6)

        with patch('time.monotonic', return_value=base + 100.0):
            data = tracker.get_status_data(worker_ct=4)

        assert data["count"] == 6
        # Top 5 highest keys: 6, 5, 4, 3, 2
        assert set(data["top5"].keys()) == {2, 3, 4, 5, 6}
        assert data["top5"][3] == "blocked"
        assert data["top5"][5] == "blocked"
        assert isinstance(data["top5"][2], float)
        assert isinstance(data["top5"][4], float)
        assert isinstance(data["top5"][6], float)
        # Near worker_ct=4: keys [2..6]
        near = data["near_worker_ct"]
        assert set(near.keys()) == {2, 3, 4, 5, 6}
        assert isinstance(near[2], float)
        assert near[3] == "blocked"
        assert isinstance(near[4], float)
        assert near[5] == "blocked"
        assert isinstance(near[6], float)

    def test_near_worker_ct_absent_keys(self):
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        tracker.record_task_finish(1)
        tracker.record_task_finish(2)
        data = tracker.get_status_data(worker_ct=8)
        near = data["near_worker_ct"]
        assert set(near.keys()) == {6, 7, 8, 9, 10}
        for v in near.values():
            assert v == "absent"

    def test_top5_returns_highest_keys(self):
        tracker = WorkerUsageTracker(scaledown_wait=15.0)
        for i in range(1, 9):
            tracker.record_task_finish(i)
        data = tracker.get_status_data(worker_ct=5)
        assert set(data["top5"].keys()) == {4, 5, 6, 7, 8}
