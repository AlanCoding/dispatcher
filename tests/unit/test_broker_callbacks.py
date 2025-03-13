import json
import logging
import pytest

from dispatcher.control import BrokerCallbacks
from dispatcher.protocols import Broker


# Dummy broker that yields first an invalid JSON message and then a valid one.
class DummyBroker(Broker):
    async def aprocess_notify(self, connected_callback=None):
        if connected_callback:
            await connected_callback()
        # First yield an invalid JSON string, then a valid one.
        yield ("reply_channel", "invalid json")
        yield ("reply_channel", json.dumps({"result": "ok"}))

    async def apublish_message(self, channel, message):
        # No-op for testing.
        return

    async def aclose(self):
        return

    def process_notify(self, connected_callback=None, timeout: float = 5.0, max_messages: int = 1):
        # Not used in this test.
        yield ("reply_channel", "")

    def publish_message(self, channel=None, message=None):
        return

    def close(self):
        return


@pytest.mark.asyncio
async def test_listen_for_replies_with_invalid_json(caplog):
    caplog.set_level(logging.WARNING)
    dummy_broker = DummyBroker()
    callbacks = BrokerCallbacks(
        queuename="reply_channel",
        broker=dummy_broker,
        send_message="{}",
        expected_replies=1
    )
    await callbacks.listen_for_replies()
    # The invalid JSON should be ignored and only the valid message appended.
    assert len(callbacks.received_replies) == 1
    assert callbacks.received_replies[0] == {"result": "ok"}
    # Verify that a warning was logged for the malformed message.
    assert any("Invalid JSON" in record.message for record in caplog.records)
