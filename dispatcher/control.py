import asyncio
import json
import logging
import time
import uuid
from typing import Optional, Union

from .factories import get_broker
from .producers import BrokeredProducer
from .protocols import Producer
from .service.asyncio_tasks import ensure_fatal

logger = logging.getLogger('awx.main.dispatch.control')


class ControlEvents:
    def __init__(self) -> None:
        self.exit_event = asyncio.Event()


class ControlCallbacks:
    """This calls follows the same structure as the DispatcherMain class

    it exists to interact with producers, using variables relevant to the particular
    control message being sent"""

    def __init__(self, queuename: Optional[str], send_data: dict, expected_replies: int) -> None:
        self.queuename = queuename
        self.send_data = send_data
        self.expected_replies = expected_replies

        # received_replies only tracks the reply message, not the channel name
        # because they come via a temporary reply_to channel and that is not user-facing
        self.received_replies: list[Union[dict, str]] = []
        self.events = ControlEvents()
        self.shutting_down = False

    async def process_message(
        self, payload: Union[dict, str], producer: Optional[Producer] = None, channel: Optional[str] = None
    ) -> tuple[Optional[str], Optional[str]]:
        self.received_replies.append(payload)
        if self.expected_replies and (len(self.received_replies) >= self.expected_replies):
            self.events.exit_event.set()
        return (None, None)

    async def connected_callback(self, producer: Producer) -> None:
        payload = json.dumps(self.send_data)
        # Ignore the type hint here because we know it is a brokered producer
        await producer.notify(channel=self.queuename, message=payload)  # type: ignore[attr-defined]
        logger.info('Sent control message, expecting replies soon')

    async def main(self) -> None:
        "Unused"
        pass


class Control(object):
    def __init__(self, broker_name: str, broker_config: dict, queue: Optional[str] = None) -> None:
        self.queuename = queue
        self.broker_name = broker_name
        self.broker_config = broker_config

    @classmethod
    def generate_reply_queue_name(cls) -> str:
        return f"reply_to_{str(uuid.uuid4()).replace('-', '_')}"

    @staticmethod
    def parse_replies(received_replies: list[Union[str, dict]]) -> list[dict]:
        ret = []
        for payload in received_replies:
            if isinstance(payload, dict):
                ret.append(payload)
            else:
                ret.append(json.loads(payload))
        return ret

    async def acontrol_with_reply_internal(self, producer: Producer, send_data: dict, expected_replies: int, timeout: float) -> list[dict]:
        control_callbacks = ControlCallbacks(self.queuename, send_data, expected_replies)

        await producer.start_producing(control_callbacks)
        for task in producer.all_tasks():
            # Make sure we catch errors
            ensure_fatal(task, exit_event=control_callbacks.events.exit_event)

        await producer.events.ready_event.wait()

        try:
            await asyncio.wait_for(control_callbacks.events.exit_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f'Did not receive {expected_replies} reply in {timeout} seconds, only {len(control_callbacks.received_replies)}')

        control_callbacks.shutting_down = True
        await producer.shutdown()

        return self.parse_replies(control_callbacks.received_replies)

    def make_producer(self, reply_queue: str) -> Producer:
        broker = get_broker(self.broker_name, self.broker_config, channels=[reply_queue])
        return BrokeredProducer(broker, close_on_exit=True)

    async def acontrol_with_reply(self, command: str, expected_replies: int = 1, timeout: int = 1, data: Optional[dict] = None) -> list[dict]:
        reply_queue = Control.generate_reply_queue_name()
        send_data: dict[str, Union[dict, str]] = {'control': command, 'reply_to': reply_queue}
        if data:
            send_data['control_data'] = data

        return await self.acontrol_with_reply_internal(self.make_producer(reply_queue), send_data, expected_replies, timeout)

    async def acontrol(self, command: str, data: Optional[dict] = None) -> None:
        send_data: dict[str, Union[dict, str]] = {'control': command}
        if data:
            send_data['control_data'] = data

        control_callbacks = ControlCallbacks(self.queuename, send_data, 0)
        producer = self.make_producer(Control.generate_reply_queue_name())  # reply queue not used
        await control_callbacks.connected_callback(producer)

    def control_with_reply(self, command: str, expected_replies: int = 1, timeout: float = 1.0, data: Optional[dict] = None) -> list[dict]:
        logger.info('control-and-reply {} to {}'.format(command, self.queuename))
        start = time.time()
        reply_queue = Control.generate_reply_queue_name()
        send_data: dict[str, Union[dict, str]] = {'control': command, 'reply_to': reply_queue}
        if data:
            send_data['control_data'] = data

        broker = get_broker(self.broker_name, self.broker_config, channels=[reply_queue])

        def connected_callback() -> None:
            payload = json.dumps(send_data)
            if self.queuename:
                broker.publish_message(channel=self.queuename, message=payload)
            else:
                broker.publish_message(message=payload)

        replies = []
        for channel, payload in broker.process_notify(connected_callback=connected_callback, max_messages=expected_replies, timeout=timeout):
            reply_data = json.loads(payload)
            replies.append(reply_data)

        logger.info(f'control-and-reply message returned in {time.time() - start} seconds')
        return replies

    def control(self, command: str, data: Optional[dict] = None) -> None:
        "Send message in fire-and-forget mode, as synchronous code. Only for no-reply control."
        send_data: dict[str, Union[dict, str]] = {'control': command}
        if data:
            send_data['control_data'] = data

        payload = json.dumps(send_data)
        broker = get_broker(self.broker_name, self.broker_config)
        broker.publish_message(channel=self.queuename, message=payload)
