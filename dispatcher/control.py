import asyncio
import json
import logging
import time
import uuid
from typing import Optional, Union

from .factories import get_broker
from .protocols import Broker
from .service.asyncio_tasks import ensure_fatal

logger = logging.getLogger('awx.main.dispatch.control')


class BrokerCallbacks:
    def __init__(self, queuename: Optional[str], broker: Broker, send_data: dict, expected_replies: int = 1) -> None:
        self.received_replies: list = []
        self.queuename = queuename
        self.broker = broker
        self.send_message = json.dumps(send_data)
        self.expected_replies = expected_replies

    async def connected_callback(self) -> None:
        await self.broker.apublish_message(self.queuename, self.send_message)

    async def listen_for_replies(self) -> None:
        async for channel, payload in self.broker.aprocess_notify(connected_callback=self.connected_callback):
            self.received_replies.append(payload)
            if len(self.received_replies) >= self.expected_replies:
                return


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

    def make_broker(self, reply_queue: Optional[str] = None) -> Broker:
        if reply_queue:
            channels = [reply_queue]
        else:
            channels = []
        return get_broker(self.broker_name, self.broker_config, channels=channels)

    async def acontrol_with_reply(self, command: str, expected_replies: int = 1, timeout: int = 1, data: Optional[dict] = None) -> list[dict]:
        reply_queue = Control.generate_reply_queue_name()
        send_data: dict[str, Union[dict, str]] = {'control': command, 'reply_to': reply_queue}
        if data:
            send_data['control_data'] = data

        broker = self.make_broker(reply_queue)
        control_callbacks = BrokerCallbacks(broker=broker, queuename=self.queuename, send_data=send_data, expected_replies=expected_replies)

        listen_task = asyncio.create_task(control_callbacks.listen_for_replies())
        ensure_fatal(listen_task)

        try:
            await asyncio.wait_for(listen_task, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f'Did not receive {expected_replies} reply in {timeout} seconds, only {len(control_callbacks.received_replies)}')
            listen_task.cancel()

        return self.parse_replies(control_callbacks.received_replies)

    async def acontrol(self, command: str, data: Optional[dict] = None) -> None:
        send_data: dict[str, Union[dict, str]] = {'control': command}
        if data:
            send_data['control_data'] = data

        broker = self.make_broker()
        send_message = json.dumps(send_data)
        await broker.apublish_message(message=send_message)

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
