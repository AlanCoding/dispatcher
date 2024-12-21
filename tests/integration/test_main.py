import asyncio
import json

import pytest

SLEEP_METHOD = 'lambda: __import__("time").sleep(0.1)'


@pytest.mark.asyncio
async def test_run_lambda_function(apg_dispatcher, pg_message):
    assert apg_dispatcher.pool.finished_count == 0

    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    pg_message('lambda: "This worked!"')
    await clearing_task

    assert apg_dispatcher.pool.finished_count == 1


@pytest.mark.asyncio
async def test_multiple_channels(apg_dispatcher, pg_message):
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    pg_message(SLEEP_METHOD, channel='test_channel')
    pg_message(SLEEP_METHOD, channel='test_channel2')
    pg_message(SLEEP_METHOD, channel='test_channel3')
    pg_message(SLEEP_METHOD, channel='test_channel4')  # not listening to this
    await clearing_task

    assert apg_dispatcher.pool.finished_count == 3


@pytest.mark.asyncio
async def test_ten_messages_queued(apg_dispatcher, pg_message):
    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    for i in range(15):
        pg_message(SLEEP_METHOD)
    await clearing_task

    assert apg_dispatcher.pool.finished_count == 15


@pytest.mark.asyncio
async def test_cancel_task(apg_dispatcher, pg_message, pg_control):
    msg = json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'foobar'})
    pg_message(msg)

    clearing_task = asyncio.create_task(apg_dispatcher.pool.events.work_cleared.wait())
    canceled_jobs = await pg_control.acontrol_with_reply('cancel', data={'uuid': 'foobar'})
    worker_id, canceled_message = canceled_jobs[0][0]
    await clearing_task

    assert canceled_message['uuid'] == 'foobar'

    pool = apg_dispatcher.pool
    assert [pool.finished_count, pool.canceled_count, pool.control_count] == [0, 1, 1]

    # print('')
    # print('finding a running task by its task name')
    # publish_message(channel, json.dumps({'task': 'lambda: __import__("time").sleep(3.1415)', 'uuid': 'foobar2'}), config={'conninfo': CONNECTION_STRING})
    # running_data = ctl.control_with_reply('running', data={'task': 'lambda: __import__("time").sleep(3.1415)'})
    # print(json.dumps(running_data, indent=2))

    # print('writing a message with a delay')
    # print('     4 second delay task')
    # publish_message(channel, json.dumps({'task': 'lambda: 123421', 'uuid': 'foobar2', 'delay': 4}), config={'conninfo': CONNECTION_STRING})
    # print('     30 second delay task')
    # publish_message(channel, json.dumps({'task': 'lambda: 987987234', 'uuid': 'foobar2', 'delay': 30}), config={'conninfo': CONNECTION_STRING})
    # print('     10 second delay task')
    # # NOTE: this task will error unless you run the dispatcher itself with it in the PYTHONPATH, which is intended
    # sleep_function.apply_async(
    #     args=[3],  # sleep 3 seconds
    #     delay=10,
    #     config={'conninfo': CONNECTION_STRING}
    # )

    # print('')
    # print('showing delayed tasks in running list')
    # running_data = ctl.control_with_reply('running', data={'task': 'test_methods.sleep_function'})
    # print(json.dumps(running_data, indent=2))

    # print('')
    # print('cancel a delayed task with no reply for demonstration')
    # ctl.control('cancel', data={'task': 'test_methods.sleep_function'})  # NOTE: no reply
    # print('confirmation that it has been canceled')
    # running_data = ctl.control_with_reply('running', data={'task': 'test_methods.sleep_function'})
    # print(json.dumps(running_data, indent=2))

    # print('')
    # print('running alive check a few times')
    # for i in range(3):
    #     alive = ctl.control_with_reply('alive')
    #     print(alive)

    # print('')
    # print('demo of submitting discarding tasks')
    # for i in range(10):
    #     publish_message(channel, json.dumps(
    #         {'task': 'lambda: __import__("time").sleep(9)', 'on_duplicate': 'discard', 'uuid': f'dscd-{i}'}
    #     ), config={'conninfo': CONNECTION_STRING})
    # print('demo of discarding task marked as discarding')
    # for i in range(10):
    #     sleep_discard.apply_async(args=[2], config={'conninfo': CONNECTION_STRING})
    # print('demo of discarding tasks with apply_async contract')
    # for i in range(10):
    #     sleep_function.apply_async(args=[3], on_duplicate='discard', config={'conninfo': CONNECTION_STRING})
    # print('demo of submitting waiting tasks')
    # for i in range(10):
    #     publish_message(channel, json.dumps(
    #         {'task': 'lambda: __import__("time").sleep(10)', 'on_duplicate': 'serial', 'uuid': f'wait-{i}'}
    #         ), config={'conninfo': CONNECTION_STRING})
    # print('demo of submitting queue-once tasks')
    # for i in range(10):
    #     publish_message(channel, json.dumps(
    #         {'task': 'lambda: __import__("time").sleep(8)', 'on_duplicate': 'queue_one', 'uuid': f'queue_one-{i}'}
    #     ), config={'conninfo': CONNECTION_STRING})