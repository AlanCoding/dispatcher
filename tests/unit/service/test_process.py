from multiprocessing import Queue
import os

import pytest

from dispatcher.process import ProcessManager, ForkServerManager, ProcessProxy


def test_pass_messages_to_worker():
    def work_loop(a, b, c, in_q, out_q):
        has_read = in_q.get()
        out_q.put(f'done {a} {b} {c} {has_read}')

    finished_q = Queue()
    process = ProcessProxy((1, 2, 3), finished_q, target=work_loop)
    process.start()

    process.message_queue.put('start')
    msg = finished_q.get()
    assert msg == 'done 1 2 3 start'


def work_loop2(var, in_q, out_q):
    """
    Due to the mechanics of forkserver, this can not be defined in local variables,
    it has to be importable, but this _is_ importable from the test module.
    """
    has_read = in_q.get()
    out_q.put(f'done {var} {has_read}')


@pytest.mark.parametrize('manager_cls', [ProcessManager, ForkServerManager])
def test_pass_messages_via_process_manager(manager_cls):
    process_manager = manager_cls()
    process = process_manager.create_process(('value',), target=work_loop2)
    process.start()

    process.message_queue.put('msg1')
    msg = process_manager.finished_queue.get()
    assert msg == 'done value msg1'


@pytest.mark.parametrize('manager_cls', [ProcessManager, ForkServerManager])
def test_workers_have_different_pid(manager_cls):
    process_manager = manager_cls()
    processes = [process_manager.create_process((f'value{i}',), target=work_loop2) for i in range(2)]

    for i in range(2):
        process = processes[i]
        process.start()
        process.message_queue.put(f'msg{i}')

    assert processes[0].pid != processes[1].pid  # title of test

    msg1 = process_manager.finished_queue.get()
    msg2 = process_manager.finished_queue.get()
    assert set([msg1, msg2]) == set(['done value1 msg1', 'done value0 msg0'])



def return_pid(in_q, out_q):
    out_q.put(f'{os.getpid()}')


@pytest.mark.parametrize('manager_cls', [ProcessManager, ForkServerManager])
def test_pid_is_correct(manager_cls):
    process_manager = manager_cls()
    process = process_manager.create_process((), target=return_pid)
    process.start()

    msg = process_manager.finished_queue.get()
    assert int(msg) == process.pid
