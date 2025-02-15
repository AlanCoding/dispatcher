from multiprocessing import Queue

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
