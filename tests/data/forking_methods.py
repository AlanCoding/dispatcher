import asyncio
import multiprocessing
import os
import subprocess
import sys
import threading

__all__ = [
    "thread_running_asyncio_loop",
    "asyncio_then_spawn",
    "thread_then_spawn",
    "asyncio_thread_nested_event_loop",
    "thread_then_fork_multiprocessing",
    "nested_process_pool",
    "signal_handling_asyncio",
    "asyncio_then_fork_inside_asyncio"
]


def thread_running_asyncio_loop():
    def run_loop():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(asyncio.sleep(0.1))
        loop.close()

    t = threading.Thread(target=run_loop)
    t.start()
    t.join()


def asyncio_then_spawn():
    async def main():
        proc = await asyncio.create_subprocess_exec(sys.executable, "-c", "print('hello from subprocess')", stdout=subprocess.PIPE)
        stdout, _ = await proc.communicate()
        assert b"hello" in stdout

    asyncio.run(main())


def thread_then_spawn():
    def target():
        subprocess.run([sys.executable, "-c", "print('subprocess in thread')"], check=True)

    t = threading.Thread(target=target)
    t.start()
    t.join()


def asyncio_thread_nested_event_loop():
    async def main():
        def thread_target():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(asyncio.sleep(0.1))
            loop.close()

        t = threading.Thread(target=thread_target)
        t.start()
        t.join()

    asyncio.run(main())


def thread_then_fork_multiprocessing():
    def worker():
        print("worker in subprocess")

    def thread_target():
        ctx = multiprocessing.get_context("fork")
        p = ctx.Process(target=worker)
        p.start()
        p.join()

    t = threading.Thread(target=thread_target)
    t.start()
    t.join()


def sub_worker(x):
    return x * x


def nested_process_pool():
    ctx = multiprocessing.get_context("forkserver")
    with ctx.Pool(2) as pool:
        results = pool.map(sub_worker, [1, 2, 3])
        assert results == [1, 4, 9]


def signal_handling_asyncio():
    import signal

    received = []

    def handler(signum, frame):
        received.append(signum)

    signal.signal(signal.SIGUSR1, handler)

    async def main():
        os.kill(os.getpid(), signal.SIGUSR1)
        await asyncio.sleep(0.1)
        assert signal.SIGUSR1 in received

    asyncio.run(main())


def asyncio_then_fork_inside_asyncio():
    async def main():
        ctx = multiprocessing.get_context("fork")
        p = ctx.Process(target=lambda: print("this is forked inside asyncio"))
        p.start()
        p.join()
    asyncio.run(main())

