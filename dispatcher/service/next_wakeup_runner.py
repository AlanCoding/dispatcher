import asyncio
import logging
import time
from abc import abstractmethod
from typing import Any, Callable, Coroutine, Iterable, Optional

from .asyncio_tasks import ensure_fatal

logger = logging.getLogger(__name__)


class HasWakeup:
    """A mixin to indicate that this class gives a future timestamp of when a call is needed"""

    @abstractmethod
    def next_wakeup(self) -> Optional[float]:
        """The next time that we need to call the callback for, outline of contract:

        return None - no need to call the callback
        return positive value - however long we need to wait before calling the callback
        return zero of negative value - callback needs to be called right away
        """
        ...


class NextWakeupRunner:
    """Implements a general contract to wakeup for next timestamp of a set of objects

    For example, you have a set of schedules, each with a given period.
    You want to run each on their period - this does that using one lazy asyncio task.
    This is a repeated pattern in the code base with task schedules, delays, and timeouts

    Arguments:
     - wakeup_objects: an iterable of objects with an associated wakeup
     - process_object: and async callback that takes a an object, will be called when wakeup happens for that object
    """

    def __init__(self, wakeup_objects: Iterable[HasWakeup], process_object: Callable[[Any], Coroutine[Any, Any, None]], name: Optional[str] = None) -> None:
        self.wakeup_objects = wakeup_objects
        self.process_object = process_object
        self.asyncio_task: Optional[asyncio.Task] = None
        self.kick_event = asyncio.Event()
        self.shutting_down: bool = False
        if name is None:
            method_name = getattr(process_object, '__name__', str(process_object))
            self.name = f'next-run-manager-of-{method_name}'
        else:
            self.name = name

    async def process_wakeups(self, current_time: float, do_processing: bool = True) -> Optional[float]:
        """Runs process_object for objects past for which we have passed the wakeup time

        Returns the time of the soonest wakeup that has not been processed here

        Arguments:
         - current_time - output of time.monotonic() passed from caller to keep this deterministic
         - do_processing - to help optimizations, False value allows checking next wakeup time
           without calling the callback for anything
        """
        future_wakeup = None
        for obj in self.wakeup_objects:
            if obj_wakeup := obj.next_wakeup():
                if do_processing and (obj_wakeup < current_time):
                    await self.process_object(obj)
                    # refresh wakeup, which should be nullified or pushed back by process_object
                    obj_wakeup = obj.next_wakeup()
                    if obj_wakeup is None:
                        continue
                if (future_wakeup is None) or obj_wakeup < future_wakeup:
                    future_wakeup = obj_wakeup
        return future_wakeup

    async def background_task(self) -> None:
        while not self.shutting_down:
            now_time = time.monotonic()
            next_wakeup = await self.process_wakeups(now_time)
            if next_wakeup is None:
                return

            delta = next_wakeup - now_time
            if delta <= 0.0:
                logger.info(f'Wakeup processor {self.name} has leftover wakeups, {delta}s in past, sleeping for 0.1s')
                delta = 0.1

            try:
                await asyncio.wait_for(self.kick_event.wait(), timeout=delta)
            except asyncio.TimeoutError:
                pass  # intended mechanism to hit the next schedule
            except asyncio.CancelledError:
                logger.info(f'Task {self.name} cancelled, returning')
                return

            self.kick_event.clear()

    def mk_new_task(self) -> None:
        """Should only be called if a task is not currently running"""
        self.asyncio_task = asyncio.create_task(self.background_task(), name=self.name)
        ensure_fatal(self.asyncio_task)

    def kick(self) -> None:
        """Initiates the asyncio task to wake up at the next run time

        This needs to be called if objects in wakeup_objects are changed, for example
        """
        if self.process_wakeups(current_time=time.monotonic(), do_processing=False) is None:
            # Optimization here, if there is no next time, do not bother managing tasks
            return
        if self.asyncio_task:
            if self.asyncio_task.done():
                self.mk_new_task()
            else:
                self.kick_event.set()
        else:
            self.mk_new_task()

    def all_tasks(self) -> list[asyncio.Task]:
        if self.asyncio_task:
            return [self.asyncio_task]
        return []

    async def shutdown(self):
        self.shutting_down = True
        self.kick()
