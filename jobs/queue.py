import asyncio
import logging
from typing import Any, Callable

logger = logging.getLogger("jobs.queue")


class JobQueue:
    def __init__(self, workers: int = 4):
        self.queue: asyncio.Queue = asyncio.Queue()
        self.workers = workers
        self._started = False

    async def enqueue(self, func: Callable, *args, **kwargs):
        await self.queue.put((func, args, kwargs))

    async def worker(self):
        while True:
            func, args, kwargs = await self.queue.get()
            try:
                result = func(*args, **kwargs)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error("[JOB] Failed: %s: %s", getattr(func, "__name__", "unknown"), e)
            finally:
                self.queue.task_done()

    async def start(self):
        if self._started:
            return
        self._started = True
        for _ in range(self.workers):
            asyncio.create_task(self.worker())


job_queue = JobQueue(workers=4)
