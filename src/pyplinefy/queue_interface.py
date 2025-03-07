# src/pyplinefy/queue_interface.py
import asyncio
import logging
from .dynamic_queue_manager import DynamicQueueManager

class QueueInterface:
    """
    Provides asynchronous methods for putting and getting items from queues
    managed by DynamicQueueManager.
    """
    def __init__(self, queue_manager: DynamicQueueManager):
        self.queue_manager = queue_manager

    async def put(self, key: str, item) -> None:
        queue = self.queue_manager.get_queue(key)
        await queue.put(item)
        logging.debug(f"Put item '{item}' into queue '{key}'.")

    async def get(self, key: str):
        queue = self.queue_manager.get_queue(key)
        item = await queue.get()
        queue.task_done()
        logging.debug(f"Got item '{item}' from queue '{key}'.")
        return item

    async def join_queue(self, key: str) -> None:
        queue = self.queue_manager.get_queue(key)
        await queue.join()
        logging.info(f"All tasks in queue '{key}' have been processed.")
