# src/pyplinefy/queue_interface.py
import asyncio
import logging
from .queue_manager import QueueManager

class QueueInterface:
    """
    Provides asynchronous methods for interacting with queues managed by QueueManager.
    This class simplifies adding (`put`) and retrieving (`get`) items from named asyncio queues.
    
    It ensures safe queue access by checking for queue existence before performing operations.
    """

    def __init__(self, queue_manager: QueueManager):
        """
        Initializes the QueueInterface with a QueueManager instance.

        Args:
            queue_manager (QueueManager): The QueueManager instance responsible for managing the queues.
        """
        self.queue_manager = queue_manager

    async def put(self, key: str, item) -> None:
        """
        Adds an item to the specified queue.

        Args:
            key (str): The name of the queue.
            item (Any): The item to be added to the queue.

        Raises:
            KeyError: If the specified queue does not exist (for explicit QueueManager).
        """
        try:
            queue = self.queue_manager.get_queue(key)
        except KeyError:
            raise KeyError(f"Queue '{key}' does not exist. Ensure it is created before using put().")
        await queue.put(item)
        logging.debug(f"Put item '{item}' into queue '{key}'.")

    async def get(self, key: str):
        """
        Retrieves an item from the specified queue.

        Args:
            key (str): The name of the queue.

        Returns:
            Any: The next item from the queue.

        Raises:
            KeyError: If the specified queue does not exist (for explicit QueueManager).
        """
        try:
            queue = self.queue_manager.get_queue(key)
        except KeyError:
            raise KeyError(f"Queue '{key}' does not exist. Ensure it is created before using get().")
        item = await queue.get()
        queue.task_done()
        logging.debug(f"Got item '{item}' from queue '{key}'.")
        return item

    async def join_queue(self, key: str) -> None:
        """
        Blocks until all tasks in the specified queue have been processed.

        Args:
            key (str): The name of the queue.

        Raises:
            KeyError: If the specified queue does not exist (for explicit QueueManager).
        """
        try:
            queue = self.queue_manager.get_queue(key)
        except KeyError:
            raise KeyError(f"Queue '{key}' does not exist. Ensure it is created before using join_queue().")
        await queue.join()
        logging.info(f"All tasks in queue '{key}' have been processed.")