# src/pyplinefy/queue_manager.py
import asyncio
import logging

class QueueManager:
    """
    Manages multiple asyncio queues, creating them on-demand.
    Provides methods for retrieving, removing, listing, and logging queue sizes.
    """
    def __init__(self, maxsize: int = 0):
        self.queues = {}
        self.maxsize = maxsize

    def create_queue(self, key: str) -> None:
        """
        Explicitly create a new queue with the given key.
        Raises an error if the queue already exists.
        """
        if key in self.queues:
            raise ValueError(f"Queue '{key}' already exists.")
        self.queues[key] = asyncio.Queue(maxsize=self.maxsize)
        logging.info(f"Queue '{key}' created.")

    def get_queue(self, key: str) -> asyncio.Queue:
        """
        Retrieve an existing queue. Raises an error if the queue does not exist.
        """
        if key not in self.queues:
            raise KeyError(f"Queue '{key}' does not exist. Call `create_queue('{key}')` first.")
        return self.queues[key]

    def remove_queue(self, key: str) -> None:
        """
        Remove a queue from the manager.
        """
        if key in self.queues:
            del self.queues[key]
            logging.info(f"Queue '{key}' removed.")
    def exists(self, key: str) -> bool:
        """Checks if a queue exists."""
        return key in self.queues


    def list_queues(self) -> list:
        """
        Get a list of all queue names.
        """
        return list(self.queues.keys())

    def get_queue_sizes(self) -> dict:
        """
        Get the size of all queues.
        """
        return {key: queue.qsize() for key, queue in self.queues.items()}

    def log_queue_sizes(self) -> None:
        """
        Log the sizes of all queues.
        """
        sizes = self.get_queue_sizes()
        logging.info("Current queue sizes:")
        for key, size in sizes.items():
            logging.info(f"  {key}: {size}")


class AutoQueueManager(QueueManager):
    """
    Similar to QueueManager, but automatically creates a queue if get_queue() is called on a queue that does not exist.
    """
    def get_queue(self, key: str) -> asyncio.Queue:
        """
        Retrieve an existing queue. If the queue does not exist, it is automatically created.
        """
        if key not in self.queues:
            self.create_queue(key)  # Calls parent method to create a queue
        return self.queues[key]