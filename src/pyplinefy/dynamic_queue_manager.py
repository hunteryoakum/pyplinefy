# src/pyplinefy/dynamic_queue_manager.py
import asyncio
import logging

class DynamicQueueManager:
    """
    Manages multiple asyncio queues, creating them on-demand.
    Provides methods for retrieving, removing, listing, and logging queue sizes.
    """
    def __init__(self, maxsize: int = 0):
        self.queues = {}
        self.maxsize = maxsize

    def get_queue(self, key: str) -> asyncio.Queue:
        if key not in self.queues:
            self.queues[key] = asyncio.Queue(maxsize=self.maxsize)
            logging.info(f"Queue '{key}' created.")
        return self.queues[key]

    def remove_queue(self, key: str) -> None:
        if key in self.queues:
            del self.queues[key]
            logging.info(f"Queue '{key}' removed.")

    def list_queues(self) -> list:
        return list(self.queues.keys())

    def get_queue_sizes(self) -> dict:
        return {key: queue.qsize() for key, queue in self.queues.items()}

    def log_queue_sizes(self) -> None:
        sizes = self.get_queue_sizes()
        logging.info("Current queue sizes:")
        for key, size in sizes.items():
            logging.info(f"  {key}: {size}")
