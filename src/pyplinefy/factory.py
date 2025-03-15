# src/pyplinefy/factory.py
from .queue_manager import QueueManager, AutoQueueManager
from .queue_interface import QueueInterface

def create_queue_interface(queue_keys: list = None, maxsize: int = 0, auto: bool = False) -> QueueInterface:
    """
    Creates and returns a QueueInterface instance with an associated QueueManager.

    Args:
        queue_keys (list, optional): A list of queue names to pre-create.
        maxsize (int, optional): The maximum size for each queue (default: 0, unlimited).
        auto (bool, optional): If True, uses AutoQueueManager (implicit queue creation).
                               If False, uses QueueManager (explicit queue creation).

    Returns:
        QueueInterface: An instance of QueueInterface with an internally managed QueueManager.
    """
    queue_manager_cls = AutoQueueManager if auto else QueueManager
    queue_manager = queue_manager_cls(maxsize=maxsize)

    # Pre-create queues if `queue_keys` are provided
    if queue_keys:
        for key in queue_keys:
            queue_manager.create_queue(key)

    return QueueInterface(queue_manager)