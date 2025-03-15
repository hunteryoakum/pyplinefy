# src/pyplinefy/__init__.py
from .queue_manager import QueueManager, AutoQueueManager
from .queue_interface import QueueInterface
from .managed_pipeline import ManagedPipeline, StageWorker
from .factory import create_queue_interface

__all__ = [
    "QueueManager",
    "AutoQueueManager",
    "QueueInterface",
    "ManagedPipeline",
    "StageWorker",
    "create_queue_interface",
]

