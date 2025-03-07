# src/pyplinefy/pipeline_manager.py
import asyncio
import logging
from .dynamic_queue_manager import DynamicQueueManager
from .queue_interface import QueueInterface

# A unique sentinel to signal shutdown gracefully.
_SENTINEL = object()

class ManagedPipeline:
    """
    Fully managed asynchronous pipeline that automatically creates queues and worker tasks.
    
    Users only need to provide a list of async functions as stages.
    This pipeline supports:
      - Configurable concurrency (number of worker tasks per stage).
      - Graceful shutdown via a sentinel value that propagates through all stages.
      - Robust error handling (errors are logged and processing continues).
      - Logging of queue sizes for monitoring.
    """
    def __init__(self, stage_funcs: list, concurrency=1, maxsize: int = 0):
        """
        Args:
            stage_funcs (list): List of async functions for each stage.
            concurrency (int or list): Number of workers per stage. If an int, it applies to all stages.
            maxsize (int): Maximum size for each queue (0 means unlimited).
        """
        self.queue_manager = DynamicQueueManager(maxsize=maxsize)
        self.queue_interface = QueueInterface(self.queue_manager)
        self.stage_funcs = stage_funcs

        # Normalize concurrency into a list (one entry per stage).
        if isinstance(concurrency, int):
            self.concurrency = [concurrency] * len(stage_funcs)
        else:
            if len(concurrency) != len(stage_funcs):
                raise ValueError("Length of concurrency list must match number of stages")
            self.concurrency = concurrency

        self.pipeline_tasks = []
        # Automatically create queue keys; note that we need one more queue than stages.
        self.queue_keys = [f"stage_{i}" for i in range(len(stage_funcs) + 1)]
        for key in self.queue_keys:
            self.queue_manager.get_queue(key)

        # Launch worker tasks for each stage with configurable concurrency.
        for i, stage_func in enumerate(stage_funcs):
            in_key = self.queue_keys[i]
            out_key = self.queue_keys[i + 1]
            num_workers = self.concurrency[i]
            for _ in range(num_workers):
                task = asyncio.create_task(self._stage_worker(in_key, stage_func, out_key, stage_index=i))
                self.pipeline_tasks.append(task)

    async def _stage_worker(self, in_key: str, stage_func, out_key: str, stage_index: int):
        """
        Worker task for processing items in a stage.
        Gracefully shuts down upon receiving the _SENTINEL.
        """
        while True:
            item = await self.queue_interface.get(in_key)
            if item is _SENTINEL:
                # Propagate the shutdown signal to downstream stages.
                await self.queue_interface.put(out_key, _SENTINEL)
                logging.info(f"Worker in stage {stage_index} received shutdown signal.")
                break
            try:
                result = await stage_func(item)
            except Exception as e:
                logging.error(f"Error processing item '{item}' in stage {stage_index}: {e}", exc_info=True)
                continue
            await self.queue_interface.put(out_key, result)

    async def add_data(self, data) -> None:
        """
        Feed initial data into the pipeline.
        """
        await self.queue_interface.put(self.queue_keys[0], data)

    async def get_result(self):
        """
        Retrieve a processed result from the final stage's output queue.
        If a shutdown sentinel is encountered, returns None.
        """
        result = await self.queue_interface.get(self.queue_keys[-1])
        if result is _SENTINEL:
            return None
        return result

    async def shutdown(self):
        """
        Gracefully shut down the pipeline by sending sentinel values to the first stage.
        These shutdown signals propagate through all stages, causing each worker to exit cleanly.
        """
        # Send as many sentinel values as there are workers in the first stage.
        num_workers = self.concurrency[0]
        for _ in range(num_workers):
            await self.queue_interface.put(self.queue_keys[0], _SENTINEL)
        # Wait for all worker tasks to finish.
        await asyncio.gather(*self.pipeline_tasks, return_exceptions=True)
        logging.info("Pipeline shutdown complete.")

    def log_queue_sizes(self) -> None:
        """
        Log the sizes of all queues in the pipeline.
        """
        self.queue_manager.log_queue_sizes()
