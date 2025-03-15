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

    Users may provide the pipeline stages either as:
      - A list of async functions, in which case queues are auto-named (e.g. "stage_0", "stage_1", ...)
      - A dict mapping names to async functions, so you can name the queues. In this case,
        the insertion order is used, and an extra final output queue (named "final") is appended.

    The pipeline supports:
      - Configurable concurrency (number of worker tasks per stage).
      - Graceful shutdown via a sentinel value that propagates through all stages.
      - Immediate shutdown that cancels tasks and discards pending work.
      - Robust error handling (errors are logged and processing continues).
      - Logging of queue sizes for monitoring.
    """
    def __init__(self, stage_funcs, concurrency=1, maxsize: int = 0):
        """
        Args:
            stage_funcs (list or dict): Either a list of async functions or a dict mapping queue names
                                        to async functions.
            concurrency (int or list): Number of workers per stage. If an int, it applies to all stages.
            maxsize (int): Maximum size for each queue (0 means unlimited).
        """
        self.queue_manager = DynamicQueueManager(maxsize=maxsize)
        self.queue_interface = QueueInterface(self.queue_manager)

        # Handle both list and dict inputs for stage_funcs.
        if isinstance(stage_funcs, dict):
            # In dict mode, preserve insertion order (Python 3.7+)
            self.stage_keys = list(stage_funcs.keys())
            self.stage_funcs_list = list(stage_funcs.values())
            # Append an extra queue for the final output, named "final".
            self.queue_keys = self.stage_keys + ["final"]
        elif isinstance(stage_funcs, list):
            # Auto-generate keys as "stage_0", "stage_1", ...
            num_stages = len(stage_funcs)
            self.stage_funcs_list = stage_funcs
            self.queue_keys = [f"stage_{i}" for i in range(num_stages + 1)]
        else:
            raise ValueError("stage_funcs must be either a list or a dict")

        # For user convenience, expose the input and output keys.
        self.input_queue_key = self.queue_keys[0]
        self.output_queue_key = self.queue_keys[-1]

        # Normalize concurrency into a list (one entry per stage).
        num_stages = len(self.stage_funcs_list)
        if isinstance(concurrency, int):
            self.concurrency = [concurrency] * num_stages
        else:
            if len(concurrency) != num_stages:
                raise ValueError("Length of concurrency list must match number of stages")
            self.concurrency = concurrency

        self.pipeline_tasks = []
        # Create all queues.
        for key in self.queue_keys:
            self.queue_manager.get_queue(key)

        # Launch worker tasks for each stage with configurable concurrency.
        for i, stage_func in enumerate(self.stage_funcs_list):
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

    @property
    def dynamic_queue_manager(self) -> DynamicQueueManager:
        """Expose the underlying DynamicQueueManager instance."""
        return self.queue_manager

    @property
    def queue_iface(self) -> QueueInterface:
        """Expose the underlying QueueInterface instance."""
        return self.queue_interface

    async def add_data(self, data) -> None:
        """
        Feed initial data into the pipeline. Data is added to the first stage's input queue.
        """
        await self.queue_interface.put(self.input_queue_key, data)

    async def get_result(self):
        """
        Retrieve a processed result from the final output queue.
        If a shutdown sentinel is encountered, returns None.
        """
        result = await self.queue_interface.get(self.output_queue_key)
        if result is _SENTINEL:
            return None
        return result

    async def shutdown(self, immediate: bool = False):
        """
        Shut down the pipeline.
        
        Args:
            immediate (bool): If True, cancel all worker tasks immediately (discarding pending work)
                              and inject a shutdown sentinel into the final queue.
                              If False, perform a graceful shutdown by propagating sentinel values.
        """
        if immediate:
            # Immediate shutdown: cancel all worker tasks.
            for task in self.pipeline_tasks:
                task.cancel()
            await asyncio.gather(*self.pipeline_tasks, return_exceptions=True)
            # Clear the final queue.
            final_queue = self.queue_manager.get_queue(self.output_queue_key)
            while not final_queue.empty():
                try:
                    final_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            await self.queue_interface.put(self.output_queue_key, _SENTINEL)
            logging.info("Immediate pipeline shutdown complete.")
        else:
            # Graceful shutdown: send sentinel values to the first stage.
            num_workers = self.concurrency[0]
            for _ in range(num_workers):
                await self.queue_interface.put(self.input_queue_key, _SENTINEL)
            await asyncio.gather(*self.pipeline_tasks, return_exceptions=True)
            logging.info("Pipeline shutdown complete.")

    def log_queue_sizes(self) -> None:
        """
        Log the sizes of all queues in the pipeline.
        """
        self.queue_manager.log_queue_sizes()
