# src/pyplinefy/managed_pipeline.py
import asyncio
import logging
from .queue_manager import QueueManager, AutoQueueManager
from .queue_interface import QueueInterface

_SENTINEL = object()

class ManagedPipeline:
    """
    Managed asynchronous pipeline that automatically creates queues and worker tasks. Supports conditional routing.
    Uses QueueManager to manage queues and QueueInterface to manage items in queues.
    """
    def __init__(self, stage_funcs: list, concurrency=1, maxsize: int = 0, queue_keys: list = None, auto: bool = False, routing_funcs=None):
        """
        Args:
            stage_funcs (list): List of async functions for each stage.
            concurrency (int or list): Number of workers per stage. If an int, it applies to all stages.
            maxsize (int): Maximum size for each queue (0 means unlimited).
            queue_keys (list): Optional list of queue names. Must have len(stage_funcs) + 1 elements.
            auto (bool): Whether to use AutoQueueManager for implicit queue creation.
            routing_funcs (list): List of functions for dynamic routing, or None for fixed pipelines.
        """
        queue_manager_cls = AutoQueueManager if auto else QueueManager
        queue_manager = queue_manager_cls(maxsize=maxsize)

        self.queue_interface = QueueInterface(queue_manager)
        self.stage_funcs = stage_funcs

        # Validate or generate queue keys
        expected_queue_count = len(stage_funcs) + 1
        if queue_keys:
            if len(queue_keys) != expected_queue_count:
                raise ValueError(f"queue_keys must have {expected_queue_count} elements, but got {len(queue_keys)}")
            self.queue_keys = queue_keys
        else:
            self.queue_keys = [f"stage_{i}" for i in range(expected_queue_count)]

        # Create queues if using explicit QueueManager
        if not auto:
            for key in self.queue_keys:
                queue_manager.create_queue(key)

        num_stages = len(self.stage_funcs)
        if isinstance(concurrency, int):
            self.concurrency = [concurrency] * num_stages
        else:
            if len(concurrency) != num_stages:
                raise ValueError("Length of concurrency list must match number of stages")
            self.concurrency = concurrency

        # Default to direct linear queue connections if no routing functions are provided
        self.routing_funcs = routing_funcs or [None] * num_stages

        # Create and start workers
        self.workers = []
        for i, stage_func in enumerate(stage_funcs):
            in_key = self.queue_keys[i]
            out_key = self.routing_funcs[i] or self.queue_keys[i + 1]  # Use routing function if provided
            for _ in range(self.concurrency[i]):
                worker = StageWorker(self.queue_interface, in_key, out_key, stage_func, stage_index=i)
                self.workers.append(worker)
                worker.start()


    async def add_data(self, data):
        await self.queue_interface.put(self.queue_keys[0], data)

    async def get_result(self):
        result = await self.queue_interface.get(self.queue_keys[-1])
        if result is _SENTINEL:
            return None
        return result

    async def shutdown(self):
        """
        Shut down the pipeline by sending shutdown sentinels to the input queue,
        then cancelling all worker tasks. Finally, flush the final output queue and
        inject a shutdown sentinel so that subsequent get_result() calls return None.
        """
        logging.info("Shutting down pipeline...")
        # Send a shutdown sentinel for each worker to the first queue.
        for _ in range(sum(self.concurrency)):
            await self.queue_interface.put(self.queue_keys[0], _SENTINEL)

        # Stop all workers.
        for worker in self.workers:
            worker.stop()
        # Await all worker tasks; suppress cancellation errors.
        tasks = [worker.task for worker in self.workers if worker.task is not None]
        await asyncio.gather(*tasks, return_exceptions=True)

        # Flush the final output queue.
        out_queue = self.queue_interface.queue_manager.get_queue(self.queue_keys[-1])
        while not out_queue.empty():
            try:
                out_queue.get_nowait()
                out_queue.task_done()
            except asyncio.QueueEmpty:
                break

        # Inject a shutdown sentinel into the final output queue.
        await self.queue_interface.put(self.queue_keys[-1], _SENTINEL)
        logging.info("Pipeline shutdown complete.")


class StageWorker:
    """
    A worker that processes items from an input queue using a provided async function
    and dynamically routes results to one or more output queues.
    """
    def __init__(self, queue_interface, in_key: str, out_key, stage_func, stage_index: int):
        """
        Args:
            queue_interface: Interface to interact with queues.
            in_key (str): Input queue key.
            out_key (str or callable): Output queue key or a function that determines the output queue dynamically.
            stage_func (callable): Processing function.
            stage_index (int): Index of the pipeline stage.
        """
        self.queue_interface = queue_interface
        self.in_key = in_key
        self.out_key = out_key
        self.stage_func = stage_func
        self.stage_index = stage_index
        self.task = None

    async def _worker(self):
        try:
            while True:
                item = await self.queue_interface.get(self.in_key)
                if item is _SENTINEL:
                    if callable(self.out_key):
                        # If it's a function, we need to broadcast the sentinel to all possible outputs
                        possible_outputs = self.out_key(None)  # Get all potential output keys
                        for out_q in possible_outputs:
                            await self.queue_interface.put(out_q, _SENTINEL)
                    else:
                        await self.queue_interface.put(self.out_key, _SENTINEL)
                    
                    logging.info(f"Worker in stage {self.stage_index} received shutdown signal.")
                    break
                
                try:
                    result = await self.stage_func(item)

                    # Determine the output queue dynamically if out_key is a function
                    if callable(self.out_key):
                        selected_queue = self.out_key(result)
                        if isinstance(selected_queue, list):  # If multiple outputs, send to all
                            for q in selected_queue:
                                await self.queue_interface.put(q, result)
                        else:
                            await self.queue_interface.put(selected_queue, result)
                    else:
                        if result is not None:
                            await self.queue_interface.put(self.out_key, result)

                    # else:
                    #     await self.queue_interface.put(self.out_key, result)
                
                except Exception as e:
                    logging.error(f"Error processing item '{item}' in stage {self.stage_index}: {e}", exc_info=True)
        except asyncio.CancelledError:
            logging.info(f"Worker in stage {self.stage_index} cancelled.")
            raise

    def start(self):
        self.task = asyncio.create_task(self._worker())

    def stop(self):
        if self.task:
            self.task.cancel()
