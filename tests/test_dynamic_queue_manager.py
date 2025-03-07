# tests/test_dynamic_queue_manager.py
import asyncio
import logging
import pytest
from pyplinefy.dynamic_queue_manager import DynamicQueueManager

@pytest.mark.asyncio
async def test_get_and_remove_queue():
    dqm = DynamicQueueManager()
    key = "test_queue"
    queue = dqm.get_queue(key)
    assert isinstance(queue, asyncio.Queue)
    # Test that the queue is listed
    assert key in dqm.list_queues()
    # Put an item and check queue size
    await queue.put("item")
    sizes = dqm.get_queue_sizes()
    assert sizes.get(key, 0) == 1
    # Remove queue and ensure it is no longer listed
    dqm.remove_queue(key)
    assert key not in dqm.list_queues()

@pytest.mark.asyncio
async def test_log_queue_sizes(caplog):
    dqm = DynamicQueueManager()
    key = "test_queue"
    queue = dqm.get_queue(key)
    await queue.put("item")
    with caplog.at_level(logging.INFO):
        dqm.log_queue_sizes()
    # Check that the log contains the queue name and size information.
    assert any("test_queue" in record.message for record in caplog.records)
