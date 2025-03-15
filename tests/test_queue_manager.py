# tests/test_queue_manager.py
import asyncio
import logging
import pytest
from pyplinefy.queue_manager import QueueManager, AutoQueueManager

@pytest.mark.asyncio
async def test_create_get_remove_queue():
    qm = QueueManager()
    key = "test_queue"
    # Initially, the queue should not exist.
    assert not qm.exists(key)
    
    # Create the queue explicitly.
    qm.create_queue(key)
    assert qm.exists(key)
    
    # Retrieve the queue and check type.
    queue = qm.get_queue(key)
    assert isinstance(queue, asyncio.Queue)
    
    # Put an item and check queue size.
    await queue.put("item")
    sizes = qm.get_queue_sizes()
    assert sizes.get(key, 0) == 1
    
    # Remove the queue.
    qm.remove_queue(key)
    assert not qm.exists(key)
    assert key not in qm.list_queues()

@pytest.mark.asyncio
async def test_auto_queue_manager():
    aqm = AutoQueueManager(maxsize=10)
    key = "auto_queue"
    # Using AutoQueueManager, get_queue() should create the queue if it doesn't exist.
    queue = aqm.get_queue(key)
    assert isinstance(queue, asyncio.Queue)
    # The queue should now exist.
    assert aqm.exists(key)
    # Put an item and check size.
    await queue.put("auto_item")
    sizes = aqm.get_queue_sizes()
    assert sizes.get(key, 0) == 1

@pytest.mark.asyncio
async def test_log_queue_sizes(caplog):
    qm = QueueManager()
    key = "test_queue"
    qm.create_queue(key)
    queue = qm.get_queue(key)
    await queue.put("item")
    with caplog.at_level(logging.INFO):
        qm.log_queue_sizes()
    # Check that the log contains the queue name and size information.
    assert any(key in record.message for record in caplog.records)
