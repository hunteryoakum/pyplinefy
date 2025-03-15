# tests/test_queue_interface.py
import asyncio
import pytest
from pyplinefy.queue_manager import QueueManager
from pyplinefy.queue_interface import QueueInterface

@pytest.fixture
def queue_interface():
    qm = QueueManager(maxsize=10)
    # Create a test queue.
    qm.create_queue("test_queue")
    return QueueInterface(qm)

@pytest.mark.asyncio
async def test_put_and_get(queue_interface):
    # Put an item and then get it.
    await queue_interface.put("test_queue", "value1")
    value = await queue_interface.get("test_queue")
    assert value == "value1"

@pytest.mark.asyncio
async def test_join_queue(queue_interface):
    # Test join_queue by adding multiple items and then calling join.
    await queue_interface.put("test_queue", "item1")
    await queue_interface.put("test_queue", "item2")
    
    # Start a background task that gets the items.
    async def consumer():
        await asyncio.sleep(0.1)
        await queue_interface.get("test_queue")
        await queue_interface.get("test_queue")
    
    consumer_task = asyncio.create_task(consumer())
    
    # join_queue should block until all items are processed.
    await queue_interface.join_queue("test_queue")
    await consumer_task  # Ensure consumer is done.

@pytest.mark.asyncio
async def test_get_nonexistent_queue():
    qm = QueueManager(maxsize=10)
    qi = QueueInterface(qm)
    with pytest.raises(KeyError):
        await qi.get("nonexistent_queue")
