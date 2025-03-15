# tests/test_create_queue_interface.py
import asyncio
import pytest
from pyplinefy.factory import create_queue_interface
from pyplinefy.queue_manager import QueueManager, AutoQueueManager

@pytest.mark.asyncio
async def test_create_queue_interface_with_keys():
    # Create a QueueInterface with pre-created queues using explicit QueueManager.
    keys = ["alpha", "beta", "gamma"]
    qi = create_queue_interface(queue_keys=keys, maxsize=5, auto=False)
    
    # Verify that the underlying QueueManager (explicit) has the expected keys.
    for key in keys:
        assert qi.queue_manager.exists(key)
    
    # Try putting and getting an item in one of the queues.
    await qi.put("alpha", "test_item")
    item = await qi.get("alpha")
    assert item == "test_item"

@pytest.mark.asyncio
async def test_create_queue_interface_auto():
    # Create a QueueInterface with auto queue creation.
    keys = ["delta", "epsilon"]
    qi = create_queue_interface(queue_keys=keys, maxsize=5, auto=True)
    
    # For AutoQueueManager, the keys should be created implicitly.
    for key in keys:
        # Even if we didn't pre-create them, they should be created upon first access.
        await qi.put(key, "auto_item")
        item = await qi.get(key)
        assert item == "auto_item"
    
    # Check that the underlying QueueManager is an instance of AutoQueueManager.
    from pyplinefy.queue_manager import AutoQueueManager
    assert isinstance(qi.queue_manager, AutoQueueManager)
