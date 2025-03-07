# tests/test_pipeline_manager.py
import asyncio
import pytest
from pyplinefy.pipeline_manager import ManagedPipeline

# Define simple async stage functions for testing.
async def stage1(x):
    return x * 2

async def stage2(x):
    return x + 3

@pytest.mark.asyncio
async def test_pipeline_processing():
    stage_funcs = [stage1, stage2]
    pipeline = ManagedPipeline(stage_funcs, concurrency=1)
    inputs = [1, 2, 3]
    for i in inputs:
        await pipeline.add_data(i)
    results = []
    for _ in inputs:
        result = await pipeline.get_result()
        results.append(result)
    expected = [2 * x + 3 for x in inputs]
    assert results == expected
    await pipeline.shutdown()

@pytest.mark.asyncio
async def test_pipeline_shutdown_propagation():
    stage_funcs = [stage1, stage2]
    pipeline = ManagedPipeline(stage_funcs, concurrency=1)
    # Add one item.
    await pipeline.add_data(10)
    # Immediately initiate immediate shutdown.
    await pipeline.shutdown(immediate=True)
    # The pipeline should eventually return a shutdown signal (None).
    result = await pipeline.get_result()
    assert result is None
