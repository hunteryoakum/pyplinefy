# tests/test_managed_pipeline.py
import asyncio
import pytest
from pyplinefy.managed_pipeline import ManagedPipeline

# Define simple asynchronous stage functions.
async def stage1(x):
    return x * 2

async def stage2(x):
    return x + 3

@pytest.mark.asyncio
async def test_pipeline_processing():
    # Create a pipeline with two stages.
    pipeline = ManagedPipeline([stage1, stage2], concurrency=1, maxsize=10)
    inputs = [1, 2, 3]
    for i in inputs:
        await pipeline.add_data(i)
    results = []
    for _ in inputs:
        result = await pipeline.get_result()
        results.append(result)
    # Expected: (x * 2) + 3 for each input.
    expected = [(x * 2) + 3 for x in inputs]
    assert results == expected
    await pipeline.shutdown()

@pytest.mark.asyncio
async def test_pipeline_shutdown():
    # Create a pipeline with two stages.
    pipeline = ManagedPipeline([stage1, stage2], concurrency=1, maxsize=10)
    # Add a single input.
    await pipeline.add_data(10)
    await pipeline.shutdown()
    # After shutdown, get_result() should return None.
    result = await pipeline.get_result()
    assert result is None


