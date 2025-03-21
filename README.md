# pyplinefy

**pyplinefy** is a Python package for building and managing asynchronous processing pipelines with ease. It leverages Python’s `asyncio` to create linear pipelines that automatically manage queues between processing stages. Each stage is defined as an asynchronous function, and the package handles queue management, error handling, configurable concurrency, and graceful shutdown. The package is designed in a modular and extensible way with separate components for queue management, a queue interface, a managed pipeline, and a factory function for creating queue interfaces.

## Features

- **Managed Pipeline:** Build a full asynchronous pipeline by providing a list of async functions.
- **Dynamic Queue Management:** Automatically creates and manages queues for each processing stage.
- **Configurable Concurrency:** Easily specify the number of worker tasks per stage.
- **Graceful Shutdown:** Uses a sentinel mechanism to signal shutdown and allow workers to exit cleanly.
- **Enhanced Logging:** Built-in logging for monitoring queue sizes and error tracking.
- **Queue Factory:** Create a queue interface with pre-created queues using either explicit or auto queue management.
- **Modular Design:** The package includes components like `QueueManager`, `QueueInterface`, `ManagedPipeline`, and a factory function `create_queue_interface`.

## Installation

Until **pyplinefy** is available on PyPI, install it directly from source:

```bash
git clone https://github.com/hunteryoakum/pyplinefy.git
cd pyplinefy
pip install -e .
```

## Usage

### Creating a Managed Pipeline

Below is an example of how to create a simple asynchronous pipeline using `ManagedPipeline`:

```python
import asyncio
from pyplinefy.managed_pipeline import ManagedPipeline

# Define asynchronous processing stages
async def multiply_by_two(item):
    await asyncio.sleep(0.1)  # Simulate processing delay
    return item * 2

async def add_five(item):
    await asyncio.sleep(0.1)
    return item + 5

async def to_string(item):
    await asyncio.sleep(0.1)
    return f"Result: {item}"

async def main():
    # Create a pipeline with three stages, each with 2 worker tasks
    pipeline = ManagedPipeline([multiply_by_two, add_five, to_string], concurrency=2)
    
    # Feed data into the pipeline
    for i in range(5):
        await pipeline.add_data(i)
    
    # Allow some time for processing
    await asyncio.sleep(2)
    
    # Retrieve results from the final stage
    results = []
    for _ in range(5):
        result = await pipeline.get_result()
        results.append(result)
    
    print("Results:", results)
    
    # Shut down the pipeline gracefully
    await pipeline.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
```

This example creates a pipeline with three stages:
1. **multiply_by_two:** Multiplies each input by 2.
2. **add_five:** Adds 5 to the result.
3. **to_string:** Converts the result to a string.

### Using the Queue Factory

You can also automatically create a `QueueInterface`, its underlying `QueueManager`, and all associated queues using the `create_queue_interface` factory function:

```python
import asyncio
from pyplinefy.factory import create_queue_interface

queue_names = ["alpha", "beta", "gamma"]

# Pre-create queues "alpha", "beta", and "gamma"
qi = create_queue_interface(queue_keys=queue_names, maxsize=10, auto=False)

# Use the QueueInterface to put and get items
async def demo():
    await qi.put("alpha", "test_item")
    item = await qi.get("alpha")
    print("Retrieved item:", item)

asyncio.run(demo())
```

## Testing

Tests are written using [pytest](https://docs.pytest.org/) and [pytest-asyncio](https://github.com/pytest-dev/pytest-asyncio) to handle asynchronous test functions.

### Running the Tests

1. Install the test dependencies (if not already installed):

    ```bash
    pip install pytest pytest-asyncio
    ```

2. Execute the tests from the root of the repository:

    ```bash
    pytest
    ```

## Contributing

Contributions are welcome! If you encounter issues or have suggestions for new features, please open an issue or submit a pull request on GitHub.

## License

**pyplinefy** is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
