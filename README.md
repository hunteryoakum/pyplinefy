# pyplinefy

**pyplinefy** is a Python package for building and managing asynchronous processing pipelines with ease. It is built on Python’s `asyncio` to create linear pipelines that automatically manage queues between processing stages. Each stage is defined as an asynchronous function, and the package takes care of queue management, error handling, configurable concurrency, and graceful shutdown.


## Features

- **Managed Pipeline:** Build a full asynchronous pipeline by simply providing a list of async functions.
- **Dynamic Queue Management:** Automatically creates and manages queues for each processing stage.
- **Configurable Concurrency:** Easily specify how many worker tasks should run per stage.
- **Graceful Shutdown:** Uses a sentinel mechanism to signal shutdown and allow workers to exit cleanly.
- **Enhanced Logging:** Built-in logging for queue sizes and error tracking.
- **Extensible and Modular:** A clean, modular codebase that you can easily extend or integrate into your own projects.

## Installation

You must install **pyplinefy** directly from source until it is available from PyPI.

<!-- ### From PyPI

```bash
pip install pyplinefy
``` -->

### From Source

```bash
git clone https://github.com/your_username/pyplinefy.git
cd pyplinefy
pip install -e .
```

## Usage

Below is an example of how to use pyplinefy to create a simple asynchronous pipeline:

```python
import asyncio
from pyplinefy.pipeline_manager import ManagedPipeline

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
    
    # Allow some time for the pipeline to process the items
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

1. multiply_by_two: Multiplies each input by 2.
2. add_five: Adds 5 to the result.
3. to_string: Converts the result to a string.


## Testing

Tests are provided using pytest and pytest-asyncio to handle asynchronous test functions.

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

pyplinefy is licensed under the MIT License. See the LICENSE file for details.