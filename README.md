# PyTL

PyTL is a lightweight asynchronous framework for building ETL pipelines in Python. Data flows in chunks, and you can compose `Source`, `Processor`, and `Loader` components to define ingestion, transformation, and storage stages.

## Features

- Chunk-based processing built on asynchronous iterables
- Support for both synchronous and asynchronous processing functions
- Preservation of the original value and execution context for each transformed item
- Flat processors that expand one input into multiple outputs
- `CompositeLoader` for routing values to loaders by their concrete type
- A generic API built around type hints

## Requirements and Installation

`pytl/models.py` currently uses some syntax, which is supported starting with Python 3.12 (for example, PEP 695 generic syntax (`class Item[T]`)), so Python 3.12 or later is recommended in practice.

To install PyTL from the repository, run:

```bash
python -m pip install -e .
```

To install the test dependencies and run the tests:

```bash
python -m pip install pytest pytest-asyncio
pytest
```

## Quick Start

Each component declares the types it processes using generics and implements only the methods it needs.

```python
import asyncio
from typing import AsyncIterator

from pytl import Loader, Pipeline, Processor, Source


class NumberSource(Source[int]):
    async def stream(self) -> AsyncIterator[int]:
        for number in range(1, 6):
            yield number


class StringProcessor(Processor[int, str]):
    def process(self, input: int) -> str:
        return f"number={input}"


class PrintLoader(Loader[str]):
    async def load(self, chunk: list[str]) -> None:
        print(chunk)


async def main() -> None:
    pipeline = (
        Pipeline.builder()
        .source(NumberSource(chunk_size=2))
        .steps(StringProcessor(), PrintLoader())
        .build()
    )

    await pipeline.run()


asyncio.run(main())
```

The output follows the source chunk size:

```text
['number=1', 'number=2']
['number=3', 'number=4']
['number=5']
```

## Processing Model

The pipeline passes data through the following stages.

```text
Source.execute()
    -> attach context
    -> Step.execute() ...
    -> consume through the final Step
```

### Source

`Source[T]` asynchronously generates values one at a time from `stream()`. Once `chunk_size` values have been collected, they are emitted as a chunk. Any remaining values are flushed as a separate chunk when the stream ends.

```python
class EventSource(Source[dict]):
    async def stream(self):
        for event in events:
            yield event


source = EventSource(chunk_size=100)
```

`chunk_size` must be at least 1.

### Processor

PyTL provides four processor types:

| Class | Method to implement | Use |
| --- | --- | --- |
| `Processor[In, Out]` | `def process(...) -> Out` | Synchronous one-to-one transformation |
| `AsyncProcessor[In, Out]` | `async def process(...) -> Out` | Asynchronous one-to-one transformation |
| `FlatProcessor[In, Out]` | `def process(...) -> list[Out]` | Synchronous one-to-many transformation |
| `AsyncFlatProcessor[In, Out]` | `async def process(...) -> list[Out]` | Asynchronous one-to-many transformation |

Synchronous processors use `asyncio.to_thread()` by default to process items concurrently. You can pass a `concurrent.futures.Executor` to use a specific executor instead.

```python
class SplitWords(FlatProcessor[str, str]):
    def process(self, input: str) -> list[str]:
        return input.split()
```

If `process()` raises an exception for an item in a synchronous one-to-one processor, that item is omitted from the results while successful items in the same chunk continue to be processed. For a flat processor, an exception for any item causes the transformation to fail, so the one-to-many transformation is not produced.

### Loader

`Loader[T]` receives a list of the chunk's `Item.value` values through `load()`.

```python
class DatabaseLoader(Loader[dict]):
    async def load(self, chunk: list[dict]) -> None:
        await save_to_database(chunk)
```

After `load()` completes, the loader passes the input `Item` chunk unchanged to the next step. This means you can connect another step after a loader. `Pipeline.run()` consumes the entire stream until the final output is produced.

### CompositeLoader

Use `CompositeLoader` to send different types to their respective loaders.

```python
class IntLoader(Loader[int]):
    async def load(self, chunk: list[int]) -> None:
        await save_integers(chunk)


class TextLoader(Loader[str]):
    async def load(self, chunk: list[str]) -> None:
        await save_texts(chunk)


loader = CompositeLoader(IntLoader(), TextLoader())
```

Each input value is routed to the loader group that exactly matches `type(value)`. The current implementation does not search through inheritance relationships, and values without a matching loader are skipped. If multiple loaders are registered for one type, that type's chunk is passed to every matching loader.

## Items and Execution Context

For each source value, the pipeline creates an `Item` with the following structure:

```python
Item(
    origin=source_value,
    value=current_value,
    context=ItemContext(
        pipeline_cls_name="Pipeline",
        pipeline_run_id="...",
        batch_id=1,
    ),
)
```

- `origin`: The original source value
- `value`: The value transformed by the current step
- `context`: Information about the pipeline run and the original chunk

For one-to-many transformations, `origin` and `context` also let you trace each result back to its source.

## API Overview

- `Pipeline.builder().source(source).step(step).build()`: Register steps one at a time
- `Pipeline.builder().source(source).steps(*steps).build()`: Register multiple steps at once
- `await pipeline.run()`: Run the pipeline
- `Source[T]`: Define an asynchronous input stream and chunk size
- `Processor`, `AsyncProcessor`: One-to-one transformations
- `FlatProcessor`, `AsyncFlatProcessor`: One-to-many transformations
- `Loader[T]`: Store chunks or send them to an external system
- `CompositeLoader`: Distribute values to loaders by type

## Development

```bash
pytest -q
```

Run the example:

```bash
python example.py
```

`example.py` contains both a finite batch pipeline and a streaming pipeline that continuously generates values. Because the streaming example runs indefinitely, uncomment `asyncio.run(streaming_pipeline.run())` when running it directly and stop the process separately.