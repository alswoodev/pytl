from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import AsyncIterator

import pytest

from pytl import (
    AsyncFlatProcessor,
    AsyncProcessor,
    FlatProcessor,
    Loader,
    Pipeline,
    PolymorphicLoader,
    Processor,
    Source,
)
from pytl.models import Item, ItemContext


async def collect(async_iterable):
    return [item async for item in async_iterable]


class NumberSource(Source[int]):
    def __init__(self, values: list[int], chunk_size: int = 1):
        super().__init__(chunk_size)
        self.values = values

    async def stream(self) -> AsyncIterator[int]:
        for value in self.values:
            yield value


class UpperProcessor(Processor[str, str]):
    def process(self, input: str) -> str:
        return str(input).upper()


class AsyncUpperProcessor(AsyncProcessor[str, str]):
    async def process(self, input: str) -> str:
        await asyncio.sleep(0)
        return input.upper()


class SplitProcessor(FlatProcessor[str, str]):
    def process(self, input: str) -> list[str]:
        return list(input)


class AsyncSplitProcessor(AsyncFlatProcessor[str, str]):
    async def process(self, input: str) -> list[str]:
        await asyncio.sleep(0)
        return list(input)


class RecordingLoader(Loader[str]):
    def __init__(self):
        self.loaded: list[list[str]] = []

    async def load(self, chunk: list[str]) -> None:
        self.loaded.append(chunk)


def make_items(*values: str) -> list[Item[str]]:
    context = ItemContext("TestPipeline", "run-1", "batch-1")
    return [Item(origin=value, value=value, context=context) for value in values]


@pytest.mark.asyncio
async def test_source_emits_fixed_size_chunks_and_flushes_remainder():
    source = NumberSource([1, 2, 3, 4, 5], chunk_size=2)

    assert await collect(source.execute()) == [[1, 2], [3, 4], [5]]


@pytest.mark.parametrize("chunk_size", [0, -1])
def test_source_rejects_non_positive_chunk_size(chunk_size):
    with pytest.raises(Exception, match="Chunk size must be upper 0"):
        NumberSource([], chunk_size=chunk_size)


@pytest.mark.asyncio
async def test_processor_transforms_values_concurrently_and_preserves_context():
    items = make_items("a", "b")

    output = await collect(UpperProcessor().execute(iter_chunks(items)))

    assert [item.value for item in output[0]] == ["A", "B"]
    assert [item.origin for item in output[0]] == ["a", "b"]
    assert all(item.context is items[0].context for item in output[0])


@pytest.mark.asyncio
async def test_processor_skips_failed_items_without_dropping_successes():
    class FailingProcessor(Processor[int, int]):
        def process(self, input: int) -> int:
            if input == 2:
                raise ValueError("bad item")
            return input * 2

    items = [
        Item(value=value, origin=value, context=ItemContext("P", "r", "b"))
        for value in [1, 2, 3]
    ]

    output = await collect(FailingProcessor().execute(iter_chunks(items)))

    assert [item.value for item in output[0]] == [2, 6]


@pytest.mark.asyncio
async def test_async_processor_transforms_values():
    output = await collect(AsyncUpperProcessor().execute(iter_chunks(make_items("a", "b"))))

    assert [item.value for item in output[0]] == ["A", "B"]


@pytest.mark.asyncio
@pytest.mark.parametrize("processor", [SplitProcessor(), AsyncSplitProcessor()])
async def test_flat_processors_expand_values_and_preserve_item_metadata(processor):
    items = make_items("ab")

    output = await collect(processor.execute(iter_chunks(items)))

    assert [item.value for item in output[0]] == ["a", "b"]
    assert [item.origin for item in output[0]] == ["ab", "ab"]
    assert all(item.context is items[0].context for item in output[0])


@pytest.mark.asyncio
async def test_loader_receives_values_and_yields_items_for_downstream_steps():
    loader = RecordingLoader()
    items = make_items("a", "b")

    output = await collect(loader.execute(iter_chunks(items)))

    assert loader.loaded == [["a", "b"]]
    assert output == [items]


@pytest.mark.asyncio
async def test_pipeline_runs_source_steps_and_loader_in_order():
    loader = RecordingLoader()
    pipeline = (
        Pipeline.builder()
        .source(NumberSource([1, 2, 3], chunk_size=2))
        .steps(UpperProcessor(), loader)
        .build()
    )

    await pipeline.run()

    assert loader.loaded == [["1", "2"], ["3"]]

@pytest.mark.asyncio
async def test_polymorphic_loader_routes_each_type_to_matching_loaders():
    class IntLoader(Loader[int]):
        def __init__(self):
            self.loaded: list[list[int]] = []

        async def load(self, chunk: list[int]) -> None:
            self.loaded.append(chunk)

    class StringLoader(Loader[str]):
        def __init__(self):
            self.loaded: list[list[str]] = []

        async def load(self, chunk: list[str]) -> None:
            self.loaded.append(chunk)

    int_loader = IntLoader()
    string_loader = StringLoader()
    loader = PolymorphicLoader(int_loader, string_loader)

    await loader.load([1, "one", 2, "two"])

    assert int_loader.loaded == [[1, 2]]
    assert string_loader.loaded == [["one", "two"]]


@pytest.mark.asyncio
async def test_builder_requires_a_source():
    with pytest.raises(Exception, match="Source is required"):
        Pipeline.builder().build()


async def iter_chunks(items: list[Item]):
    yield items
