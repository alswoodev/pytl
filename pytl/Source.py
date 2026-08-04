from abc import abstractmethod, ABC
from typing import AsyncIterator, TypeVar, Generic

Out = TypeVar("Out", covariant=True)

class Source(ABC, Generic[Out]):
    def __init__(self, chunk_size: int = 1):
        self._set_chunk_size(chunk_size)

    @abstractmethod
    async def provide(self) -> AsyncIterator[Out]:
        ...

    def _set_chunk_size(self, chunk_size: int):
        if chunk_size <= 0: raise Exception("Chunk size must be upper 0")
        self.chunk_size = chunk_size

    async def stream(self) -> AsyncIterator[list[Out]]:
        batch = []

        async for item in self.provide():
            batch.append(item)
            if len(batch) >= self.chunk_size:
                yield batch
                batch = []

        if batch:
            yield batch