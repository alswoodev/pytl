from .Step import Step
from abc import abstractmethod
from typing import TypeVar, AsyncIterator

In = TypeVar("In", contravariant=True)

class Loader(Step[list[In], None]): 
    @abstractmethod
    async def load(self, chunk: list[In]):
        ...

    async def execute(self, input: AsyncIterator[list[In]]) -> None:
        async for chunk in input:
            await self.load(chunk)