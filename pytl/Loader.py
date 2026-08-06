from .Step import Step
from abc import abstractmethod
from typing import TypeVar, AsyncIterator

In = TypeVar("In", contravariant=True)

class Loader(Step[list[In], list[In]]): 
    @abstractmethod
    async def load(self, chunk: list[In]) -> None:
        ...

    async def execute(self, input: AsyncIterator[list[In]]) -> AsyncIterator[list[In]]:
        async for chunk in input:
            await self.load(chunk)
            yield chunk