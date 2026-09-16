from .Step import Step
from abc import abstractmethod
from typing import TypeVar, AsyncIterator
from .models import Item

In = TypeVar("In", contravariant=True)

class Loader(Step[list[In], list[In]]): 
    @abstractmethod
    async def load(self, chunk: list[In]) -> None:
        ...

    async def execute(self, input: AsyncIterator[list[Item[In]]]) -> AsyncIterator[list[Item[In]]]:
        async for chunk in input:
            payload = [item.value for item in chunk]
            await self.load(payload)
            yield chunk