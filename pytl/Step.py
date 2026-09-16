from abc import ABC, abstractmethod
from typing import AsyncIterator, TypeVar, Generic
from .models import Item

In = TypeVar("In", contravariant=True)
Out = TypeVar("Out", covariant=True)

class Step(ABC, Generic[In, Out]):
    @abstractmethod
    async def execute(self, input: AsyncIterator[list[Item[In]]]) -> AsyncIterator[list[Item[Out]]] | None:
        ...

