from abc import ABC, abstractmethod
from typing import AsyncIterator, TypeVar, Generic

In = TypeVar("In", contravariant=True)
Out = TypeVar("Out", covariant=True)

class Step(ABC, Generic[In, Out]):
    @abstractmethod
    async def execute(self, input: In) -> AsyncIterator[Out] | None:
        ...