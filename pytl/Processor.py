from .Step import Step
from concurrent.futures import Executor

from abc import abstractmethod
from typing import TypeVar, AsyncIterator
import asyncio

In = TypeVar("In", contravariant=True)
Out = TypeVar("Out", covariant=True)

class Processor(Step[list[In],list[Out]]):
    def __init__(self, executor: Executor = None):
        self.executor=executor

    @abstractmethod
    def process(self, input: In) -> Out:
        ...

    async def execute(self, input: AsyncIterator[list[In]]) -> AsyncIterator[list[Out]]:    
        async for chunk in input:
            yield await asyncio.gather(
                *(self._execute_process(item) for item in chunk),
                return_exceptions=False, # Keep return_exceptions=False until process-level error handling is implemented.
            )                            # Switch to True when exceptions need to be collected and handled per item.
        
    async def _execute_process(self, input: In) -> Out:
        if self.executor:
            loop = asyncio.get_running_loop()

            return await loop.run_in_executor(
                self.executor,
                self.process,
                input,
            )

        return await asyncio.to_thread(
            self.process,
            input,
        )

class AsyncProcessor(Step[list[In],list[Out]]):
    @abstractmethod
    async def process(self, input: In) -> Out:
        ...

    async def execute(self, input: AsyncIterator[list[In]]) -> AsyncIterator[list[Out]]:    
        async for chunk in input:
            yield await asyncio.gather(
                *(self.process(item) for item in chunk),
                return_exceptions=False, # Keep return_exceptions=False until process-level error handling is implemented.
            )                            # Switch to True when exceptions need to be collected and handled per item.