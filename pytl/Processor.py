from .Step import Step
from .models import Item
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

    async def execute(self, input: AsyncIterator[list[Item[In]]]) -> AsyncIterator[list[Item[Out]]]:    
        async for chunk in input:
            results = await asyncio.gather(
                *(self._execute_process(item.value) for item in chunk),
                return_exceptions=True , # Keep return_exceptions=False until process-level error handling is implemented.
            )                            # Switch to True when exceptions need to be collected and handled per item.

            output_chunk: list[Item[Out]] = []

            for item, result in zip(chunk, results):
                if isinstance(result, Exception):
                    ...
                    continue
                item.value = result
                output_chunk.append(item)
            if output_chunk:
                yield output_chunk

        
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

    async def execute(self, input: AsyncIterator[list[Item[In]]]) -> AsyncIterator[list[Item[Out]]]:
        async for chunk in input:
            results = await asyncio.gather(
                *(self.process(item.value) for item in chunk),
                return_exceptions=True , # Keep return_exceptions=False until process-level error handling is implemented.
            )                            # Switch to True when exceptions need to be collected and handled per item.

            output_chunk: list[Item[Out]] = []

            for item, result in zip(chunk, results):
                if isinstance(result, Exception):
                    ...
                    continue
                item.value = result
                output_chunk.append(item)
            if output_chunk:
                yield output_chunk


class FlatProcessor(Step[list[In],list[Out]]):
    def __init__(self, executor: Executor = None):
        self.executor=executor

    @abstractmethod
    def process(self, input: In) -> list[Out]:
        ...

    async def execute(self, input: AsyncIterator[list[Item[In]]]) -> AsyncIterator[list[Item[Out]]]:
        async for chunk in input:
            results = await asyncio.gather(
                *(self._execute_process(item.value) for item in chunk),
                return_exceptions=False, # Keep return_exceptions=False until process-level error handling is implemented.
            )                            # Switch to True when exceptions need to be collected and handled per item.

            output_chunk: list[Item[Out]] = []

            for item, result in zip(chunk, results):
                if isinstance(result, Exception):
                    ...
                    pass
                for result_element in result:
                    new_item = Item(
                        origin=item.origin,
                        value=result_element,
                        context=item.context
                    )

                    output_chunk.append(new_item)
            
            if output_chunk:
                yield output_chunk

    async def _execute_process(self, input: In) -> list[Out]:
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

class AsyncFlatProcessor(Step[list[In],list[Out]]):
    def __init__(self, executor: Executor = None):
        self.executor=executor

    @abstractmethod
    async def process(self, input: In) -> list[Out]:
        ...

    async def execute(self, input: AsyncIterator[list[Item[In]]]) -> AsyncIterator[list[Item[Out]]]:
        async for chunk in input:
            results = await asyncio.gather(
                *(self.process(item.value) for item in chunk),
                return_exceptions=False, # Keep return_exceptions=False until process-level error handling is implemented.
            )                            # Switch to True when exceptions need to be collected and handled per item.

            output_chunk: list[Item[Out]] = []

            for item, result in zip(chunk, results):
                if isinstance(result, Exception):
                    ...
                    pass
                for result_element in result:
                    new_item = Item(
                        origin=item.origin,
                        value=result_element,
                        context=item.context
                    )

                    output_chunk.append(new_item)
            
            if output_chunk:
                yield output_chunk