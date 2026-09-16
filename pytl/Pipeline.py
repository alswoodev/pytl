from .Step import Step
from .Source import Source
from .models import Item, ItemContext
from typing import AsyncIterator
import uuid

class Pipeline:
    class PipelinBuilder:
        def __init__(self):
            self._source: Source = None
            self._steps: list[Step] = []

        def source(self, source: Source):
            self._source = source
            return self

        def step(self, step: Step):
            self._steps.append(step)
            return self

        def steps(self, *steps: Step):
            for step in steps:
                self._steps.append(step)
            return self

        def build(self):
            if self._source is None: raise Exception("Source is required")
            return Pipeline(
                self._source,
                self._steps,
            )

    def __init__(self, 
                 source: Source, 
                 steps: list[Step], 
                 chunk_size: int = 0):
        self.source = source
        self.steps = steps
        self.batch_id: int = 1
        if chunk_size != 0: self.source._set_chunk_size(chunk_size)

    @classmethod
    def builder(self):
        return self.PipelinBuilder()

    async def run(self):
        run_id = uuid.uuid4()
        stream = self.source.execute()
        stream = self._attatch_context(stream, run_id)
        for step in self.steps:
            stream = step.execute(stream)
        async for _ in stream:
            pass

    async def _attatch_context(self, stream: AsyncIterator[list], run_id: str) -> AsyncIterator[list[Item]]:
        item_ctx = ItemContext(
            pipeline_cls_name=self.__class__.__name__,
            pipeline_run_id=run_id,
            batch_id=self.batch_id
        )
        self.batch_id += 1
        async for batch in stream:
            yield [Item(
                origin = item,
                value = item,
                context = item_ctx
            ) for item in batch]