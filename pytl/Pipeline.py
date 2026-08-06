from .Step import Step
from .Source import Source

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
        if chunk_size != 0: self.source._set_chunk_size(chunk_size)

    @classmethod
    def builder(self):
        return self.PipelinBuilder()

    async def run(self):
        stream = self.source.stream()
        for step in self.steps:
            stream = step.execute(stream)
        async for _ in stream:
            pass