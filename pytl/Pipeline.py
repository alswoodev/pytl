from .Step import Step
from .Source import Source
from .Loader import Loader

class Pipeline:
    class PipelinBuilder:
        def __init__(self):
            self._source: Source = None
            self._steps: list[Step] = []
            self._loader: Loader = None

        def source(self, source: Source):
            self._source = source
            return self

        def step(self, step: Step):
            self._steps.append(step)
            return self

        def loader(self, loader: Loader):
            self._loader = loader
            return self

        def build(self):
            if self._source is None or self._loader is None: raise Exception("Source and Loader are required")
            return Pipeline(
                self._source,
                self._steps,
                self._loader
            )

    def __init__(self, 
                 source: Source, 
                 steps: list[Step], 
                 loader: Loader,
                 chunk_size: int = 0):
        self.source = source
        self.steps = steps
        self.loader = loader
        if chunk_size != 0: source.set_chunk_size(chunk_size)

    @classmethod
    def builder(self):
        return self.PipelinBuilder()

    async def run(self):
        stream = self.source.stream()
        for step in self.steps:
            stream = step.execute(stream)
        await self.loader.execute(stream)