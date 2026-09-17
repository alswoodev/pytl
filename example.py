from pytl import Source, Processor, Loader, Pipeline

class ExampleSource(Source[str]):
    async def stream(self):
        for item in ["a", "b", "c", "d", "e", "f"]:
            yield item

class ExampleStreamingSource(Source[str]):
    async def stream(self):
        import asyncio
        while True:
            yield "abc"
            await asyncio.sleep(1)

class ExampleProcessor(Processor[str, str]):
    def process(self, txt: str):
        return txt.upper()

class ExampleLoader(Loader[str]):
    async def load(self, chunk: list[str]):
        print(chunk)

batch_pipeline = (
    Pipeline
    .builder()
    .source(ExampleSource(chunk_size=2))
    .steps(ExampleProcessor(), ExampleLoader())
    .build()
)

streaming_pipeline = (
    Pipeline
    .builder()
    .source(ExampleStreamingSource(chunk_size=2))
    .steps(ExampleProcessor(), ExampleLoader())
    .build()   
)

if __name__ == "__main__":
    import asyncio
    asyncio.run(batch_pipeline.run())
    #   print result
    #    ['A', 'B']
    #    ['C', 'D']
    #    ['E', 'F']

    #asyncio.run(streaming_pipeline.run())
    #   print result
    #  ['ABC', 'ABC']
    #  ['ABC', 'ABC']
    #  ['ABC', 'ABC']
    #       ...


