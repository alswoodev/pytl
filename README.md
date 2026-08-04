# PyTL: Asynchronous Chunked Batch Pipeline Framework
`PyTL` is a lightweight Python-based **ETL framework** designed to resolve the fundamental trade-off between `memory overhead` and `I/O performance` during **large-scale data ingestion.**

By leveraging Python's AsyncIterator protocol, point-wise functional transformations, and internal execution buffering, PyTL enforces strict decoupled contracts for processing pipelines.

## 🎯 Architecture & Design Rationale
When processing high-throughput data streams, monolithic batching often leads to memory exhaustion (Out-Of-Memory exceptions), while naive single-item I/O incurs severe network latency overhead. PyTL addresses this via a multi-granularity lifecycle model:
```bash
+---------------------------------------------------------------------------------------+
| Framework Execution Engine                                                            |
|                                                                                       |
|  [ Source ] (Configured with Chunk Size N)                                            |
|  stream() <--- Template Method                                                        |
|    |                                                                                  |
|    +--> provide()  <-- User yields record or raw batch                                |
|    |                                                                                  |
|    v yields AsyncIterator[List[T_in]]                                                 |
|                                                                                       |
|  [ Processor ]                                                                        |
|  execute(AsyncIterator[List[T_in]]) <--- Template Method                              |
|    |                                                                                  |
|    +--> process(item: T_in)  <-- User implements point-wise logic (1-by-1)            |
|    |                                                                                  |
|    v yields AsyncIterator[List[T_out]]                                                |
|                                                                                       |
|  [ Loader ]                                                                           |
|  execute(AsyncIterator[List[T_out]]) <--- Template Method                             |
|    |                                                                                  |
|    +--> load(batch: List[T_out])  <-- User implements bulk write                      |
+---------------------------------------------------------------------------------------+
```
## 📜 Protocol Contracts

`PyTL` operates on a strict Separation of Concerns (SoC) model using the Template Method Pattern. Framework users interact exclusively with domain-level abstract methods, while internal execution templates handle asynchronous stream propagation, iteration safety, and buffering.

---

### 1. `Source[T_out]` Contract
* **Template Method (`stream() -> AsyncIterator[List[T_out]]`)**: 
  Drives the root execution loop and initializes data emission. The global variable `chunk_size` controls the amount of data consumed across the downstream pipeline.
* **User Contract (`provide() -> AsyncIterator[T_out]`)**: 
  * **Role:** Yields raw records (`T_out`) asynchronously from upstream systems (e.g., database cursors, API pagination).
  * **Responsibility:** Developers only need to `yield` individual items. Memory management is preserved by emitting items lazily rather than pulling full datasets into memory.

---

### 2. `Processor[T_in, T_out]` Contract
* **Template Method (`execute(stream) -> AsyncIterator[List[T_out], None]`)**: 
  Encapsulates stream consumption and concurrent `process()` execution. It automatically traverses the incoming `AsyncIterator`, process concurrently and streams out valid `T_out` elements.
* **User Contract (`process(item: T_in) -> Optional[T_out]`)**: 
  * **Role:** Applies pure, point-wise domain transformations on a single record.
  * **Behavioral Rules:** 
    * Returning a transformed object `T_out` forwards it down the pipeline.
  * **Responsibility:** Developers write stateless 1-by-1 conversion logic without implementing `async for` loops.

---

### 3. `Loader[T_in]` Contract
* **Template Method (`execute(stream) -> None`)**: 
  Consumes the `AsyncIterator`. Consumed chunk's length is `chunk_size` defined by `Source`. This method flushes the chunk(buffer) to the user contract.
* **User Contract (`load(batch: List[T_in]) -> None`)**: 
  * **Role:** Executes bulk persistence or downstream transmission (e.g., SQL `BULK INSERT`, vector index updates).
  * **Responsibility:** Developers process pre-chunked lists (`List[T_in]`).

## 🛠️ Usage Example
Below is a complete implementation demonstrating async streaming, transformation, and chunked batch writing using PyTL:


```python
from pytl import Source, Processor, Loader, Pipeline
from typing import AsyncIterator, List

class ExampleSource(Source[str]):
    async def provide(self) -> AsyncIterator[str]:
        for item in ["a", "b", "c", "d", "e", "f"]:
            yield item

class ExampleStreamingSource(Source[str]):
    async def provide(self) -> AsyncIterator[str]:
        import asyncio
        while True:
            yield "abc"
            await asyncio.sleep(1)

class ExampleProcessor(Processor[str, str]):
    def process(self, txt: str) -> str:
        return txt.upper()

class ExampleLoader(Loader[str]):
    async def load(self, chunk: list[str]) -> None:
        print(chunk)

batch_pipeline = (
    Pipeline
    .builder()
    .source(ExampleSource(chunk_size=2))
    .step(ExampleProcessor())
    .loader(ExampleLoader())
    .build()
)

streaming_pipeline = (
    Pipeline
    .builder()
    .source(ExampleStreamingSource(chunk_size=2))
    .step(ExampleProcessor())
    .loader(ExampleLoader())
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

```

## 🔬 Key Engineering Considerations & Roadmap
Backpressure Management: Currently, buffer clearing relies on a deterministic batch_size threshold. Future iterations will explore bounded queue channels to prevent producer-consumer rate imbalances.

Non-1:1 Transformations: Support for 1-to-N (flattening/expansion) topologies is under evaluation without breaking the point-wise abstraction contract.

Fault Isolation & DLQ: Integration of an isolated exception handler (Dead Letter Queue) to isolate malformed records during the Transform stage without terminating the stream loop.

## 📝 License & Contributions
Distributed under the MIT License. Issues and architectural discussions regarding stream processing optimization are welcome!