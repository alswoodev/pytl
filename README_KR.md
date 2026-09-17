# PyTL

PyTL은 Python으로 ETL 파이프라인을 구성하기 위한 가벼운 비동기 프레임워크입니다. 데이터는 청크 단위로 흐르며, `Source`, `Processor`, `Loader`를 조합해 수집, 변환, 저장 단계를 정의할 수 있습니다.

## 주요 특징

- 비동기 iterable 기반의 청크 처리
- 동기 처리 함수와 비동기 처리 함수 모두 지원
- 항목별 변환 결과에 원본 값과 실행 컨텍스트 보존
- 하나의 입력을 여러 출력으로 확장하는 flat processor 지원
- 값의 실제 타입별로 loader를 라우팅하는 `CompositeLoader` 지원
- 타입 힌트를 활용한 제네릭 API

## 요구 사항 및 설치

현재 `pytl/models.py`는 Python 3.12부터 지원되는 몇몇 문법(PEP 695 제네릭 문법(`class Item[T]`) 등)을 사용하므로, 실제 사용에는 Python 3.12 이상을 권장합니다.

저장소에서 설치하려면 다음을 실행합니다.

```bash
python -m pip install -e .
```

테스트 의존성까지 설치한 뒤 테스트를 실행할 수 있습니다.

```bash
python -m pip install pytest pytest-asyncio
pytest
```

## 빠른 시작

각 구성 요소는 자신이 처리할 타입을 제네릭으로 선언하고, 필요한 메서드만 구현합니다.

```python
import asyncio
from typing import AsyncIterator

from pytl import Loader, Pipeline, Processor, Source


class NumberSource(Source[int]):
    async def stream(self) -> AsyncIterator[int]:
        for number in range(1, 6):
            yield number


class StringProcessor(Processor[int, str]):
    def process(self, input: int) -> str:
        return f"number={input}"


class PrintLoader(Loader[str]):
    async def load(self, chunk: list[str]) -> None:
        print(chunk)


async def main() -> None:
    pipeline = (
        Pipeline.builder()
        .source(NumberSource(chunk_size=2))
        .steps(StringProcessor(), PrintLoader())
        .build()
    )

    await pipeline.run()


asyncio.run(main())
```

출력은 다음처럼 소스의 청크 크기를 따릅니다.

```text
['number=1', 'number=2']
['number=3', 'number=4']
['number=5']
```

## 처리 모델

파이프라인은 다음 순서로 데이터를 전달합니다.

```text
Source.execute()
    -> context 부착
    -> Step.execute() ...
    -> 마지막 Step까지 소비
```

### Source

`Source[T]`는 `stream()`에서 값을 하나씩 비동기적으로 생성합니다. `chunk_size`만큼 값이 모이면 하나의 청크로 내보내며, 스트림이 끝날 때 남은 값도 별도의 청크로 flush합니다.

```python
class EventSource(Source[dict]):
    async def stream(self):
        for event in events:
            yield event


source = EventSource(chunk_size=100)
```

`chunk_size`는 1 이상이어야 합니다.

### Processor

다음 네 가지 processor를 제공합니다.

| 클래스 | 구현할 메서드 | 용도 |
| --- | --- | --- |
| `Processor[In, Out]` | `def process(...) -> Out` | 동기 일대일 변환 |
| `AsyncProcessor[In, Out]` | `async def process(...) -> Out` | 비동기 일대일 변환 |
| `FlatProcessor[In, Out]` | `def process(...) -> list[Out]` | 동기 일대다 변환 |
| `AsyncFlatProcessor[In, Out]` | `async def process(...) -> list[Out]` | 비동기 일대다 변환 |

동기 processor는 기본적으로 `asyncio.to_thread()`를 사용해 각 항목을 병렬 처리합니다. 별도 `concurrent.futures.Executor`를 전달하면 해당 executor를 사용합니다.

```python
class SplitWords(FlatProcessor[str, str]):
    def process(self, input: str) -> list[str]:
        return input.split()
```

일대일 동기 processor에서 특정 항목의 `process()`가 예외를 발생시키면 해당 항목은 결과에서 제외되고 같은 청크의 성공한 항목은 계속 처리됩니다. 반면 flat processor의 에서는 특정 항목의 `process()`가 예외를 발생시키면, 변환과정 자체가 실패한 것으로 간주되어 일대다 변환 자체가 이루어지지 않습니다.

### Loader

`Loader[T]`는 청크의 `Item.value` 목록을 `load()`에 전달합니다.

```python
class DatabaseLoader(Loader[dict]):
    async def load(self, chunk: list[dict]) -> None:
        await save_to_database(chunk)
```

`load()`가 끝나면 loader는 입력 `Item` 청크를 그대로 다음 단계로 전달합니다. 따라서 loader 뒤에 다른 step을 연결할 수도 있습니다. `Pipeline.run()`은 마지막 출력이 생길 때까지 전체 스트림을 소비합니다.

### CompositeLoader

서로 다른 타입을 각각의 loader로 보내려면 `CompositeLoader`를 사용합니다.

```python
class IntLoader(Loader[int]):
    async def load(self, chunk: list[int]) -> None:
        await save_integers(chunk)


class TextLoader(Loader[str]):
    async def load(self, chunk: list[str]) -> None:
        await save_texts(chunk)


loader = CompositeLoader(IntLoader(), TextLoader())
```

각 입력 값은 `type(value)`와 정확히 일치하는 loader 그룹으로 라우팅됩니다. 현재 구현은 상속 관계를 따라 loader를 찾지 않으며, 매칭되는 loader가 없는 값은 건너뜁니다. 하나의 타입에 여러 loader를 등록하면 해당 타입의 청크가 모든 loader에 전달됩니다.

## Item과 실행 컨텍스트

파이프라인은 소스 값마다 다음 형태의 `Item`을 생성합니다.

```python
Item(
    origin=source_value,
    value=current_value,
    context=ItemContext(
        pipeline_cls_name="Pipeline",
        pipeline_run_id="...",
        batch_id=1,
    ),
)
```

- `origin`: 최초 소스 값
- `value`: 현재 step까지 변환된 값
- `context`: 파이프라인 실행 및 원본 청크 정보

일대다 변환에서도 `origin`과 `context`를 통해 결과와 원본의 관계를 추적할 수 있습니다.

## API 개요

- `Pipeline.builder().source(source).step(step).build()`: step을 하나씩 등록
- `Pipeline.builder().source(source).steps(*steps).build()`: 여러 step을 한 번에 등록
- `await pipeline.run()`: 파이프라인 실행
- `Source[T]`: 비동기 입력 스트림과 청크 크기 정의
- `Processor`, `AsyncProcessor`: 일대일 변환
- `FlatProcessor`, `AsyncFlatProcessor`: 일대다 변환
- `Loader[T]`: 청크 저장 또는 외부 시스템 전달
- `CompositeLoader`: 값의 타입별 loader 분배

## 개발

```bash
pytest -q
```

예제 실행:

```bash
python example.py
```

`example.py`에는 유한한 배치 파이프라인과 계속 값을 생성하는 스트리밍 파이프라인 예제가 함께 들어 있습니다. 스트리밍 예제는 무한 루프이므로 직접 실행할 때는 `asyncio.run(streaming_pipeline.run())` 주석을 해제하고 별도로 종료해야 합니다.