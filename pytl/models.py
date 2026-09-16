from dataclasses import dataclass
from typing import Any

@dataclass(slots=True)
class ItemContext:
    pipeline_cls_name: str
    pipeline_run_id: str
    batch_id: str | None

@dataclass(slots=True)
class Item[T]:
    origin: T
    value: Any
    context: ItemContext