from ..Loader import Loader
from typing import get_args, Any
from collections import defaultdict
import asyncio

class CompositeLoader(Loader[Any]):
    def __init__(self, *loaders: Loader):
        self._loaders_by_type: dict[type, list[Loader]] = defaultdict(list)

        for loader in loaders:
            input_type = self._resolve_input_type(loader)
            self._loaders_by_type[input_type].append(loader)

    @staticmethod
    def _resolve_input_type(loader: Loader) -> type:
        orig_class = getattr(loader, "__orig_class__", None)

        if orig_class is not None:
            args = get_args(orig_class)

            if args:
                return args[0]

        for base in getattr(loader.__class__, "__orig_bases__", ()):
            args = get_args(base)
            if args and args[0] is not Any:
                return args[0]

        raise TypeError(
            f"{loader.__class__.__name__} must be instantiated as Loader[T]"
        )
    
    async def load(self, chunk: list[Any]) -> None:

        # type -> items
        buckets: dict[type, list[Any]] = defaultdict(list)

        for item in chunk:
            buckets[type(item)].append(item)
        tasks = []

        for item_type, items in buckets.items():
            loaders = self._loaders_by_type.get(item_type)
            if not loaders:
                continue

            for loader in loaders:
                tasks.append(loader.load(items))

        if tasks:
            await asyncio.gather(*tasks)