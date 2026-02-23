from dataclasses import dataclass
from typing import Callable, Any, Union

from src.entities.run import Run

HeaderType = Union[
    dict[str, str],
    Callable[[dict, str], str]
]

@dataclass(frozen=True)
class RunColumn:
    header: HeaderType
    getter: Callable[['Run', dict], Any]

@dataclass(frozen=True)
class IntervalColumn:
    header: HeaderType
    getter: callable  # (run, df_row, ctx, state) -> value

