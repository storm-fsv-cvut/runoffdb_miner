from dataclasses import dataclass
from typing import Callable, Any

from src.entities.run import Run


@dataclass(frozen=True)
class RunColumn:
    header: dict[str, str]
    getter: Callable[['Run', dict], Any]

@dataclass(frozen=True)
class IntervalColumn:
    header: dict[str, str]
    getter: callable  # (run, df_row, ctx, state) -> value

