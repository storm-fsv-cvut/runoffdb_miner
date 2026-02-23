
from typing import Iterable
from dataclasses import dataclass
from datetime import time

from .entities.type_entities import *


@dataclass(frozen=True)
class RunFilter:
    date_from: datetime | None = None
    date_to: datetime | None = None
    simulators: int | Iterable[int] | None = None
    localities: int | Iterable[int] | None = None
    crops: int | Iterable[int] | None = None
    run_id: int | Iterable[int] | None = None
    with_runoff_only: bool = False
    limit: int | None = None

    def __str__(self) -> str:
        parts = []

        def fmt_iterable(value):
            if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
                return f"[{', '.join(map(str, value))}]"
            return str(value)

        if self.date_from:
            parts.append(f"date_from = {self.date_from.isoformat()}\n")
        if self.date_to:
            parts.append(f"date_to = {self.date_to.isoformat()}\n")

        if self.simulators is not None:
            parts.append(f"simulators = {fmt_iterable(self.simulators)}\n")
        if self.localities is not None:
            parts.append(f"localities = {fmt_iterable(self.localities)}\n")
        if self.crops is not None:
            parts.append(f"crops = {fmt_iterable(self.crops)}\n")
        if self.run_id is not None:
            parts.append(f"run_id = {fmt_iterable(self.run_id)}\n")

        if self.with_runoff_only:
            parts.append("with_runoff_only = True")

        if self.limit is not None:
            parts.append(f"limit = {self.limit}\n")

        return f"RunFilter:\n{''.join(parts)}" if parts else "RunFilter(<no constraints>)"

    def matches(self, run) -> bool:
        if self.run_id is not None:
            if run.id not in as_list(self.run_id):
                return False

        if self.date_from is not None:
            if run.datetime < datetime.combine(self.date_from, time.min):
                return False

        if self.date_to is not None:
            if run.datetime > datetime.combine(self.date_to, time.max):
                return False

        if self.simulators is not None:
            if run.simulator_id not in as_list(self.simulators):
                return False

        if self.localities is not None:
            if run.locality_id not in as_list(self.localities):
                return False

        if self.crops is not None:
            if run.crop_id not in as_list(self.crops):
                return False

        if self.with_runoff_only and run.ttr is None:
            return False

        return True

    def is_id_only(self) -> bool:
        return (
            self.run_id is not None and
            self.date_from is None and
            self.date_to is None and
            self.simulators is None and
            self.localities is None and
            self.crops is None and
            not self.with_runoff_only and
            self.limit is None
        )