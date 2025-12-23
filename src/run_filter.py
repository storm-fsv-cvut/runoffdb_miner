
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
