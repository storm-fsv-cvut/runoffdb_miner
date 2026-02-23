from typing import Mapping, Iterable
from .variables_definition import VariableDefinition, VariableGroup
from ..entities.type_entities import Unit

class VariableRegistry:
    def __init__(self, variables: Iterable[VariableDefinition], units: Mapping[int, Unit]):
        self._vars: dict[str, VariableDefinition] = {
            v.key: v for v in variables
        }
        self._units = units

    def __iter__(self):
        return iter(self._vars.values())

    def __getitem__(self, key: str) -> VariableDefinition:
        return self._vars[key]

    def keys(self):
        return self._vars.keys()

    def values(self):
        return self._vars.values()

    def subregistry_by_group(self, group: VariableGroup):
        return VariableRegistry(
            variables=[v for v in self._vars.values() if group in v.groups],
            units=self._units,
        )
    def default_units(self) -> dict[str, int]:
        return {
            var.key: var.default_unit_id
            for var in self._vars.values()
            if var.default_unit_id
        }

    def acceptable_units(self) -> dict[str, tuple[int, ...]]:
        return {
            var.key: var.allowed_unit_ids
            for var in self._vars.values()
            if var.allowed_unit_ids
        }

    def default_interpolations(self) -> dict[str, str]:
        return {
            var.key: var.default_interpolation
            for var in self._vars.values()
            if var.default_interpolation
        }

    def dependencies(self) -> dict[str, tuple[dict, ...]]:
        return {
            var.key: var.dependencies
            for var in self._vars.values()
            if var.dependencies
        }

    def default_labels(self, lang: str = "en") -> dict[str, str]:
        labels = {}

        for var in self._vars.values():
            if not var.base_labels or not var.default_unit_id:
                continue

            unit = self._units[var.default_unit_id]

            labels[var.key] = (
                f"{var.base_labels[lang]} "
                f"[{unit.unit}]"
            )

        return labels

