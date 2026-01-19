import json
from typing import Iterable, Dict
from collections import defaultdict
from .trace import DataTrace, DataIssue, serialize_trace, serialize_issue

class TraceCollector:
    SCHEMA_VERSION = 1

    def __init__(self):
        self._traces: list[dict] = []

    def add(self, *, run_id, dataset, trace: DataTrace):
        """
        Add a single trace entry.
        """
        self._traces.append({
            "run_id": run_id,
            "dataset": dataset,
            **serialize_trace(trace),
        })
    def __iter__(self) -> Iterable[dict]:
        return iter(self._traces)

    def __len__(self) -> int:
        return len(self._traces)


    # serialization
    def to_dict(self) -> dict:
        return {
            "task": "RunoffDB data trace stack",
            "schema_version": self.SCHEMA_VERSION,
            "trace_count": len(self._traces),
            "traces": list(self._traces),
        }

    def to_dict_grouped_by_run(self) -> Dict[str, dict]:
        grouped: Dict[str, dict] = {}

        for trace in self._traces:
            run_id = trace["run_id"]
            dataset = trace.get("dataset", "unknown")

            if run_id not in grouped:
                grouped[run_id] = {
                    "trace_count": 0,
                    "datasets": defaultdict(list),
                }

            grouped[run_id]["datasets"][dataset].append(trace)
            grouped[run_id]["trace_count"] += 1

        for run_id in grouped:
            grouped[run_id]["datasets"] = dict(grouped[run_id]["datasets"])

        return grouped


    def to_json(self, *, dict: Dict, indent: int = 2) -> str:
        """
        Serialize trace stack to JSON string.
        """
        return json.dumps(
            dict,
            indent=indent,
            ensure_ascii=False,
            default=str,  # safety net for enums / timestamps
        )

    def dump_json(self, path: str, *, indent: int = 2) -> None:
        """
        Write trace stack to a JSON file.
        """
        with open(path, "w", encoding="utf-8") as f:
            f.write(self.to_json(dict=self.to_dict(), indent=indent))

    def dump_json_by_run(self, path: str, *, indent: int = 2) -> None:
        """
        Write trace stack to a JSON file.
        """
        with open(path, "w", encoding="utf-8") as f:
            f.write(self.to_json(dict=self.to_dict_grouped_by_run(), indent=indent))

    def as_metadata(self) -> list[dict]:
        """
        Backward-compatible raw trace list.
        """
        return list(self._traces)
