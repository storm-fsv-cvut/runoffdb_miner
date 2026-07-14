from collections import defaultdict
import json

from src.diagnostics.trace import DataTrace
from src.diagnostics.issue import DataIssue
from src.diagnostics.severity import IssueSeverity


class ReportCollector:
    """
    Stores provenance trees produced during processing.
    All diagnostics are derived from traces.
    """

    def __init__(self):

        self._traces: list[DataTrace] = []

        self._traces_by_dataset: dict[
            str,
            list[DataTrace]
        ] = defaultdict(list)

        self._traces_by_owner: dict[
            str,
            dict[int, list[DataTrace]]
        ] = defaultdict(lambda: defaultdict(list))

    # =====================================================
    # registration
    # =====================================================

    def add_trace(self, trace: DataTrace):
        """
        Register root trace.
        """

        self._traces.append(trace)

        if trace.dataset:
            self._traces_by_dataset[trace.dataset].append(trace)

        if trace.owner_class is not None and trace.owner_id is not None:
            self._traces_by_owner[trace.owner_class][trace.owner_id].append(trace)

    # =====================================================
    # accessors
    # =====================================================

    @property
    def traces_by_owner(self):
        return self._traces_by_owner

    @property
    def traces_by_dataset(self):
        return self._traces_by_dataset

    # =====================================================
    # iteration
    # =====================================================

    def iter_root_traces(self):
        for trace in self._traces:
            yield trace

    def iter_traces(self):
        for root in self.iter_root_traces(): yield from iter_trace_tree(root)

    def iter_issues(self):
        for root in self.iter_root_traces(): yield from iter_trace_issues(root)

    # =====================================================
    # summaries
    # =====================================================

    @property
    def issues(self) -> list[DataIssue]:
        return list(self.iter_issues())

    @property
    def trace_count(self) -> int:
        return sum(1 for _ in self.iter_traces())

    @property
    def issue_count(self) -> int:
        return sum(1 for _ in self.iter_issues())

    @property
    def max_severity(self):
        severities = [i.severity for i in self.iter_issues()]

        if not severities:
            return IssueSeverity.INFO

        return max(severities)

    @property
    def has_errors(self) -> bool:
        return any(
            i.severity == IssueSeverity.ERROR
            for i in self.iter_issues()
        )

    # =====================================================
    # filtering (fixed to current model)
    # =====================================================

    def get_traces_by_owner(
        self,
        owner_class: str,
        owner_id: int,
    ) -> list[DataTrace]:

        return self._traces_by_owner.get(owner_class, {}).get(owner_id, [])

    def get_traces_by_dataset(
        self,
        dataset: str,
    ) -> list[DataTrace]:

        return self._traces_by_dataset.get(dataset, [])

    def dump_to_json(self, path: str):
        """
        Export full trace forest to JSON.
        """

        data = {
            "traces": [
                t.to_dict()
                for t in self._traces
            ]
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
def iter_trace_tree(trace):
    """ Depth-first traversal. """

    yield trace
    for child in trace.traces: yield from iter_trace_tree(child)

def iter_trace_issues(trace):
    """ Yield all issues from trace subtree. """

    for issue in trace.issues: yield issue
    for child in trace.traces: yield from iter_trace_issues(child)
