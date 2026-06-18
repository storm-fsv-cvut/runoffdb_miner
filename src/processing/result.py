from dataclasses import dataclass
from typing import Any, Dict, Optional

from src.diagnostics.report_collector import ReportCollector

@dataclass
class ProcessingResult:
    data: Any

    # DataReport root object
    report: ReportCollector

    metadata: Dict[str, Any]

    # optional quality indicator
    completeness: Optional[float] = None

    run_count: Optional[int] = None