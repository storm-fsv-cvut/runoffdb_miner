from src.processing.policy import DataPolicy, ResolutionMode
from src.processing.request import ProcessingRequest
from src.processing.result import ProcessingResult
from src.diagnostics.report_collector import ReportCollector


class ProcessingEngine:
    """
    Central orchestration layer:
    - run selection
    - variable resolution
    - loading
    - transformation
    - derivation
    - diagnostics
    """

    def __init__(self, runoffdb, report=None):
        self.runoffdb = runoffdb
        self.variable_registry = runoffdb.variable_registry
        self.report = report or ReportCollector()

    def execute(self, request: ProcessingRequest,):
        policy = request.policy or self._default_policy()
        runs = self._select_runs(request.selection)

        return ProcessingResult(
            data={
                "runs": runs,
                "run_count": len(runs),
            },
            policy=policy,
            report=self.report,

            metadata={
                "policy": policy,
            },
        )

    # -------------------------
    # internal pipeline stages
    # -------------------------

    def _select_runs(self, run_filter):
        return self.runoffdb.load_runs(run_filter)

    def _default_policy(self) -> DataPolicy:
        """
        Conservative default processing behavior.

        Philosophy:
        - prefer measured data
        - allow derivation fallback
        - keep runs unless explicitly discarded
        - allow transformations needed for exports
        """

        return DataPolicy(

            # first use measured values,
            # derive only if necessary
            resolution_mode=ResolutionMode.PREFER_DIRECT,

            # retain runs unless request explicitly requires removal
            skip_runs_missing_records=False,

            # interpolation is generally useful for interval exports
            allow_interpolation=True,

            # automatic unit normalization allowed
            allow_unit_conversion=True,

            # no per-variable exceptions
            variable_overrides={},
        )
