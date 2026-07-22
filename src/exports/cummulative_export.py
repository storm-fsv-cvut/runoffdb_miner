from datetime import datetime
import os
from pandas import isna

from src.schemas.column_schemas import resolve_header_of_column
from src.schemas.run_properties_columns import RUN_PROPERTIES
from src.schemas.hydro_sediment_columns import HYDRO_SEDIMENT_INTERVALS
from src.setup.variables_definition import VariableGroup

from src.utilities.utilities import czech_date
from src.exports.writers import write_row_to_csv
from src.services.hydro_data import get_hydro_sediment_timeline
from src.exports.filesystem import ensure_directory
from src.filters.run_filter import RunFilter

from src.diagnostics.issue import DataIssue
from src.diagnostics.trace import DataTrace, create_trace
from src.diagnostics.absence_reasons import DataAbsenceReason
from src.diagnostics.severity import IssueSeverity


def generate_interval_values_csv(
    processing_result,
    runoffdb,
    output_path,
    lang="en",
    no_data_value="",
    output_trace_path=None,
):
    """
    Schema-driven interval exports.
    """

    # report provided by engine
    report = processing_result.report
    # policy provided by engine
    policy = processing_result.policy

    runs = list(processing_result.data["runs"].values())

    if not runs:
        print("No runs available fitting used filter.")

        trace = DataTrace(
            source="generate_interval_values_csv",
            details="retrieving runs for exports",
            issues=[DataIssue(
                reason=DataAbsenceReason.NO_RUN_SELECTED,
                details="no runs available matching used filter",
                severity=IssueSeverity.WARNING
            )]
        )
        report.add_trace(trace)
        return

    var_registry = (
        runoffdb.variable_registry
        .subregistry_by_group(
            VariableGroup.HYDRO_SEDIMENT
        )
    )

    labels = var_registry.default_labels()
    request = {k: False for k in labels}

    # acquire column headers
    run_headers = [
        resolve_header_of_column(c, var_registry, lang)
        for c in RUN_PROPERTIES
    ]

    interval_headers = [
        resolve_header_of_column(c, var_registry, lang)
        for c in HYDRO_SEDIMENT_INTERVALS
    ]

    # exports level execution context
    general_ctx = {
        "lang": lang,
        "no_data_value": no_data_value,
    }

    try:

        with open(output_path, "w", encoding="utf-8") as output_csv:
            # write the headers to output
            write_row_to_csv(
                output_csv,
                run_headers + interval_headers,
            )

            for run in runs:
                # run specific cash for data
                run_ctx = {}
                # local context where the global context keys are directly accessible and includes run cash
                local_ctx = {
                    **general_ctx,
                    "run_ctx": run_ctx,
                }

                print(
                    f"\n#{run.id} – {czech_date(run.datetime)} – "
                    f"{run.locality.name} – [{run.plot_id}]"
                )
    except PermissionError:
        print(f"\033[91mspecified output file '{output_path}' is being used by another application\033[00m\n")

        return