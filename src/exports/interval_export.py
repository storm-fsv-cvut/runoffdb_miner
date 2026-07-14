from datetime import datetime
import os
from pandas import isna

from src.schemas import resolve_header_of_column
from src.schemas import RUN_PROPERTIES
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

                # -------------------
                # run-level columns
                # -------------------
                run_line = []
                run_trace = create_trace(
                            source="generate_interval_values_csv",
                            owner=run,
                            )

                for col in RUN_PROPERTIES:

                    val, col_trace = col.getter(run, local_ctx)

                    if val is None:
                        val = no_data_value

                    if col_trace is not None:
                        run_trace.traces.append(col_trace)

                    run_line.append(val)

                # -------------------
                # hydro + sediment data
                # -------------------

                hydro_data, hydro_trace = get_hydro_sediment_timeline(run=run)

                hydro_data = hydro_data.infer_objects(copy=False)

                print(hydro_data)
                if hydro_trace:
                    run_trace.traces.append(hydro_trace)

                # prepare interval context dict
                interval_ctx = {
                    "i": 1,
                    "prev_index": None,
                    "index": None,
                }
                # put it inside run context
                run_ctx["interval_ctx"] = interval_ctx

                for index, row in hydro_data.iterrows():

                    interval_ctx["index"] = index

                    interval_values = []

                    for col in HYDRO_SEDIMENT_INTERVALS:

                        val = col.getter(run, row, local_ctx,)

                        if val is None or isna(val):
                            val = no_data_value

                        interval_values.append(val)

                    write_row_to_csv(
                        output_csv,
                        run_line + interval_values,
                    )

                    interval_ctx["prev_index"] = index
                    interval_ctx["i"] += 1

                report.add_trace(run_trace)

        if output_trace_path:
            report.dump_to_json(output_trace_path)

    except PermissionError:
        print(f"\033[91mspecified output file '{output_path}' is being used by another application\033[00m\n")

        return


def generate_intervals_csv_by_simulator(miner, output_dir, lang="en", no_data_value="", output_trace_path=None):

    ensure_directory(output_dir)

    simulator_ids = (
            miner.filter.simulators
            or list(miner.runoffdb.simulators.keys())
    )

    for simulator_id in simulator_ids:
        query = RunFilter(simulators=simulator_id)

        with miner.scoped_runs(query):
            if not miner.runs:
                continue

            filename = (
                f"runoff_sediment_intervals_"
                f"{datetime.now().strftime('%Y%m%d')}_"
                f"sim{simulator_id}_{lang}.csv"
            )
            output_path = os.path.join(output_dir, filename)

            generate_interval_values_csv(
                miner,
                output_path,
                lang=lang,
                no_data_value=no_data_value,
                output_trace_path=output_trace_path
            )
