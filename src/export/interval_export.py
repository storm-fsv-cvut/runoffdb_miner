from datetime import datetime
import os
from src.export.schemas.column_sets import RUN_INFO_COLUMNS, INTERVAL_COLUMNS
from src.utilities.utilities import czech_date
from src.export.writers import write_row_to_csv
from src.services.hydro_data import get_best_hydro_data
from src.export.filesystem import ensure_directory
from src.run_filter import RunFilter

from src.diagnostics.collector import TraceCollector
from src.diagnostics.trace import DataTrace, DataIssue
from src.diagnostics.absence_reasons import DataAbsenceReason
from src.diagnostics.severity import TraceSeverity

def generate_interval_values_csv(miner, output_path, lang="en", no_data_value="", output_trace_path=None):
    """
    Schema-driven interval export.
    Logging intentionally omitted.
    """

    if not miner.runs:
        print("No runs available within given limits.")
        return

    runs = list(miner.runs.values())
    collector = TraceCollector()

    # collect headers
    run_headers = [c.header[lang] for c in RUN_INFO_COLUMNS]
    interval_headers = [c.header[lang] for c in INTERVAL_COLUMNS]

    try:
        with open(output_path, "w", encoding="utf-8") as output_csv:
            write_row_to_csv(output_csv, run_headers + interval_headers)

            for run in runs:
                print(
                    f"\n#{run.id} – {czech_date(run.datetime)} – "
                    f"{run.locality.name} – {run.plot_id}"
                )

                ctx = {"lang": lang, "no_data_value": no_data_value}

                # run-level values
                run_line = []
                for col in RUN_INFO_COLUMNS:
                    val, issues = col.getter(run, ctx)
                    # replace None value with export-specific no_data_value
                    if val is None:
                        val = ctx["no_data_value"]
                    run_line.append(val)

                    if issues is not None:
                        trace = DataTrace(
                            issues=issues,
                            category="run_metadata",
                            level="run_info_columns",
                            severity=TraceSeverity.WARNING,
                        )
                        collector.add(
                            run_id=run.id,
                            dataset="run_info",
                            trace=trace)

                labels = {
                    "rainfall_intensity": "rainfall_intensity",
                    "rainfall_total": "rainfall_total",
                    "runoff": "runoff",
                    "sediment_concentration": "sediment_concentration",
                    "discharge": "discharge",
                    "sediment_flux": "sediment_flux",
                    "sediment_yield": "sediment_yield",
                }

                request = {k: False for k in labels}

                hydro_data, trace = get_best_hydro_data(
                    run=run,
                    request_map=request,
                    labels_map=labels,
                    return_trace=True,
                )

                # write empty line to export if no runoff-sediment data were retrieved
                if hydro_data.empty:
                    write_row_to_csv(
                        output_csv,
                        run_line + [no_data_value] * len(interval_headers),
                    )
                    # collect the trace if any
                    if trace:
                        collector.add(
                            run_id=run.id,
                            dataset="runoff_sediment_data",
                            trace=trace,
                        )

                    continue

                # ensure fillna is safe for numeric columns
                hydro_data = hydro_data.infer_objects(copy=False)
                hydro_data.fillna(no_data_value, inplace=True)

                ctx["labels"] = labels
                state = {"i": 1, "prev_index": None, "index": None}

                # interval rows
                for index, row in hydro_data.iterrows():
                    state["index"] = index

                    # evaluate each interval column once, fallback to no_data_value if None
                    interval_values = []
                    for col in INTERVAL_COLUMNS:
                        val = col.getter(run, row, ctx, state)
                        if val is None:
                            collector.add(
                                run_id=run.id,
                                dataset=f"hydro_data: {col.header[lang]} in interval #{state['index']}",
                                trace=DataTrace(
                                    issues=(DataIssue(
                                        reason=DataAbsenceReason.MISSING_DATA,
                                        source="get_best_hydro_data",
                                        details=f"missing data within series '{col.header[lang]}'",
                                        ),
                                    ),
                                    category="missing_data",
                                    level="hydro_sediment_records_derivation",
                                    severity=TraceSeverity.WARNING,
                                    ),
                            )
                            interval_values.append(no_data_value)
                        else:
                            interval_values.append(val)

                    write_row_to_csv(output_csv, run_line + interval_values)

                    state["prev_index"] = index
                    state["i"] += 1

        # do something with the collected traces
        if output_trace_path:
            collector.dump_json_by_run(output_trace_path)

    except PermissionError:
        print(f"\033[91mspecified output file '{output_path}' is being used by another application\033[00m\n")
        return


def generate_intervals_csv_by_simulator(miner, output_dir, lang="en", no_data_value="NA"):

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
            )