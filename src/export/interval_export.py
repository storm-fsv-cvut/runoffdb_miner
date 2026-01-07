from src.export.schemas.column_sets import RUN_INFO_COLUMNS, INTERVAL_COLUMNS
from src.utilities.utilities import czech_date
from src.export.writers import write_row_to_csv
from src.exceptions import RecordSetNotComplete
from src.services.hydro_data import get_best_hydro_data


def generate_interval_values_csv(miner, output_path, lang="en", no_data_value="NA"):
    """
    Schema-driven interval export.
    Logging intentionally omitted.
    """

    if not miner.runs:
        print("No runs available within given limits.")
        return

    runs = list(miner.runs.values())

    # collect headers
    run_headers = [c.header[lang] for c in RUN_INFO_COLUMNS]
    interval_headers = [c.header[lang] for c in INTERVAL_COLUMNS]

    try:
        output_csv = open(output_path, "w", encoding="utf-8")
    except PermissionError:
        print(f"\033[91mspecified output file '{output_path}' is being used by another application\033[00m\n")
        return

    write_row_to_csv(output_csv, run_headers + interval_headers)

    # collect data
    for run in runs:
        print(
            f"\n#{run.id} – {czech_date(run.datetime)} – "
            f"{run.locality.name} – {run.plot_id}"
        )

        # run-level values
        ctx = {
            "lang": lang,
            "no_data_value": no_data_value,
        }

        run_line = [
            col.getter(run, ctx) if col.getter(run, ctx) is not None else no_data_value
            for col in RUN_INFO_COLUMNS
        ]

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

        try:
            hydro_data = get_best_hydro_data(
                run=run,
                request_map=request,
                labels_map=labels
            )
        except RecordSetNotComplete:
            continue
        print(hydro_data.head())

        hydro_data.fillna(no_data_value, inplace=True)

        if hydro_data.empty:
            empty_row = [no_data_value] * len(interval_headers)
            write_row_to_csv(output_csv, run_line + empty_row)
            continue

        # interval rows
        state = {
            "i": 1,
            "prev_index": None,
            "index": None,
        }

        ctx["labels"] = labels

        print(
            f"hydro_data rows={len(hydro_data)}, \n"
            f"columns={list(hydro_data.columns)}, \n"
            f"index_type={type(hydro_data.index)}"
        )

        for index, row in hydro_data.iterrows():
            state["index"] = index

            interval_values = [
                col.getter(run, row, ctx, state)
                if col.getter(run, row, ctx, state) is not None
                else no_data_value
                for col in INTERVAL_COLUMNS
            ]

            write_row_to_csv(output_csv, run_line + interval_values)

            state["prev_index"] = index
            state["i"] += 1

    output_csv.close()