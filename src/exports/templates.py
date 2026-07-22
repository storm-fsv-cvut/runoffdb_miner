import os
from datetime import datetime
from dataclasses import replace

from src.processing.engine import ProcessingEngine
from src.exports.filesystem import ensure_directory
from .interval_export import generate_interval_values_csv

def interval_export(runoffdb, request, output_path, lang, no_data_value, output_trace_path):

    engine = ProcessingEngine(runoffdb)
    result = engine.execute(request)

    generate_interval_values_csv(processing_result=result,
                                 runoffdb=runoffdb,
                                 output_path=output_path,
                                 lang=lang,
                                 no_data_value=no_data_value,
                                 output_trace_path=output_trace_path)

def interval_export_by_simulator(runoffdb, request, output_dir, lang, no_data_value, output_trace_path):

    ensure_directory(output_dir)

    simulator_ids = (
        list(request.selection.simulators)
        if request.selection.simulators
        else list(runoffdb.simulators.keys())
    )

    engine = ProcessingEngine(runoffdb)

    for simulator_id in simulator_ids:
        selection = replace(
            request.selection,
            simulators=[simulator_id],
        )

        simulator_request = replace(
            request,
            selection=selection,
        )

        processing_result = engine.execute(simulator_request)

        if not processing_result.data["runs"]:
            continue

        filename = f"runoff_sediment_intervals_{datetime.now():%Y%m%d}_sim{simulator_id}_{lang}.csv"
        output_path = os.path.join(output_dir, filename)

        generate_interval_values_csv(
            processing_result=processing_result,
            runoffdb=runoffdb,
            output_path=output_path,
            lang=lang,
            no_data_value=no_data_value,
            output_trace_path=output_trace_path,
        )