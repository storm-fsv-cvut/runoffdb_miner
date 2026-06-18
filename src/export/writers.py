import json
import os
import locale

from .filesystem import sanitize_path

def write_row_to_csv(fileref, towrite, lined="\n", celld=";"):
    linestring = ""
    i = 0
    for item in towrite:
        if isinstance(item, float):
            linestring += locale.format_string('%.3f', item)
        else:
            linestring += f"{item}"

        if i < len(towrite)-1:
            linestring += celld
        else:
            linestring += lined
        i += 1
    fileref.write(linestring)
    return
def write_run_metadata_json(run, run_dir, lang):
    with open(os.path.join(run_dir, f"{run.id}.json"), "w") as f:
        json.dump(run.get_metadata(lang), f, ensure_ascii=False, indent=4)

def write_all_run_records_to_csv(run, dir, lang, no_data_value):
    import  pandas as pd
    # loop through all phenomena and if measurement exists go through it's records
    for phid in run.runoffdb.get_all_phenomena_ids():
        msrmnts = run.get_measurements(phid)
        if msrmnts is not None:
            for ms in msrmnts:
                # loop through units and if record exists export it
                for uid in run.runoffdb.get_all_units_ids():
                    rcrds = ms.get_records(unit_id=uid)
                    if rcrds is not None:
                        recids = []
                        for rec in rcrds:
                            recids.append(rec.id)
                            if rec.record_type_id != 99:
                                # the dataframe is TimeDelta indexed if is_timeline attribute is True
                                index_column = "time" if rec.is_timeline else None
                                index = True if rec.is_timeline else False
                                # column_headers = ["time"] if rec.is_timeline else []
                                column_headers = []
                                rec_filename = sanitize_path(f"{rec.id}-{rec.unit.name[lang]}-[{rec.unit.unit}]")

                                # try:
                                data_df = rec.get_data("value", index_column=index_column)
                                # except DataframeEmptyError as e:
                                #     print(f"\t{e.message}")
                                #     print(f"rec_filename: {rec_filename}")
                                #     print(f"dir: {dir}")
                                #     with open(os.path.join(dir, f"{rec_filename}.csv"), "w") as f:
                                #         f.write(e.message)
                                # else:
                                if data_df is not None:

                                    column_headers.append(f"{rec.unit.name[lang]} [{rec.unit.unit}]")
                                    column_headers.append(f"{rec.unit_rel_x.name[lang]} [{rec.unit_rel_x.unit}]") \
                                        if rec.related_value_x_unit_id is not None else None
                                    column_headers.append(f"{rec.unit_rel_y.name[lang]} [{rec.unit_rel_y.unit}]")\
                                        if rec.related_value_y_unit_id is not None else None
                                    column_headers.append(f"{rec.unit_rel_z.name[lang]} [{rec.unit_rel_z.unit}]")\
                                        if rec.related_value_z_unit_id is not None else None

                                    # format the TimeDelta index to desired format (get rid of the '0 days')
                                    if pd.api.types.is_timedelta64_dtype(data_df.index):
                                        data_df.index = data_df.index.map(lambda
                                                                              x: f"{x.components.hours:02}:{x.components.minutes:02}:{x.components.seconds:02}")

                                    print \
                                        (f"#{rec.id}: {', '.join(column_headers)} ({rec.record_type.name[lang]}){' *' if rec.is_timeline else ''}")

                                    # try:
                                    local_seps = {"celld": {"cz": ";", "en": ","}, "decd": {"cz": ",", "en": "."}}
                                    data_df.to_csv(os.path.join(dir, rec_filename + ".csv"),
                                    index = index,
                                    sep = local_seps["celld"][lang],
                                    decimal = local_seps["decd"][lang],
                                    header = column_headers)
                                    # except ValueError:
                                    #     print(data_df)
                                else:
                                    print\
                                        (f"record {rec.id} ({rec.unit.name[lang]} [{rec.unit.unit}]) gains no data on load")
                                    with open(os.path.join(dir, rec_filename +".csv"), "w") as f:
                                        f.write\
                                            (f"record {rec.id} ({rec.unit.name[lang]} [{rec.unit.unit}]) gains no data on load")
                        # print(f"{phid} - {len(ms.records)} ({', '.join([str(rid) for rid in recids])})")
