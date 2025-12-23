import os
import typing
import datetime

from ..entities.run import Run


def sanitize_path(path_str):
    from re import  sub
    # Replace invalid characters for both Windows and Unix-like systems
    sanitized = sub(r'[<>:"/\\|?* ]', '_', path_str)
    return sanitized


def ensure_directory(path: str):
    try:
        os.mkdir(path) if not os.path.isdir(path) else None
    except OSError as error:
        print("Selected directory for the dump does not exist and it's not possible to create it.")
        print("Dump failed.")
        return False
    else:
        return True

def make_day_dir(root_path: str, day: datetime.date, format: str = '%Y-%m-%d'):
    day_dir = os.path.join(root_path, day.strftime(format))
    try:
        os.mkdir(day_dir)
    except OSError as error:
        print(f"Directory for the day '{day_dir}' does not exist and it's not possible to create it.")
        print("All simulation runs on this day will be skipped")
        return False
    else:
        return day_dir

def make_run_dir(root_path, run: Run, lang: str = "en"):
    sim_dir_name = sanitize_path \
        (f"{run.id}-{run.locality.name}-{run.crop.name[lang]}-{run.plot_id}-{run.run_type.name[lang]}")
    sim_dir = os.path.join(root_path, sim_dir_name)
    try:
        os.mkdir(sim_dir)
    except OSError as error:
        print(f"Directory for the simulation run '{sim_dir}' does not exist and it's not possible to create it.")
        print("The run will be skipped")
        return False
    else:
        return sim_dir