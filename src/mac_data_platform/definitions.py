from pathlib import Path

from dagster import definitions, load_from_defs_folder


@definitions
def defs():  # noqa: ANN201
    return load_from_defs_folder(path_within_project=Path(__file__).parent)
