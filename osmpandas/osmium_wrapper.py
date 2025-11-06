"""
Wrapper for osmium tags-filter command.
"""

import subprocess
from logging import getLogger
from pathlib import Path

__all__ = ["tags_filter"]

logger = getLogger(__name__)


DEFAULT_RAILWAY_TAGS = [
    "nwr/railway",
    "r/route=train",
    "r/route_master=train",
    "r/public_transport",
]


def tags_filter(
    file_path: Path | str,
    *,
    file_suffix: str = "-railway",
    tags: list[str] = DEFAULT_RAILWAY_TAGS,
    force: bool = False,
    progress: bool = True,
) -> Path:
    file_path = Path(file_path)

    file_stem = file_path.stem
    if ".osm" in file_stem:
        file_stem = file_stem.replace(".osm", "")
    file_stem = file_stem + file_suffix
    output_file = file_path.parent.joinpath(file_stem).with_suffix(".osm.pbf")

    if output_file.exists() and not force:
        print(f"Skipping {file_path} because {output_file} already exists")
        return

    extra_args = []
    if force:
        extra_args.append("--overwrite")
    if progress:
        extra_args.append("--progress")
    else:
        extra_args.append("--no-progress")

    subprocess.run(
        [
            "osmium",
            "tags-filter",
            file_path,
            *tags,
            "-o",
            output_file,
            *extra_args,
        ]
    )

    return output_file
