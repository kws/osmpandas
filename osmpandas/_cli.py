from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path

import click
from tqdm import tqdm


@contextmanager
def progress_counter():
    counter = defaultdict(lambda: 0)

    def format_progress() -> str:
        return ", ".join(f"{key}: {value:^10,d}" for key, value in counter.items())

    with tqdm() as pbar:

        def progress_callback(**kwargs):
            for key, value in kwargs.items():
                counter[key] += value
            pbar.set_description(format_progress())

        yield progress_callback


@click.group()
def cli():
    pass


@cli.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.option("--force", is_flag=True, help="Force overwrite of existing output file")
@click.option("--no-progress", is_flag=True, help="Hide the progress bar")
@click.option("--file-suffix", default=None, help="Suffix for the output file")
def filter(input_file: str, *, force: bool, no_progress: bool, file_suffix: str):
    from osmpandas.osmium_wrapper import tags_filter

    tags_filter(input_file, force=force, progress=not no_progress, file_suffix=file_suffix)


@cli.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path(), default=None)
def convert(input_file: str, output_file: str | None):
    from osmpandas.osm_parser import convert_osm_to_parquet

    input_file = Path(input_file)

    if output_file is None:
        if input_file.name.endswith(".osm.pbf"):
            output_file = input_file.name.replace(".osm.pbf", ".osmpkg")
        else:
            output_file = input_file.with_suffix(".osmpkg")

    with progress_counter() as progress_callback:
        convert_osm_to_parquet(input_file, output_file, progress_callback=progress_callback)


@cli.command()
@click.argument("input_file", type=click.Path(exists=True))
def load(input_file: str):
    from osmpandas.package import OSMDataPackage

    data = OSMDataPackage.load(input_file)
    print(data)


@cli.command()
def check():
    from osmpandas.osmium_wrapper import check_osmium

    if check_osmium():
        print("\n✅ osmium is installed")
    else:
        print("\n❌ osmium is not installed")


if __name__ == "__main__":
    cli()
