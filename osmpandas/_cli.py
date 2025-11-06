from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path

import click
import click_log
from tqdm import tqdm

click_log.basic_config()


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
@click_log.simple_verbosity_option()
def filter(input_file: str, *, force: bool, no_progress: bool, file_suffix: str):
    from osmpandas.osmium_wrapper import tags_filter

    tags_filter(input_file, force=force, progress=not no_progress, file_suffix=file_suffix)


@cli.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path(), default=None)
@click_log.simple_verbosity_option()
def convert(input_file: str, output_file: str | None):
    from osmpandas.osm_parser import convert_osm_to_parquet

    input_file = Path(input_file)

    if output_file is None:
        if input_file.name.endswith(".osm.pbf"):
            name = input_file.name.replace(".osm.pbf", ".osmpkg")
            output_file = input_file.with_name(name)
        else:
            output_file = input_file.with_suffix(".osmpkg")

    with progress_counter() as progress_callback:
        convert_osm_to_parquet(input_file, output_file, progress_callback=progress_callback)

    print(f"Converted {input_file} to {output_file}")


@cli.command()
@click.argument("input_file", type=click.Path(exists=True))
@click_log.simple_verbosity_option()
def load(input_file: str):
    from osmpandas.package import OSMDataPackage

    data = OSMDataPackage.load(input_file)
    print(data)


@cli.command()
@click.argument("input_file", type=click.Path())
@click.argument("output_file", type=click.Path(), default=None)
@click.option("--types", type=str, default="nwr")
@click_log.simple_verbosity_option()
def to_excel(input_file: str, output_file: str | None, types: str):
    from osmpandas.package import OSMDataPackage
    from osmpandas.package import to_excel as package_to_excel

    if output_file is None:
        output_file = input_file.with_suffix(".xlsx")

    export_nodes = "n" in types
    export_ways = "w" in types
    export_relations = "r" in types

    data = OSMDataPackage.load(input_file)
    package_to_excel(
        output_file=output_file,
        data=data,
        export_nodes=export_nodes,
        export_ways=export_ways,
        export_relations=export_relations,
    )


@cli.command()
@click_log.simple_verbosity_option()
def check():
    from osmpandas.osmium_wrapper import check_osmium

    if check_osmium():
        print("\n✅ osmium is installed")
    else:
        print("\n❌ osmium is not installed")


if __name__ == "__main__":
    cli()
