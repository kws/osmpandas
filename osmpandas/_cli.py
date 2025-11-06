from pathlib import Path

import click


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
    from osmpandas.data import OSMDataPackage

    input_file = Path(input_file)
    data = OSMDataPackage.from_osm(input_file)
    if output_file is None:
        if input_file.name.endswith(".osm.pbf"):
            output_file = output_file.replace(".osm.pbf", ".osmpkg")
        else:
            output_file = input_file.with_suffix(".osmpkg")
    data.save(output_file)


@cli.command()
def check():
    from osmpandas.osmium_wrapper import check_osmium

    if check_osmium():
        print("\n✅ osmium is installed")
    else:
        print("\n❌ osmium is not installed")


if __name__ == "__main__":
    cli()
