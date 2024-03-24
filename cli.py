import click
from oep_datahandler import OEPDataHandler
from oem_datapackage import OemDataPackage


"""
This script provides a CLI (Command Line Interface) for managing and uploading datasets using the OEPDataHandler and creating Datapackages with CustomPackage.

- The 'create-package' command creates a datapackage from the specified input files, requiring the path to the input folder, output folder, name, description, and version of the datapackage. The '--oem' flag can optionally be added to include OEM metadata.
- The 'oep-upload' command uploads data for a given dataset to the OEP database, requiring the path to the 'datapackage.json', the dataset name, and optionally the schema to be used.

Example calls:
oemdp create-package "input/path" "output/path" "name" "description" "version" --oem
oemdp oep-upload "path/to/datapackage.json" "dataset_name" --schema "model_draft"
"""


@click.group()
def cli():
    """CLI for OEPDataHandler and OemDataPackage"""
    pass


@cli.command()
@click.argument("input_path", type=click.Path(exists=True))
@click.argument("output_path")
@click.argument("name")
@click.argument("description")
@click.argument("version")
@click.option(
    "--oem", is_flag=True, help="Include OEM metadata in the package."
)
def create_package(input_path, output_path, name, description, version, oem):
    """Creates a datapackage from input files."""
    package = OemDataPackage(
        input_path, output_path, name, description, version, oem=oem
    )
    package.create()


@cli.command()
@click.argument("datapackage_path", type=click.Path(exists=True))
@click.option(
    "--dataset_selection",
    default=None,
    help="list of dataset names to handle. If not provided, all datasets in the datapackage will be processed.",
)
@click.option(
    "--schema", default="model_draft", help="Schema to use in the OEP database."
)
def oep_upload(datapackage_path, dataset_selection, schema):
    """Uploads data to the OEP database. If dataset selection is given, only those are handled; otherwise, all datasets in the datapackage are processed."""
    handler = OEPDataHandler(
        datapackage_path=datapackage_path,
        oep_schema=schema,
        dataset_selection=[dataset_selection] if dataset_selection else None,
    )
    handler.run_all()


if __name__ == "__main__":
    cli()
