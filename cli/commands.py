import click
from oep_oem_handler import OEPDataHandler  
from datapackage_creater import CustomPackage  


"""
This script provides a CLI (Command Line Interface) for managing and uploading datasets using the OEPDataHandler and creating Datapackages with CustomPackage.

- The 'create-package' command creates a datapackage from the specified input files, requiring the path to the input folder, output folder, name, description, and version of the datapackage. The '--oem' flag can optionally be added to include OEM metadata.
- The 'oep-upload' command uploads data for a given dataset to the OEP database, requiring the path to the 'datapackage.json', the dataset name, and optionally the schema to be used.

Example calls:
mydp-cli create-package "input/path" "output/path" "name" "description" "version" --oem
mydp-cli oep-upload "path/to/datapackage.json" "dataset_name" --schema "model_draft"
"""

@click.group()
def cli():
    """OEP Data Handler und Custom Package CLI."""
    pass

@cli.command()
@click.argument('datapackage_path', type=click.Path(exists=True))
@click.argument('dataset_name')
@click.option('--schema', default='model_draft', help='Schema to use in the OEP database.')
def oep_upload(datapackage_path, dataset_name, schema):
    """Uploads data to the OEP database for a given dataset."""
    handler = OEPDataHandler(datapackage_path=datapackage_path, dataset_name=dataset_name, schema=schema)
    handler.run_all()
    click.echo(f"Uploaded dataset '{dataset_name}' to OEP database successfully.")

@cli.command()
@click.argument('input_path', type=click.Path(exists=True))
@click.argument('output_path')
@click.argument('name')
@click.argument('description')
@click.argument('version')
@click.option('--oem', is_flag=True, help='Include OEM metadata in the package.')
def create_package(input_path, output_path, name, description, version, oem):
    """Creates a datapackage from input files."""
    package = CustomPackage(input_path, output_path, name, description, version, oem=oem)
    package.create()
    click.echo(f"Datapackage '{name}' created successfully at {output_path}.")

if __name__ == '__main__':
    cli()
