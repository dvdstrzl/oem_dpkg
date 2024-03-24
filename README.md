# oem_dpkg

## OEM Data Package Creation & OEP Upload Handler

This project provides tools for packaging and uploading datasets along with their metadata, specifically tailored for compatibility with the Frictionless Data specifications, Open Energy Metadata (OEM) standards and the Open Energy Platform (OEP).

## Features

- **"OEM Data Package":** Gathers and packages datasets and metadata into a customized Frictionless Data Package, ready for sharing or uploading to OEP. The package is fully compatible with the Frictionless Data specification and framework and integrates compatibility with the specifications of OEM/OEP.
- **Geodata Support**: Includes custom handling for GeoPackage files, extracting and validating CRS (Coordinate Reference System), geometry types, and bounding boxes.
- **Data Upload:** Manages data upload to OEP-DB, automatically creating the necessary tables and uploading the data in batches of rows (batchsize can be adjusted).
- **Update metadata on OEP:** Supports updating metadata of specific tables on OEP based on the contents of the respective OEM files in the provided data package.
- **OEM Validation Reports**: Generates detailed reports on metadata validation, identifying any discrepancies or non-compliance issues with OEM standards.
- **CLI Support:** Offers a command-line interface for easy execution of the data packaging and uploading process.

### Contents

- `oem_datapackage.py`: Defines the `OemDataPackage` class, which packages datasets and metadata into custom "OEM Data Package"
- `oep_datahandler.py`: Implements the `OepUploadHandler` class for uploading datasets of an OEM Data Package to the OEP, including dataset and metadata.
- `cli.py`: Provides a Command Line Interface (CLI) to facilitate the use of `OemDataPackage` and `OepUploadHandler` functionalities.
- `utils.py`: Contains utility functions that support data processing tasks across the project.
- `requirements.txt`: Lists all the necessary Python packages required to run this project.
- `setup.py`: Contains setup configurations for packaging this project.
  
### Installation

Ensure you have Python 3.8 or newer installed. Clone or download the project repository, navigate to the project directory, and install the required dependencies:

```bash
pip install -r requirements.txt
```

## `OEM Data Package`

The `OEMDataPackage` class is designed to streamline the creation, validation, and packaging of datasets along with their respective OEM, adhering to the Frictionless Data Package standard and incorporating standards of OEM and OEP. It should facilitate the organization of datasets for easy sharing, publication, and further processing, specifically enabling improved integration with the OEP.

### Class Features

- **Automated Packaging**: Packages datasets and metadata for OEP and OEM compliance.
- **Metadata Validation**: Ensures metadata meets OEM standards for publication.
- **Customization**: Enables detailed package naming, description, and versioning.
- **Geospatial Features**: Validates geospatial data, including CRS and geometry.
- **Validation Reports**: Provides reports on metadata compliance with OEM.

### Usage

1. **Initialize**: Specify the input directory containing datasets and metadata, the output directory for the data package, package name, description, version, and whether to enable OEM integration.

```python
from oem_datapackage import OemDataPackage

package = OemDataPackage(
    input_path="/path/to/datasets",
    output_path="/path/to/output",
    name="Example Data Package",
    description="A comprehensive data package for energy research.",
    version="1.0",
    oem=True
)
```

2. **Create the Data Package**: Call the `create()` method to automatically package the datasets, perform metadata validation, and prepare the data package.

```python
package.create()
```

This process copies the datasets and metadata to the specified output directory, validates the metadata against OEM standards, and generates a `datapackage.json` file that describes the entire data package.

### Considerations

- Ensure the input directory is well-organized, with each dataset and its corresponding metadata placed in separate subdirectories. (see example structure)
- The metadata files should be named `metadata.json` and formatted according to the OEM standards for seamless validation.
- The naming convention for the data package should adhere to Frictionless Data Package specifications. The class automatically adjusts names to fit these specifications, if necessary.


### `OepUploadHandler`

The `OepUploadHandler` class is designed to improve the workflow of uploading data to the Open Energy Platform (OEP). It facilitates the preparation and uploading of datasets and their metadata, ensuring compliance with the Open Energy Metadata (OEM) standards. This guide will explain how to effectively utilize this class, highlighting important considerations to ensure successful data uploads.

#### Class Features

- **Batch Uploads**: Enables efficient dataset uploads to the Open Energy Platform (OEP) in batches.
- **Selective Processing**: Offers flexibility in processing specific datasets or all datasets within a package.
- **Metadata Updates**: Automatically updates metadata on OEP to keep dataset information current.
- **Table Management**: Gives options to newly create necessary tables or manage existing tables on OEP, including overwriting capabilities with user confirmation.

#### Prerequisites

Before using `OepUploadHandler`, ensure you have:

- A valid API token for the Open Energy Platform.
- Properly packaged the datasets and metadata you intend to upload, using the `OEMDataPackage` functions.

#### Initialization

To begin, instantiate the `OepUploadHandler` class with the path to your data package, your OEP API token, and other relevant information:
EXAMPLE:

```python
from oep_datahandler import OepUploadHandler

upload_handler = OepUploadHandler(
    datapackage_path="path/to/your/datapackage.json",
    api_token="your_oep_api_token",
    oep_username="your_oep_username",
    oep_schema="model_draft",  # Optional, defaults to "model_draft"
    dataset_selection=["dataset1", "dataset2"]  # Optional, specify datasets to upload
)
```

#### Extracting Dataset Resources

The `extract_dataset_resources` method filters the datasets you wish to upload based on your `dataset_selection`. If no selection is provided, all datasets within the data package are processed:

```python
upload_handler.extract_dataset_resources()
```

#### Setting up Database Connection

Establish a connection to the OEP Database API using `setup_db_connection`:

```python
upload_handler.setup_db_connection()
```

This step is crucial for enabling dataset uploads and table creation on the OEP.

#### Uploading Datasets

Use the `upload_datasets` method to upload the datasets to the OEP. This method handles data preparation, batch uploading, and metadata updating:

```python
upload_handler.upload_datasets()
```

During the upload process, a progress bar will display the upload status for each dataset.

#### Updating Metadata

If you need to update the metadata for a dataset already on the OEP, use the `update_oep_metadata` method. Provide the path to the OEM file and the table name:

```python
upload_handler.update_oep_metadata(
    oem_path="path/to/your/oem_file.json",
    table_name="your_table_name"
)
```

#### Run All

With "run_all()" it is possible to efficiently execute a complete upload process (initializing the handler, extracting resources, setting up a database connection, creating necessary tables, uploading metadata and dataset-data).

```python
oep_uploadhandler = OepUploadHandler(
    datapackage_path="output/LATEST/datapackage",
    api_token="your_api_token",
    oep_username="your_oep_username"
)
oep_uploadhandler.run_all()
```

#### Considerations

- **Data and Metadata Compatibility:** Ensure your datasets and metadata files comply with the OEM standards and the specific requirements of the OEP database schema you're targeting.
- **Batch Size:** The default batch size is set to 1000 rows. Depending on your dataset size and network conditions, you may adjust this value in the `upload_data_to_table` method call within `upload_datasets`. 
- **Error Handling:** The class includes basic error handling for database connections, API requests, and batch uploads. Monitor the console output for error messages to troubleshoot issues.

## CLI

The provided CLI tool (`cli.py`) offers an accessible way to use the functionalities of this project from the command line, streamlining the process of data package creation and uploading to OEP.

#### Creating a Data Package

To create a data package from your datasets and metadata, run:

```bash
oem_dpkg create-package <input_path> <output_path> <name> <description> <version> [--oem]
```

#### Uploading to OEP

To upload your prepared data package to the Open Energy Platform, use:

```bash
oem_dpkg oep-upload <datapackage_path> [dataset_name] --schema <schema_name>
```

The `dataset_selection` argument is optional; if not provided, all datasets within the data package will be processed.

#### Example calls:

```bash
oem_dpkg create-package "input/path" "output/path" "name" "description" "version" --oem

oem_dpkg oep-upload "output/path/datapackage/datapackage.json" --dataset_selection "dataset1" --dataset_selection "dataset2" --schema "model_draft"
``` 
