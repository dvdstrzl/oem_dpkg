import re
from typing import List, Optional, Dict, Any
import logging
import os
from pathlib import Path
from frictionless import Package, Resource
import getpass
import sqlalchemy as sa
import json
import requests as req
from oep_client import OepClient
from oem2orm import oep_oedialect_oem2orm as oem2orm
from tqdm import tqdm
from utils import prepare_csv_data, prepare_gpkg_data, prepare_json_data
import oedialect

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)


class OepUploadHandler:
    """
    Handles the preparation and uploading of datasets and metadata of a provided data package (OemDataPackage) to the Open Energy Platform (OEP) via OEP Database API.

    This class manages the process from reading the Frictionless data package (OemDataPackage),
    selecting specific datasets for creating the tables if necessary, and performing the dataset upload operations (in batches).
    It also supports updating specific metadata on OEP based on the contents of a provided OEM file.

    Attributes:
        datapackage_json (str): Path to the frictionless data package JSON file.
        dataset_selection (List[str]): Optional list of dataset names to be processed.
        oep_schema (str): Schema name on the OEP under which the tables will be created.
        datapackage (Package): Frictionless data package object loaded from datapackage_json (OemDataPackage).
        resources (List[Resource]): List of resources (datasets) to be uploaded.
        oem_paths (List[str]): Paths to OEM metadata files related to the datasets.
        resources_ignore_list (List[str]): List of dataset names to be ignored during the upload.
    """

    def __init__(
        self,
        datapackage_path: str,
        api_token: Optional[str] = None,
        oep_username: Optional[str] = None,
        oep_schema: str = "model_draft",
        dataset_selection: Optional[List[str]] = None,
    ) -> None:
        self.datapackage_json: str = str(
            Path(datapackage_path) / "datapackage.json"
        )
        self.dataset_selection: List[str] = (
            dataset_selection if dataset_selection is not None else []
        )
        self.oep_schema: str = oep_schema
        self.datapackage: Package = Package(self.datapackage_json)
        if "OEP_TOKEN" not in os.environ:
            if api_token:
                os.environ["OEP_TOKEN"] = api_token
            else:
                os.environ["OEP_TOKEN"] = getpass.getpass("Enter API-Token:")
        if "OEP_USER" not in os.environ:
            if oep_username:
                os.environ["OEP_USER"] = oep_username
            else:
                os.environ["OEP_USER"] = getpass.getpass("Enter OEP-username:")
        self.resources: List[Resource] = []
        self.oem_paths: List[str] = []
        self.resources_ignore_list: List[str] = []

    def extract_dataset_resources(
        self, datapackage: Package, datasets: Optional[List[str]]
    ) -> None:
        """
        Extracts resources from the data package based on the provided dataset selection.

        If a dataset selection is provided, only resources matching those dataset names are processed.
        If NO dataset selection is provided, ALL resources in the data package are processed.

        Additionally, checks for the existence of corresponding OEM files for each selected dataset.

        Parameters:
            datapackage (Package): The frictionless data package object to extract resources from.
            datasets (Optional[List[str]]): A list of dataset names to filter the resources by.

        Raises:
            ValueError: If no datasets are found matching the selection or if OEM files are missing for selected datasets.
        """
        if datasets:
            found_resources = False
            found_oem = False
            for dataset_name in datasets:
                for resource in datapackage.resources:
                    if resource.name == (f"{dataset_name}.oem"):
                        self.oem_paths.append(resource.path)
                        found_oem = True
                    elif resource.name.startswith(dataset_name):
                        self.resources.append(resource)
                        found_resources = True

            if not found_resources:
                raise ValueError(
                    f"No datasets found for '{', '.join(datasets)}' within the data package."
                )
            if not found_oem:
                raise ValueError(
                    f"No OEM found for '{', '.join(datasets)}' within the data package."
                )
        else:
            # ALL RESOURCES of data package will be handled if dataset selection is not provided
            for resource in datapackage.resources:
                if not resource.name.endswith(".oem"):
                    self.resources.append(resource)
                else:
                    self.oem_paths.append(resource.path)

    def setup_db_connection(self):
        """
        Establishes a connection to the OEP Database API.

        This method attempts to set up a connection to the OEP database using the oem2orm library.
        It logs the outcome of the connection attempt, providing feedback on success or failure.

        Raises:
            Exception: If the connection attempt fails, an exception is logged and then raised to
            indicate the failure to establish a connection. This could be due to a variety of reasons,
            such as network issues, incorrect credentials, or configuration errors.
        """
        try:
            self.db = oem2orm.setup_db_connection()
            logging.info("Connection to OEP Database API established.")
        except Exception as e:
            logging.error(
                f"An error occured while trying to create connection to OEP Database API: {e}"
            )
            raise

    def upload_data_to_table(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        auth_headers: Dict[str, str],
        batch_size: int = 1000,
    ) -> None:
        """
        Uploads data to a specified table on the OEP in batches (to reduce possible timeout issues).

        The function splits the data into batches and makes POST requests to the OEP API
        to insert the data into the specified table.

        Parameters:
            table_name (str): Name of the table to upload data to.
            data (List[Dict[str, Any]]): The data to be uploaded, formatted as a list of dictionaries.
            auth_headers (Dict[str, str]): Authorization headers containing the OEP API token.
            batch_size (int): The number of rows to include in each batch upload.

        Raises:
            requests.exceptions.RequestException: If an error occurs during the batch upload request.
        """
        for i in range(0, len(data), batch_size):
            batch = data[i : i + batch_size]
            try:
                res = req.post(
                    f"https://openenergy-platform.org/api/v0/schema/model_draft/tables/{table_name}/rows/new",
                    json={"query": batch},
                    headers=auth_headers,
                )
                res.raise_for_status()
            except req.exceptions.RequestException as e:
                logging.error(f"An error occurred during batch upload: {e}")
                raise

    def upload_datasets(self):
        """
        Uploads datasets to OEP in batches, supporting CSV, JSON, and GeoPackage formats.

        For each resource, this method checks if it's in the 'ignore list', prepares the data,
        and uploads it in batches to avoid request size limits.
        It displays a progress bar for each resource being uploaded.
        After uploading, it updates the OEP table's metadata based on the resource's OEM file.

        Batch failures are logged, and the process continues with the next batch or resource.

        Raises:
            Exception: On errors during batch upload, such as connection issues or formatting problems.
        """
        auth_headers = {"Authorization": f"Token {os.environ.get('OEP_TOKEN')}"}
        batch_size = 2000  # number of rows per batch

        for resource in self.resources:
            table_name = resource.name.split(".")[-1]
            resource_abs_path = Path(self.datapackage.basepath) / Path(
                resource.path
            )

            if table_name not in self.resources_ignore_list:
                self.update_oep_metadata(
                    resource.custom["oem_path"], table_name
                )
                if resource.format == "csv":
                    data_to_insert = prepare_csv_data(resource_abs_path)
                elif resource.format == "json":
                    data_to_insert = prepare_json_data(resource_abs_path)
                elif resource.format == "gpkg":
                    data_to_insert = prepare_gpkg_data(resource_abs_path)
                else:
                    logging.warning(
                        f"'{resource.name}': Format not supported ('{resource.format}')."
                    )
                    continue

                total_size = len(data_to_insert)
                with tqdm(
                    total=total_size,
                    desc=f"Uploading '{resource.name}'",
                    unit="rows",
                    leave=True,
                ) as pbar:
                    for i in range(0, total_size, batch_size):
                        batch = data_to_insert[i : i + batch_size]
                        try:
                            self.upload_data_to_table(
                                table_name, batch, auth_headers, batch_size
                            )
                            pbar.update(len(batch))
                        except Exception as e:
                            logging.error(
                                f"Failed to upload batch for {resource.name}. Error: {e}"
                            )
        logging.info("Finished uploading.")

    def prepare_oep_tables(self, datapackage_path):
        """
        Prepares the OEP database tables based on metadata from OEM files.

        This method checks if tables already exist on the OEP and offers the user the option to overwrite them.
        It deletes existing tables if chosen to overwrite and creates new tables from the OEM metadata.

        Parameters:
            datapackage_path (str): Path to the directory containing the datapackage and OEM files.
        """
        base_path = Path(datapackage_path).parent
        for oem_resource in self.oem_paths:
            full_oem_path = base_path / oem_resource
            tables_orm = self.generate_tables_from_metadata(
                self.db, full_oem_path
            )
            existing_tables = []
            for table in tables_orm:
                if self.db.engine.dialect.has_table(
                    self.db.engine, table.name, schema=table.schema
                ):
                    while True:
                        table_warning = input(
                            f"Table '{table.name}' already exists on OEP. Do you want to REPLACE it? [y] or [n]\n>>> "
                        )
                        if re.fullmatch("[Nn]", table_warning):
                            existing_tables.append(table)
                            self.resources_ignore_list.append(table.name)
                            break
                        elif re.fullmatch("[Yy]", table_warning):
                            table_api_url = f"https://openenergy-platform.org/api/v0/schema/model_draft/tables/{table.name}/"
                            req.delete(
                                table_api_url,
                                headers={
                                    "Authorization": f"Token {os.environ.get('OEP_TOKEN')}"
                                },
                            )
                            logging.info(
                                f"Deleted existing table on OEP: '{table.name}'."
                            )
                            break
                        else:
                            logging.warning(
                                "Invalid input... Please enter 'y' or 'n'."
                            )

            for table in existing_tables:
                tables_orm.remove(table)
            for table in tables_orm:
                try:
                    table.create(checkfirst=True)
                    logging.info(f"Created table on OEP: '{table.name}'.")
                except oedialect.engine.ConnectionException as ce:
                    error_msg = f"Error when creating table '{table.name}'."
                    logging.error(error_msg)
                    raise oem2orm.DatabaseError(error_msg) from ce

    def generate_tables_from_metadata(
        self, db, oem_file_path
    ) -> List[sa.Table]:
        """
        Generates SQLAlchemy database table objects based on the structure defined in provided OEM.
        If the OEM is found and can be processed, the tables are created in the database.

        Parameters:
            db (DB): Database connection object.
            oem_file_path (Path): Path to the OEM metadata file.

        Returns:
            List[sa.Table]: A list of SQLAlchemy table objects ready for creation in the database.

        Raises:
            Exception: If tables cannot be created from the OEM metadata.
        """
        tables = []
        if oem_file_path.exists():
            try:
                md_tables = oem2orm.create_tables_from_metadata_file(
                    db, str(oem_file_path)
                )
                tables.extend(md_tables)
            except Exception as e:
                logging.error(
                    f"Could not create tables from OEM: '{oem_file_path}'\nError: {e}"
                )
                raise
        else:
            logging.warning(f"Metadata file not found in: '{oem_file_path}'.")
        return oem2orm.order_tables_by_foreign_keys(tables)

    def update_oep_metadata(self, oem_path: str, table_name: str) -> None:
        """
        Updates the metadata on the OEP platform for a specific table.
        The oem is also adjusted so that only the respective resource is listed under "resources".

        Parameters:
        - oem_path (str): The relative path to the metadata JSON file.
        - table_name (str): The name of the table for which to update the metadata, which also matches the resource name to keep.

        This function modifies the metadata in-memory to only include the resource matching the table_name before uploading.
        """
        oem_path = Path(self.datapackage.basepath) / Path(oem_path)
        with open(oem_path) as json_file:
            metadata = json.load(json_file)

        filtered_resources = [
            resource
            for resource in metadata.get("resources", [])
            if resource.get("name") == table_name
        ]

        if not filtered_resources:
            raise ValueError(
                f"'No matching metadata found for '{table_name}' in the provided oem."
            )

        metadata["resources"] = filtered_resources
        try:
            cli = OepClient(
                token=os.environ["OEP_TOKEN"], default_schema=self.oep_schema
            )
            cli.set_metadata(table_name, metadata)
            logging.info(f"Updated metadata for table '{table_name}' on OEP.")
        except Exception as e:
            logging.error(
                f"Issue while trying to update metadata for '{table_name}' on OEP: {e}"
            )
            raise

    def run_all(self):
        self.setup_db_connection()
        self.extract_dataset_resources(self.datapackage, self.dataset_selection)
        self.prepare_oep_tables(self.datapackage_json)
        self.upload_datasets()


# ------------------------------------------------------------------------------
# # Example usage
# oep_uploadhandler = OepUploadHandler(
#     datapackage_path="output/LATEST/datapackage",
#     # dataset_selection=["rpg_abw_regional_plan"],
#     # dataset_selection=["renewables_ninja_feedin"],
#     api_token="0c9889067a4eafa692e433de3ea67acca51510ec",
#     oep_username="davidst",
# )
# oep_uploadhandler.run_all()
# # oep_uploadhandler.setup_db_connection()
# # oep_uploadhandler.update_oep_metadata(
# #     "rpg_abw_regional_plan/metadata.json", "stp_2018_vreg"
# # )
