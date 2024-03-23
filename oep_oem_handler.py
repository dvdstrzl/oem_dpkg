import re
from typing import List, Optional, Dict, Any, Union
import logging
import os
from pathlib import Path
from frictionless import Package, Resource
import pandas as pd
import geopandas as gpd
import getpass
import sqlalchemy as sa
import json
import requests as req
from oep_client import OepClient
from oem2orm import oep_oedialect_oem2orm as oem2orm
from tqdm import tqdm
from utils import prepare_csv_data, prepare_gpkg_data, prepare_json_data

# - [ ] CONFIG auslagern (config.yaml): Api-Connection headers
# - [ ] Umfangreiches Errror handling... z.b. wenn Upload fehlschlägt --> info, und OPTION? Abbruch (y/n)
# - [ ] Funktionen besser gestalten (z.b. Struktur/Logik, REDUNDANZEN verringern!)


class OEPDataHandler:
    def __init__(
        self,
        datapackage_json: str,
        api_token: str,
        oep_username: str,
        oep_schema: str = "model_draft",
        dataset_selection: Optional[List[str]] = None,
    ) -> None:
        self.datapackage_json: str = datapackage_json
        self.dataset_selection: List[str] = (
            dataset_selection if dataset_selection is not None else []
        )
        self.oep_schema: str = oep_schema
        self.datapackage: Package = Package(self.datapackage_json)
        if "OEP_TOKEN" not in os.environ:
            # os.environ["OEP_TOKEN"] = getpass.getpass("Enter API-Token:")
            os.environ["OEP_TOKEN"] = api_token
        if "OEP_USER" not in os.environ:
            # os.environ["OEP_TOKEN"] = getpass.getpass("Enter API-Token:")
            os.environ["OEP_USER"] = oep_username
        self.resources: List[Resource] = []
        self.oem_paths: List[str] = []
        self.resources_ignore_list: List[str] = []

    def extract_dataset_resources(
        self, datapackage: Package, datasets: Optional[List[str]]
    ) -> None:
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
            # Add all resources if no dataset selection is provided
            for resource in datapackage.resources:
                if not resource.name.endswith(".oem"):
                    self.resources.append(resource)
                else:
                    self.oem_paths.append(resource.path)

    def setup_logger(self):
        oem2orm.setup_logger()

    def setup_db_connection(self):
        self.db = oem2orm.setup_db_connection()

    #################### TEST: BATCHING UPLOAD!
    def upload_data_to_table(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        auth_headers: Dict[str, str],
        batch_size: int = 1000,
    ) -> None:
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
        auth_headers = {"Authorization": f"Token {os.environ.get('OEP_TOKEN')}"}
        batch_size = 3000  # Die Größe jedes Batches

        for resource in self.resources:
            table_name = resource.name.split(".")[-1]
            resource_abs_path = Path(self.datapackage.basepath) / Path(
                resource.path
            )

            # Entscheide das Format und bereite die Daten vor
            if table_name not in self.resources_ignore_list:
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

                # Erstelle eine ProgressBar für jedes Resource
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
                            pbar.update(
                                len(batch)
                            )  # Aktualisiere ProgressBar basierend auf der tatsächlichen Batch-Größe
                        except Exception as e:
                            logging.error(
                                f"Failed to upload batch for {resource.name}. Error: {e}"
                            )
                            # Hier könnten Sie entscheiden, ob Sie den Vorgang abbrechen oder versuchen, den Batch erneut hochzuladen.
                self.update_oep_metadata(
                    resource.custom["oem_path"], table_name
                )

    # def upload_data_to_table(
    #     self,
    #     table_name: str,
    #     data: Union[List[Dict[str, Any]], Dict[str, Any]],
    #     auth_headers: Dict[str, str],
    # ) -> None:
    #     try:
    #         res = req.post(
    #             f"https://openenergy-platform.org/api/v0/schema/model_draft/tables/{table_name}/rows/new",
    #             json={"query": data},
    #             headers=auth_headers,
    #         )
    #         res.raise_for_status()
    #     except req.exceptions.JSONDecodeError as json_err:
    #         logging.error(f"JSONDecode error: {json_err}")
    #         raise
    #     except req.exceptions.HTTPError as http_err:
    #         logging.error(f"HTTP error: {http_err}")
    #         raise
    #     except req.exceptions.Timeout as timeout_err:
    #         logging.error(f"Timeout occurred: {timeout_err}")
    #         raise
    #     except req.exceptions.ConnectionError as con_err:
    #         logging.error(f"Connection error: {con_err}")
    #         raise
    #     except req.exceptions.RequestException as e:
    #         logging.error(f"An error occurred: {e}")
    #         raise

    # def upload_datasets(self):
    #     auth_headers = {"Authorization": f"Token {os.environ.get('OEP_TOKEN')}"}
    #     progress_bar = tqdm(total=len(self.resources))
    #     try:
    #         for resource in self.resources:
    #             progress_bar.set_description(
    #                 f"Now uploading '{resource.name}' | Total Progress"
    #             )
    #             table_name = resource.name.split(".")[-1]
    #             resource_abs_path = Path(self.datapackage.basepath) / Path(
    #                 resource.path
    #             )
    #             if table_name in self.resources_ignore_list:
    #                 progress_bar.total = progress_bar.total - 1
    #             else:
    #                 if resource.format == "csv":
    #                     data_to_insert = prepare_csv_data(resource_abs_path)
    #                 elif resource.format == "json":
    #                     data_to_insert = prepare_json_data(resource_abs_path)
    #                 elif resource.format == "gpkg":
    #                     data_to_insert = prepare_gpkg_data(resource_abs_path)
    #                 else:
    #                     logging.warning(
    #                         f"'{resource.name}': Format not supported ('{resource.format}')."
    #                     )
    #                     continue

    #                 self.upload_data_to_table(table_name, data_to_insert, auth_headers)
    #                 self.update_oep_metadata(resource.custom["oem_path"], table_name)
    #                 progress_bar.update(1)

    #     except Exception as e:
    #         logging.error("Failed to upload data! ", exc_info=e)
    #     finally:
    #         progress_bar.set_description("Upload completed")
    #         progress_bar.close()

    def create_oep_tables(self, datapackage_path):
        # NUR FÜR DEV-VERSION (TESTING) --------------------------------------------------------------!
        # ...Bestehende Tables ENTFERNEN:
        # for resource in resources:
        #     table_name = resource.name.split(".")[-1]
        #     table_api_url = f"https://openenergy-platform.org/api/v0/schema/model_draft/tables/{table_name}/"
        #     req.delete(
        #         table_api_url,
        #         headers={"Authorization": f"Token {os.environ.get('OEP_TOKEN')}"},
        #     )
        #     logging.info(f"Deleted existing table on OEP for {resource.name}")
        # # -----------------------------------------------------------------------------
        base_path = Path(
            datapackage_path
        ).parent  # Erhalte den Pfad bis zum Datapackage-Ordner
        for oem_resource in self.oem_paths:
            full_oem_path = (
                base_path / oem_resource
            )  # Erstelle den vollen Pfad zum OEM-Ordner
            tables_orm = self.extract_tables_from_oem(self.db, full_oem_path)
            existing_tables = []
            for table in tables_orm:
                if self.db.engine.dialect.has_table(
                    self.db.engine, table.name, schema=table.schema
                ):
                    table_warning = input(
                        f"Table '{table.name} - {self.oep_schema}' already exists on OEP. Do you want to OVERWRITE it? [Yes] or [No]\n>>> "
                    )
                    if re.fullmatch("[Nn]o", table_warning):
                        existing_tables.append(table)
                        self.resources_ignore_list.append(table.name)
            for table in existing_tables:
                tables_orm.remove(table)
            try:
                oem2orm.create_tables(self.db, tables_orm)
            except Exception as e:
                logging.error(f"Could not create tables on OEP.\nError: {e}")
                raise

    def extract_tables_from_oem(self, db, oem_file_path) -> List[sa.Table]:
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
            logging.warning(f"Metadata file not found in: {oem_file_path}")
        return oem2orm.order_tables_by_foreign_keys(tables)

    def update_oep_metadata(self, oem_path: str, table_name: str) -> None:
        """
        Updates the metadata on the OEP platform for a specific table.
        It also modifies the metadata to keep only the specified resource respectively.

        Parameters:
        - oem_path (str): The relative path to the metadata JSON file.
        - table_name (str): The name of the table for which to update the metadata, which also matches the resource name to keep.

        This function modifies the metadata in-memory to only include the resource matching the table_name before uploading.
        """
        # Construct the full path to the metadata file
        oem_path = Path(self.datapackage.basepath) / Path(oem_path)
        with open(oem_path) as json_file:
            metadata = json.load(json_file)

        # Filter the resources to keep only the one matching the table_name
        filtered_resources = [
            resource
            for resource in metadata.get("resources", [])
            if resource.get("name") == table_name
        ]

        if not filtered_resources:
            raise ValueError(
                f"No matching resource found for table '{table_name}' in the metadata."
            )

        # Replace the original resources list with the filtered one
        metadata["resources"] = filtered_resources

        # Update the metadata on the OEP platform
        cli = OepClient(
            token=os.environ["OEP_TOKEN"], default_schema=self.oep_schema
        )
        cli.set_metadata(table_name, metadata)

    def run_all(self):
        self.setup_logger()
        self.setup_db_connection()
        self.extract_dataset_resources(self.datapackage, self.dataset_selection)
        self.create_oep_tables(self.datapackage_json)
        self.upload_datasets()


# ------------------------------------------------------------------------------
# Beispiel für die Verwendung (TEST mit datapackage)
oep_oem_handler = OEPDataHandler(
    datapackage_json="output/LATEST/datapackage/datapackage.json",
    # dataset_selection=["rpg_abw_regional_plan"],
    # dataset_selection=["renewables_ninja_feedin"],
    api_token="0c9889067a4eafa692e433de3ea67acca51510ec",
    oep_username="davidst",
)
oep_oem_handler.run_all()
