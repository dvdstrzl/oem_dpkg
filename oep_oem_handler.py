from typing import List, Optional, Dict, Any, Union
import logging
import os
from pathlib import Path
from frictionless import Package, Resource
import pandas as pd
import geopandas as gpd
import getpass
import json
import requests as req
from oep_client import OepClient
from oem2orm import oep_oedialect_oem2orm as oem2orm
from tqdm import tqdm
from utils import prepare_csv_data, prepare_gpkg_data, prepare_json_data


# - [ ] Errror handling... z.b. wenn Upload fehlschlägt --> info, und OPTION: Abbruch? (y/n)
# - [ ] Funktionen unbedingt besser gestalten (v.a. REDUNDANZEN verringern!)
# - [ ] Api-Connection headers auslagern (z.b. config)


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
        self.extract_dataset_resources(self.datapackage, self.dataset_selection)

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
            # Add all resources if no dataset selection is provided, except those ending with '.metadata'
            for resource in datapackage.resources:
                if not resource.name.endswith(".oem"):
                    self.resources.append(resource)
                else:
                    self.oem_resources.append(resource)
        for resource in self.resources:
            print(resource.name)

    def setup_logger(self):
        oem2orm.setup_logger()

    def setup_db_connection(self):
        self.db = oem2orm.setup_db_connection()

    def upload_data_to_table(
        self,
        table_name: str,
        data: Union[List[Dict[str, Any]], Dict[str, Any]],
        auth_headers: Dict[str, str],
    ) -> None:
        res = req.post(
            f"https://openenergy-platform.org/api/v0/schema/model_draft/tables/{table_name}/rows/new",
            json={"query": data},
            headers=auth_headers,
        )
        if not res.ok:
            raise Exception(res.raise_for_status)

    def upload_data(self):
        auth_headers = {"Authorization": f"Token {os.environ.get('OEP_TOKEN')}"}
        progress_bar = tqdm(total=len(self.resources))
        for resource in self.resources:
            progress_bar.set_description(f"Uploading {resource.name}")
            table_name = resource.name.split(".")[-1]
            resource_abs_path = Path(self.datapackage.basepath) / Path(resource.path)

            if resource.format == "csv":
                data_to_insert = prepare_csv_data(resource_abs_path)
            elif resource.format == "json":
                data_to_insert = prepare_json_data(resource_abs_path)
            elif resource.format == "gpkg":
                data_to_insert = prepare_gpkg_data(resource_abs_path)
            else:
                continue  # Falls das Format nicht unterstützt wird, überspringen

            self.upload_data_to_table(table_name, data_to_insert, auth_headers)
            progress_bar.update(1)
        progress_bar.close()

    def gpkg_to_json(self, gpkg_path: Path) -> List[Dict[str, Any]]:
        # Lade die GeoPackage-Daten
        gdf = gpd.read_file(gpkg_path)

        # Bereite die Daten für das JSON-Format vor
        records = []
        for _, row in gdf.iterrows():
            # Erstelle ein Dictionary für jede Zeile
            record = row.to_dict()

            # Transformiere Geometriedaten in WKT, wenn vorhanden
            if "geometry" in record and record["geometry"] is not None:
                record["geometry"] = record["geometry"].wkt

            records.append(record)

        # Speichere die Daten im JSON-Format
        return records

    def create_oep_tables(self, datapackage_path, resources):
        # NUR FÜR DEV-VERSION (TESTING) --------------------------------------------------------------!
        # ...Bestehende Tables ENTFERNEN:
        for resource in resources:
            table_name = resource.name.split(".")[-1]
            table_api_url = f"https://openenergy-platform.org/api/v0/schema/model_draft/tables/{table_name}/"
            req.delete(
                table_api_url,
                headers={"Authorization": f"Token {os.environ.get('OEP_TOKEN')}"},
            )
            print(f"Deleted existing table on OEP for {resource.name}")
        # -----------------------------------------------------------------------------
        base_path = Path(
            datapackage_path
        ).parent  # Erhalte den Pfad bis zum Datapackage-Ordner
        for oem_resource in self.oem_paths:
            full_oem_path = (
                base_path / oem_resource
            )  # Erstelle den vollen Pfad zum OEM-Ordner
            tables_orm = self.extract_tables_from_oem(self.db, full_oem_path)
            oem2orm.create_tables(self.db, tables_orm)

    def extract_tables_from_oem(self, db, oem_file_path):
        tables = []
        if oem_file_path.exists():
            try:
                md_tables = oem2orm.create_tables_from_metadata_file(
                    db, str(oem_file_path)
                )
                logging.info(f"Created tables from: {oem_file_path}")
                tables.extend(md_tables)
            except Exception as e:
                logging.error(f'Could not create tables from OEM: "{oem_file_path}"')
                raise e
        else:
            logging.warning(f"Metadata file not found in: {oem_file_path}")
        return oem2orm.order_tables_by_foreign_keys(tables)

    def update_oep_metadata(self, metadata_file, table_name):
        metadata_file = Path(self.datapackage.basepath) / Path(metadata_file)
        with open(metadata_file) as json_file:
            metadata = json.load(json_file)
        cli = OepClient(token=os.environ["OEP_TOKEN"], default_schema=self.oep_schema)
        cli.set_metadata(table_name, metadata)

    def run_all(self):
        self.setup_logger()
        self.setup_db_connection()
        self.create_oep_tables(self.datapackage_json, self.resources)
        self.upload_data()


# ------------------------------------------------------------------------------
# Beispiel für die Verwendung (TEST mit datapackage)
oep_oem_handler = OEPDataHandler(
    datapackage_json="output/LATEST/datapackage/datapackage.json",
    # dataset_selection=["rpg_abw_regional_plan", "renewables_ninja_feedin"],
    dataset_selection=["renewables_ninja_feedin"],
    api_token="0c9889067a4eafa692e433de3ea67acca51510ec",
    oep_username="davidst",
)
oep_oem_handler.run_all()
