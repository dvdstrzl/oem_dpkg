import logging
import os
from pathlib import Path
from frictionless import Package
import pandas as pd
import geopandas as gpd
import getpass
import json
import requests as req
from oep_client import OepClient
from oem2orm import oep_oedialect_oem2orm as oem2orm
from tqdm import tqdm


# - [ ] ADD OPTION: Upload ALL 
# - [ ] Errror handling... z.b. wenn Upload fehlschlägt --> info, und OPTION: Abbruch? (y/n)
# - [ ] Funktionen unbedingt besser gestalten (v.a. REDUNDANZEN verringern!)
# - [ ] Api-Connection headers auslagern (z.b. config)

class OEPDataHandler:
    def __init__(self, datapackage_json:str, api_token:str, oep_username:str, oep_schema="model_draft", dataset_selection:list = None):
        self.datapackage_json = datapackage_json
        self.dataset_selection = dataset_selection if dataset_selection is not None else []
        self.oep_schema = oep_schema
        self.datapackage = Package(self.datapackage_json)
        # self.oem_paths = [f"{dataset_name}/metadata.json" for dataset_name in self.dataset_selection]        
        if "OEP_TOKEN" not in os.environ:
            # os.environ["OEP_TOKEN"] = getpass.getpass("Enter API-Token:")
            os.environ["OEP_TOKEN"] = api_token
        if "OEP_USER" not in os.environ:
            # os.environ["OEP_TOKEN"] = getpass.getpass("Enter API-Token:")
            os.environ["OEP_USER"] = oep_username
        self.resources = []
        self.extract_dataset_resources(self.datapackage, self.dataset_selection)
        
    def extract_dataset_resources(self, datapackage, datasets):
            if datasets:
                found_resources = False
                for dataset_name in datasets:
                    for resource in datapackage.resources:
                        if resource.name.startswith(dataset_name) and not resource.name.endswith(".metadata"):
                            self.resources.append(resource)
                            found_resources = True

                if not found_resources:
                    raise ValueError(
                        f"No datasets found in '{', '.join(datasets)}' within the datapackage."
                    )
            else:
                # Add all resources if no dataset selection is provided, except those ending with '.metadata'
                for resource in datapackage.resources:
                    if not resource.name.endswith(".metadata"):
                        self.resources.append(resource)
            for resource in self.resources:
                print(resource.name)

    def setup_logger(self):
        oem2orm.setup_logger()

    def setup_db_connection(self):
        self.db = oem2orm.setup_db_connection()
    
    #
    # ALTERNATIVE UPLOAD (inkl. PROGRESSBAR-Funktion) --------------------------------------------------- TEST ERFOLGREICH!
    def upload_csv_data(self, table_name, data, auth_headers):
        res = req.post(f"https://openenergy-platform.org/api/v0/schema/model_draft/tables/{table_name}/rows/new", json={"query": data}, headers=auth_headers)
        if not res.ok:
            raise Exception(res.text)

    def upload_json_data(self, table_name, data, auth_headers):
        res = req.post(f"https://openenergy-platform.org/api/v0/schema/model_draft/tables/{table_name}/rows/new", json={"query": data}, headers=auth_headers)
        if not res.ok:
            raise Exception(res.text)

    def upload_gpkg_data(self, table_name, resource_abs_path, auth_headers):
        data_to_insert = self.gpkg_to_json(resource_abs_path)
        res = req.post(f"https://openenergy-platform.org/api/v0/schema/model_draft/tables/{table_name}/rows/new", json={"query": data_to_insert}, headers=auth_headers)
        if not res.ok:
            raise Exception(res.text)

    def upload_data(self):
        auth_headers = {"Authorization": f"Token {os.environ.get('OEP_TOKEN')}"}
            
        # self.create_sql_tables_from_oem(self.datapackage_json, self.oem_paths)
        progress_bar = tqdm(total=len(self.resources))
        for resource in self.resources:
            progress_bar.set_description(f"Uploading {resource.name}")
        
            table_name = resource.name.split(".")[-1]
            resource_abs_path = Path(self.datapackage.basepath) / Path(resource.path)

            if resource.format == "csv":
                df = pd.read_csv(resource_abs_path, encoding="utf8", sep=",", dtype={"RS": "str"})
                df.columns = map(str.lower, df.columns)
                data_to_insert = json.loads(df.to_json(orient="records"))
                self.upload_csv_data(table_name, data_to_insert, auth_headers)

            elif resource.format == "json":
                with open(resource_abs_path) as json_data:
                    data_to_insert = json.load(json_data)
                self.upload_json_data(table_name, data_to_insert, auth_headers)

            elif resource.format == "gpkg":
                self.upload_gpkg_data(table_name, resource_abs_path, auth_headers)

            # Aktualisiere die OEP-Metadaten für jede Ressource
            progress_bar.update(1)
            self.update_oep_metadata(resource.custom["oem"], table_name)
        progress_bar.close()

    # ----------------------------------------------------------------------
    
    def gpkg_to_json(self, gpkg_path):
        # Lade die GeoPackage-Daten
        gdf = gpd.read_file(gpkg_path)
        
        # Bereite die Daten für das JSON-Format vor
        records = []
        for _, row in gdf.iterrows():
            # Erstelle ein Dictionary für jede Zeile
            record = row.to_dict()
            
            # Transformiere Geometriedaten in WKT, wenn vorhanden
            if 'geometry' in record and record['geometry'] is not None:
                record['geometry'] = record['geometry'].wkt
            
            records.append(record)
        
        # Speichere die Daten im JSON-Format
        return records
                
    def create_oep_tables(self, datapackage_path, resources):
        # NUR FÜR DEV-VERSION (TESTING) --------------------------------------------------------------!
        # ...Bestehende Tables ENTFERNEN:
        for resource in self.resources:
            table_name = resource.name.split(".")[-1]
            table_api_url = f"https://openenergy-platform.org/api/v0/schema/model_draft/tables/{table_name}/"
            req.delete(table_api_url, headers={"Authorization": f"Token {os.environ.get('OEP_TOKEN')}"})
            print(f"Deleted existing table @OEP for {resource.name}")
        # -----------------------------------------------------------------------------
        base_path = Path(datapackage_path).parent  # Erhalte den Pfad bis zum Datapackage-Ordner
        oem_paths = [] 
        for resource in self.datapackage.resources:
            if resource.name.endswith(".metadata"):
                oem_paths.append(resource.path)
        for oem_path in oem_paths:
            full_oem_path = base_path / oem_path  # Erstelle den vollen Pfad zum OEM-Ordner
            tables_orm = self.extract_tables_from_oem(self.db, full_oem_path)
            oem2orm.create_tables(self.db, tables_orm)

    def extract_tables_from_oem(self, db, oem_file_path):
        tables = [] 
        if oem_file_path.exists():
            try:
                md_tables = oem2orm.create_tables_from_metadata_file(db, str(oem_file_path))
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

#
#
# Beispiel für die Verwendung (TEST mit datapackage)
oep_oem_handler = OEPDataHandler(
    datapackage_json="output/LATEST/datapackage/datapackage.json",
    # dataset_selection=["rpg_abw_regional_plan", "renewables_ninja_feedin"],
    api_token="0c9889067a4eafa692e433de3ea67acca51510ec",
    oep_username="davidst"
)
oep_oem_handler.run_all()
