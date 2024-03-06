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


# - [ ]   def upload_data(self): adjust for other filetypes!!


class OEPDataHandler:
    def __init__(self, datapackage_json:str, dataset_names:list, api_token:str, oep_username:str, schema="model_draft"):
        self.datapackage_json = datapackage_json
        self.dataset_names = dataset_names
        self.schema = schema
        self.datapackage = Package(self.datapackage_json)
        self.datapackage_basepath = self.datapackage.basepath
        self.oem_paths = [f"{dataset_name}/metadata.json" for dataset_name in self.dataset_names]        
        self.resources = []
        self.db = None
        if "OEP_TOKEN" not in os.environ:
            # os.environ["OEP_TOKEN"] = getpass.getpass("Enter API-Token:")
            os.environ["OEP_TOKEN"] = api_token
        if "OEP_USER" not in os.environ:
            # os.environ["OEP_TOKEN"] = getpass.getpass("Enter API-Token:")
            os.environ["OEP_USER"] = oep_username
        self.check_datapackage(self.datapackage, self.dataset_names)

    # def load_datapackage(self, datapackage, dataset_name):
    #     package = Package(datapackage_json)
    #     resource = package.get_resource(dataset_name)
    #     if resource:
    #         self.resource_dir = os.path.dirname(resource.path)
    #         self.resource_file_path = resource.path
    #         if '.' in resource.name:
    #             self.table_name = resource.name.split('.')[-1]
    #         else:
    #             self.table_name = resource.name
    #     else:
    #         raise ValueError(f"Dataset '{dataset_name}' not found in datapackage.")

    # def load_datapackage(self):
        # package = Package(self.datapackage_json)
        # for resource in package.resources:
            # if resource.name.startswith(
                # self.dataset_folder
            # ) and not resource.name.endswith(".metadata"):
                # self.resources.append(resource)
# 
        # if not self.resources:
            # raise ValueError(
                # f"No datasets found in'{self.dataset_folder}' within the datapackage."
            # )
        
    def check_datapackage(self, datapackage, datasets):
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

    def setup_logger(self):
        oem2orm.setup_logger()

    def setup_db_connection(self):
        self.db = oem2orm.setup_db_connection()
    
    # 
    #
    # ALTERNATIVE UPLOAD (inkl. PROGRESSBAR-Funktion) ---------------------------------------------------TEST
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
        
        # NUR FüR TESTS --------------------------------------------------------------!
        # Bestehendes Tables entfernen
        for resource in self.resources:
            table_name = resource.name.split(".")[-1]
            table_api_url = f"https://openenergy-platform.org/api/v0/schema/model_draft/tables/{table_name}/"
            req.delete(table_api_url, headers=auth_headers)
            print("Deleted Table for {resource.name}")

        self.create_sql_tables_from_oem(self.datapackage_json, self.oem_paths)
        progress_bar = tqdm(total=len(self.resources))
        for resource in self.resources:
            progress_bar.set_description(f"Uploading {resource.name}")
        
            table_name = resource.name.split(".")[-1]
            resource_abs_path = Path(self.datapackage_basepath) / Path(resource.path)

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
    # 
    #
    # 
    #

    # - [ ] adjust for other filetypes!!
    # def upload_data(self):
    #     auth_headers = {"Authorization": "Token %s" % os.environ["OEP_TOKEN"]}

    #     # NUR FüR TESTS --------------------------------------------------------------!
    #     # Bestehendes Tables entfernen
    #     for resource in self.resources:
    #         table_name = resource.name.split(".")[-1]
    #         table_api_url = f"https://openenergy-platform.org/api/v0/schema/model_draft/tables/{table_name}/"
    #         res = req.delete(table_api_url, headers=auth_headers)

    #     self.create_sql_tables_from_oem(self.datapackage_json, self.oem_paths)
        
    #     for resource in self.resources:
    #         table_name = resource.name.split(".")[-1]
    #         table_api_url = f"https://openenergy-platform.org/api/v0/schema/model_draft/tables/{table_name}/"
    #         resource_abs_path = Path(self.datapackage_basepath) / Path(resource.path)


    #         if resource.format == "csv":
    #             df = pd.read_csv(
    #                 resource_abs_path, encoding="utf8", sep=",", dtype={"RS": "str"}
    #             )
    #             df.columns = map(str.lower, df.columns)
    #             data_to_insert = json.loads(df.to_json(orient="records"))
    #             cli = OepClient(
    #                 token=os.environ["OEP_TOKEN"], default_schema=self.schema
    #             )
    #             cli.insert_into_table(table_name, data_to_insert)
    #             # self.update_oep_metadata(resource.custom["oem"], table_name)

    #         if resource.format == "json":
    #             # table_name = resource.name.split(".")[-1]
    #             # Upload data needs data records in json query
    #             with open(resource_abs_path) as json_data:
    #                 data_to_insert = json.load(json_data)
    #             res = req.post(table_api_url + "rows/new", json={"query": data_to_insert}, headers=auth_headers)

    #             # raise Exception if request fails
    #             if not res.ok:
    #                 raise Exception(res.text)
                
    #         if  resource.format == "gpkg":
    #             # Umwandlung des gpkg in das adäquate Json Format 
    #             data_to_insert = self.gpkg_to_json(resource_abs_path)
    #             res = req.post(table_api_url + "rows/new", json={"query": data_to_insert}, headers=auth_headers)

    #         self.update_oep_metadata(resource.custom["oem"], table_name)
                

    
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


    # def create_sql_tables_from_metadata(self):
    #     metadata_folder = oem2orm.select_oem_dir(oem_folder_name=self.resource_dir, filename="metadata.json")
    #     tables_orm = oem2orm.collect_tables_from_oem(self.db, metadata_folder)
    #     oem2orm.create_tables(self.db, tables_orm)

    # def create_sql_tables_from_oem(self, metadata_file):
    #     # Stelle sicher, dass der Pfad zum Metadatenfile korrekt verarbeitet wird
    #     oem_folders = oem2orm.select_oem_dir(
    #         oem_folder_name=os.path.dirname(metadata_file),
    #         filename=os.path.basename(metadata_file),
    #     )
    #     tables_orm = self.extract_tables_from_oem(self.db, oem_folders)
    #     oem2orm.create_tables(self.db, tables_orm)
                
    def create_sql_tables_from_oem(self, datapackage_path, oem_paths):
        base_path = Path(datapackage_path).parent  # Erhalte den Pfad bis zum Datapackage-Ordner

        for oem_path in oem_paths:
            full_oem_path = base_path / oem_path  # Erstelle den vollen Pfad zum OEM-Ordner
            # oem_folder_name = full_oem_path.parent  # Verzeichnisname
            # filename = full_oem_path.name  # Dateiname
            
            # # Angenommen, oem2orm.select_oem_dir wurde aktualisiert, um direkt den richtigen Path zu liefern
            # oem_folder = oem2orm.select_oem_dir(oem_folder_name=str(oem_folder_name), filename=filename)
            
            # Erstelle Tabellen basierend auf dem OEM
            tables_orm = self.extract_tables_from_oem(self.db, full_oem_path)
            oem2orm.create_tables(self.db, tables_orm)


    def extract_tables_from_oem(self, db, oem_file_path):
        tables = [] 
        if oem_file_path.exists():
            try:
                md_tables = oem2orm.create_tables_from_metadata_file(db, str(oem_file_path))
                logging.info(f"Generated tables from: {oem_file_path}")
                tables.extend(md_tables)
            except Exception as e:
                logging.error(f'Could not generate tables from metadata file: "{oem_file_path}"')
                raise e
        else:
            logging.warning(f"Metadata file not found in: {oem_file_path}")
        return oem2orm.order_tables_by_foreign_keys(tables)

# Beispielaufruf der Funktion
    # def set_metadata(self, metadata_file):
    #     with open(os.path.dirname(metadata_file)) as json_file:
    #         metadata = json.load(json_file)
    #     cli = OepClient(token=os.environ["OEP_TOKEN"], default_schema=self.schema)
    #     cli.set_metadata(self.table_name, metadata)

    def update_oep_metadata(self, metadata_file, table_name):
        metadata_file = Path(self.datapackage_basepath) / Path(metadata_file)
        with open(metadata_file) as json_file:
            metadata = json.load(json_file)
        cli = OepClient(token=os.environ["OEP_TOKEN"], default_schema=self.schema)
        cli.set_metadata(table_name, metadata)

    def run_all(self):
        self.setup_logger()
        self.setup_db_connection()
        self.upload_data()


# Beispiel für die Verwendung (TEST mit datapackage)
oep_oem_handler = OEPDataHandler(
    datapackage_json="output/TEST/datapackage/datapackage.json",
    dataset_names=["rpg_abw_regional_plan", "renewables_ninja_feedin"],
    api_token="0c9889067a4eafa692e433de3ea67acca51510ec",
    oep_username="davidst"
)
oep_oem_handler.run_all()
