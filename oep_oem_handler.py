import logging
import os
from pathlib import Path
from frictionless import Package
import pandas as pd
import getpass
import json
import requests as req
from oep_client import OepClient
from oem2orm import oep_oedialect_oem2orm as oem2orm


# - [ ]   def upload_data(self): adjust for other filetypes!!


class OEPDataHandler:
    # def __init__(self, metadata_dir, csv_file_path, table_name, schema='model_draft'):
    #     self.metadata_dir = metadata_dir
    #     self.csv_file_path = csv_file_path
    #     self.table_name = table_name
    #     self.schema = schema
    #     self.db = None
    #     if "OEP_TOKEN" not in os.environ:
    #         os.environ["OEP_TOKEN"] = getpass.getpass('Enter API-Token:')

    #  (TEST mit datapackage und csv)
    # def __init__(self, datapackage, dataset_name, schema='model_draft'):
    #     self.schema = schema
    #     self.db = None
    #     self.load_datapackage(datapackage, dataset_name)
    #     if "OEP_TOKEN" not in os.environ:
    #         os.environ["OEP_TOKEN"] = getpass.getpass('Enter API-Token:')
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

    # def upload_data(self):
    #     # - [ ] adjust for other filetypes!!

    #     df = pd.read_csv(self.resource_file_path, encoding='utf8', sep=',', dtype={'RS': 'str'})
    #     df.columns = map(str.lower, df.columns)
    #     data_to_insert = json.loads(df.to_json(orient="records"))
    #     cli = OepClient(token=os.environ["OEP_TOKEN"], default_schema=self.schema)
    #     cli.insert_into_table(self.table_name, data_to_insert)
    def upload_data(self):
        # Hier wird für jede Ressource die SQL-Tabelle aus Metadaten erstellt
        self.create_sql_tables_from_oem(self.datapackage_json, self.oem_paths)
        for resource in self.resources:
            resource_abs_path = Path(self.datapackage_basepath) / Path(resource.path)
            # Beispiel für das Hochladen von CSV-Daten; für andere Formate muss dies entsprechend angepasst werden
            if resource.format == "csv":
                df = pd.read_csv(
                    resource_abs_path, encoding="utf8", sep=",", dtype={"RS": "str"}
                )
                df.columns = map(str.lower, df.columns)
                data_to_insert = json.loads(df.to_json(orient="records"))
                cli = OepClient(
                    token=os.environ["OEP_TOKEN"], default_schema=self.schema
                )
                table_name = resource.name.split(".")[
                    -1
                ]  # Angenommen, der Tabellenname folgt nach dem letzten Punkt
                cli.insert_into_table(table_name, data_to_insert)
                self.set_metadata(resource.custom["oem"], table_name)

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
            oem_folder_name = full_oem_path.parent  # Verzeichnisname
            filename = full_oem_path.name  # Dateiname
            
            # Angenommen, oem2orm.select_oem_dir wurde aktualisiert, um direkt den richtigen Path zu liefern
            oem_folder = oem2orm.select_oem_dir(oem_folder_name=str(oem_folder_name), filename=filename)
            
            # Erstelle Tabellen basierend auf dem OEM
            tables_orm = self.extract_tables_from_oem(self.db, [oem_folder])
            oem2orm.create_tables(self.db, tables_orm)


    def extract_tables_from_oem(self, db, oem_folder_paths):
        tables = []
        
        for oem_folder_path in oem_folder_paths:
            oem_folder_path = Path(oem_folder_path)  # Stellen Sie sicher, dass es ein Path-Objekt ist
            oem_file = oem_folder_path / "metadata.json"
            
            if oem_file.exists():
                try:
                    md_tables = oem2orm.create_tables_from_metadata_file(db, str(oem_file))
                    logging.info(f"Generated tables from: {oem_file}")
                    tables.extend(md_tables)
                except Exception as e:
                    logging.error(f'Could not generate tables from metadata file: "{oem_file}"')
                    raise e
            else:
                logging.warning(f"Metadata file not found in: {oem_folder_path}")

        return oem2orm.order_tables_by_foreign_keys(tables)

# Beispielaufruf der Funktion
    # def set_metadata(self, metadata_file):
    #     with open(os.path.dirname(metadata_file)) as json_file:
    #         metadata = json.load(json_file)
    #     cli = OepClient(token=os.environ["OEP_TOKEN"], default_schema=self.schema)
    #     cli.set_metadata(self.table_name, metadata)

    def set_metadata(self, metadata_file, table_name):
        metadata_file = Path(self.datapackage_basepath) / Path(metadata_file)
        with open(metadata_file) as json_file:
            metadata = json.load(json_file)
        cli = OepClient(token=os.environ["OEP_TOKEN"], default_schema=self.schema)
        cli.set_metadata(table_name, metadata)

    def run_all(self):
        self.setup_logger()
        self.setup_db_connection()
        # self.create_sql_tables_from_metadata()
        self.upload_data()
        # self.set_metadata(self.dataset_oem)


# # Beispiel für die Verwendung:
# oep_oem_handler = OEPDataHandler(
#     metadata_dir="input/testfiles/renewables.ninja_feedin",
#     csv_file_path="input/testfiles/renewables.ninja_feedin/pv_feedin_timeseries.csv",
#     table_name="pv_feedin_timeseries"
# )
# oep_oem_handler.run_all()

# Beispiel für die Verwendung (TEST mit datapackage)
oep_oem_handler = OEPDataHandler(
    datapackage_json="output/CLI/datapackage/datapackage.json",
    dataset_names=["renewables_ninja_feedin"],
    api_token="0c9889067a4eafa692e433de3ea67acca51510ec",
    oep_username="davidst"
)
oep_oem_handler.run_all()
