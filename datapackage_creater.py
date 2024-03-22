from datetime import datetime
import os
import shutil
from frictionless import Package, Resource
from pathlib import Path
import json
import fiona
from utils import find_metadata_json_files, find_dataset_paths, get_crs_from_gpkg, get_folder_name
from omi.dialects.oep.parser import JSONParser

# from metadata.v150.schema import OEMETADATA_V150_SCHEMA
from metadata.latest.schema import OEMETADATA_LATEST_SCHEMA

class CustomPackage:
    def __init__(self, input_path, output_path, name, description, version, oem=True):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path) / 'datapackage'
        self.name = name
        self.description = description
        self.version = version
        self.oem = oem
        self.oem_schema = OEMETADATA_LATEST_SCHEMA
        self.resources = []
        self.oem_validity_reports_path = Path(output_path) / 'oem_validity_reports'
        if self.oem_validity_reports_path.exists():
            shutil.rmtree(self.oem_validity_reports_path)

    def create(self):
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.copy_datasets_and_metadata()
        self.create_resources()
        self.create_package()

    def copy_datasets_and_metadata(self):
        # Gehe alle Unterordner im input_path durch
        for dataset_dir in self.input_path.iterdir():
            if dataset_dir.is_dir():
                data_path = dataset_dir / 'data'
                target_dataset_path = self.output_path / dataset_dir.name

                # Erstelle den Zielordner, falls er noch nicht existiert
                target_dataset_path.mkdir(parents=True, exist_ok=True)

                # Kopiere metadata.json, falls vorhanden
                metadata_path = dataset_dir / 'metadata.json'
                if metadata_path.exists():
                    shutil.copy(metadata_path, target_dataset_path)
                
                # Gehe alle Dateien im 'data'-Ordner durch und kopiere sie
                if data_path.is_dir():
                    for file in data_path.iterdir():
                        if file.is_file() and not file.name.endswith('.gitkeep'):
                            shutil.copy(file, target_dataset_path)

    def make_paths_relative(self, package, base_path):
        for resource in package.resources:
            resource.path = str(Path(resource.path).relative_to(base_path))
            oem_path = resource.custom.get('oem')
            if oem_path:
                resource.custom['oem'] = str(Path(oem_path).relative_to(base_path))

    
    def create_resources(self):
        for file_path in find_dataset_paths(self.output_path):
            resource = Resource(path=str(file_path))
            if resource.format == 'gpkg':
                crs = get_crs_from_gpkg(resource.path)
                resource.custom["crs"] = str(crs)
            if self.oem:
                self.attach_oem_metadata(resource, file_path)
            resource.name = f"{get_folder_name(resource.path)}.{resource.name}"
            self.resources.append(resource)

    # def create_resources(self):
    #         for file_path in find_dataset_paths(self.output_path):
    #             relative_path = self.make_relative_path(Path(file_path))
    #             resource = Resource(path=str(relative_path))
    #             if resource.format == 'gpkg':
    #                 crs = get_crs_from_gpkg(resource.path)
    #                 resource.custom["crs"] = str(crs)
    #             if self.oem:
    #                 self.attach_oem_metadata(resource, file_path)
    #             resource.name = f"{get_folder_name(resource.path)}.{resource.name}"
    #             self.resources.append(resource)

    # def attach_oem_metadata(self, resource, file_path):
    #     metadata_files = find_metadata_json_files(Path(file_path).parent)
    #     if metadata_files:
    #         relative_metadata_path = self.make_relative_path(metadata_files[0])
    #         resource.custom["oem"] = str(relative_metadata_path)
    #     else:
    #         print(f"No OEM (metadata.json) found for '{get_folder_name(resource.path)}'!")


    # ------ "metadata-files" als liste ist doch hier irgendwie nicht optimal? 
    #              wird ja nur auf eine zugegriffen... geht wohl eleganter
    def attach_oem_metadata(self, resource, file_path):
        metadata_files = find_metadata_json_files(Path(file_path).parent)
        if resource.name != "metadata":
            if metadata_files:
                resource.custom["oem"] = str(metadata_files[0])
                if self.validate_oem(str(metadata_files[0]), self.oem_schema):
                    resource.custom["oem_validity"] = self.oem_schema['description']
                else:
                    resource.custom["oem_validity"] = "INVALID"
            else:
                resource.custom["oem"] = ""
                resource.custom["oem_validated"] = ""
                print(f"MISSING OEMetadata for '{file_path}'!")
    
    def validate_oem(self, oem, oem_schema):
        with open(oem, "r", encoding="utf-8") as f:
            oem_loaded = json.load(f)

        parser = JSONParser()
        schema = oem_schema

        report = parser.validate(oem_loaded, schema, save_report=False)

        # Erstelle den Report-Ordner, falls nicht vorhanden
        if report:
            self.oem_validity_reports_path.mkdir(parents=True, exist_ok=True)
            oem_report_filename = f"{self.oem_validity_reports_path}/oem_validity_report.{get_folder_name(oem)}.json"
            if not Path(oem_report_filename).exists():
                with open(oem_report_filename, "w", encoding="utf-8") as fp:
                    json.dump(report, fp, indent=4, sort_keys=False)
                print(f"\nINFO:\nOEMetadata for dataset '{get_folder_name(oem)}' does not fully comply with '{schema['description']}'... \nCheck validity report: '{oem_report_filename}'")
            return False
        else:
            return True

    def create_package(self):
        try:
            package = Package(
            basepath=self.output_path,
            name=self.name,
            description=self.description,
            version=self.version,
            resources=self.resources
        )            
            self.make_paths_relative(package, self.output_path)
            package.to_json(str(self.output_path / 'datapackage.json'))
            print(f"\nDatapackage successfully created: '{self.output_path}/'")
        except Exception as e:
            print(f"Could not create datapackage! Error: {e}")

            


# # Beispielaufruf
input_path = "IGNORE/latest_test_path"
output_path = "output/LATEST/"
package = CustomPackage(input_path, output_path, name="unique-identifier", description="Datapackage Description", version="0.9", oem=True)
package.create()
