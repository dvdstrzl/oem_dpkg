from datetime import datetime
import shutil
from frictionless import Package, Resource
from pathlib import Path
import json
import fiona
from utils import find_metadata_json_files, find_dataset_paths, get_crs_from_gpkg, get_folder_name

class CustomPackage:
    def __init__(self, input_path, output_path, name, description, version, oem=True):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path) / 'datapackage'
        self.name = name
        self.description = description
        self.version = version
        self.oem = oem
        self.resources = []

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

    def attach_oem_metadata(self, resource, file_path):
        metadata_files = find_metadata_json_files(Path(file_path).parent)
        if resource.name != "metadata":
            if metadata_files:
                resource.custom["oem"] = str(metadata_files[0])
            else:
                resource.custom["oem"] = ""
                print(f"No OEM (metadata.json) found for '{get_folder_name(resource.path)}'!")

    def create_package(self):
        package = Package(
            basepath=self.output_path,
            name=self.name,
            description=self.description,
            version=self.version,
            resources=self.resources
        )
        self.make_paths_relative(package, self.output_path)
        package.to_json(str(self.output_path / 'datapackage.json'))


# # Beispielaufruf
input_path = "IGNORE/testing/input/pipefiles/raw"
output_path = "output/CLI/"
package = CustomPackage(input_path, output_path, name="unique-identifier", description="Datapackage Description", version="0.9", oem=True)
package.create()
