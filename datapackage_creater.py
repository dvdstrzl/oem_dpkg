from typing import List, Union
from pathlib import Path
from frictionless import Package, Resource
from datetime import datetime, timezone
import shutil
import json
from utils import find_metadata_json_files, find_dataset_paths, get_crs_from_gpkg, get_folder_name
from omi.dialects.oep.parser import JSONParser

# from metadata.v152.schema import OEMETADATA_V152_SCHEMA
# from metadata.v160.schema import OEMETADATA_V160_SCHEMA
from metadata.latest.schema import OEMETADATA_LATEST_SCHEMA

class CustomPackage:
    def __init__(
        self, 
        input_path: Union[str, Path], 
        output_path: Union[str, Path], 
        name: str, 
        description: str, 
        version: str, 
        oem: bool = True
    ) -> None:
        self.input_path: Path = Path(input_path)
        self.output_path: Path = Path(output_path) / 'datapackage'
        self.name: str = name
        self.description: str = description
        self.version: str = version
        self.created_date: datetime = datetime.now(timezone.utc)
        self.oem: bool = oem
        self.oem_schema: dict = OEMETADATA_LATEST_SCHEMA
        self.resources: List[Resource] = []
        self.oem_validity_reports_path: Path = Path(output_path) / 'oem_validity_reports'
        if self.oem_validity_reports_path.exists():
            shutil.rmtree(self.oem_validity_reports_path)

    def create(self) -> None:
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.copy_datasets_and_metadata()
        self.create_resources()
        self.create_package()

    def copy_datasets_and_metadata(self) -> None:        
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

    def make_paths_relative(self, package: Package, base_path: Union[str, Path]) -> None:
        for resource in package.resources:
            resource.path = str(Path(resource.path).relative_to(base_path))
            oem_path = resource.custom.get('oem_path')
            if oem_path:
                resource.custom['oem_path'] = str(Path(oem_path).relative_to(base_path))

    
    def create_resources(self) -> None:
        for file_path in find_dataset_paths(self.output_path):
            resource = Resource(path=str(file_path))
            if resource.format == 'gpkg':
                crs = get_crs_from_gpkg(resource.path)
                resource.custom["crs"] = str(crs)
            if self.oem:
                self.reference_and_validate_oem_metadata(resource, file_path)
            resource.name = f"{get_folder_name(resource.path)}.{resource.name}"
            self.resources.append(resource)


    def reference_and_validate_oem_metadata(self, resource: Resource, file_path: Union[str, Path]) -> None:        
        metadata_files = find_metadata_json_files(Path(file_path).parent)
        if resource.name != "metadata":
            if metadata_files:
                oem_file = str(metadata_files[0])
                if self.validate_oem(oem_file, self.oem_schema):
                    resource.custom["oem_path"] = oem_file
                    resource.custom["oem_schema_validity"] = self.oem_schema['description']
                else:
                    resource.custom["oem_schema_validity"] = "INVALID! Check report for details."
            else:
                print(f"MISSING OEMetadata for '{file_path}'!")
        else: 
            resource.name = resource.name.replace("metadata", "oem")
    
    def validate_oem(self, oem: str, oem_schema: dict) -> bool:        
        with open(oem, "r", encoding="utf-8") as f:
            oem_loaded = json.load(f)

        parser = JSONParser()
        schema = oem_schema

        report = parser.validate(oem_loaded, schema, save_report=False)

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

    def create_package(self) -> None:
        try:
            package = Package(
            basepath=self.output_path,
            name=self.name,
            description=self.description,
            version=self.version,
            created=self.created_date.isoformat(),
            resources=self.resources,
            contributors=[],
            keywords=[],
            licenses=[]
        )            
            self.make_paths_relative(package, self.output_path)
            package.to_json(str(self.output_path / 'datapackage.json'))
            print(f"\nData package successfully created: '{self.output_path}/'")
        except Exception as e:
            print(f"Could not create data package!\n{e}")

            

# -----------------------------------------
# Beispielaufruf (TEST)
input_path = "IGNORE/latest_test_path"
output_path = "output/LATEST/"
package = CustomPackage(input_path, output_path, name="your-data-package", description="Describe your data package.", version="0.1", oem=True)
package.create()
