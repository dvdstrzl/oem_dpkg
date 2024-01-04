from datetime import datetime
import os
from frictionless import Package, Resource
import geopandas as gpd

# --- Work-in-progress ---
# --- Hier sollen alle zentralen Funktionen definiert werden welche von diesem Package zur Verfügung gestellt werden --- 

def get_resources(folder_path):
    # (sollte wohl noch für path-Angaben in config.yml angepasst werden) 
    all_files = os.listdir(folder_path)

    # csv und gpkgs seperat erfassen... 
    # (momentan unnötig, aber vielleicht brauche ich das später noch)
    csv_files = [f for f in all_files if f.endswith(".csv")]
    gpkg_files = [f for f in all_files if f.endswith(".gpkg")]

    csv_resources = [Resource(path=os.path.join(folder_path, f)) for f in csv_files]
    gpkg_resources = [Resource(path=os.path.join(folder_path, f)) for f in gpkg_files]

    return csv_resources + gpkg_resources

def create_datapackage(input_folder, name, description, version):
    # Get resources
    resources = get_resources(input_folder)

    # Create a Datapackage
    datapackage = Package(
        name=name,
        title="Titel",
        description=description,
        created=str(datetime.now()),
        version=version,
        resources=resources
    )
    datapackage.custom["schema-version"] = "1.0"
    datapackage.custom["data_version“"] ="0.1"
 

    # Infer and process GeoPackage resources
    for resource in datapackage.resources:
        resource.infer(stats=True)
        if resource.format == "gpkg":
            gdf = gpd.read_file(resource.path)
            resource.custom["crs"] = str(gdf.crs)
            fields = {"fields": []}
            for column in gdf.columns:
                field = {"name": column, "type": str(gdf[column].dtype)}
                fields["fields"].append(field)
            resource.custom["schema"] = fields


    # Save the Datapackage
    output_path = "output/datapackage.json"
    datapackage.to_json(output_path)
    datapackage.to_zip("output/datapackage.zip")

    print(f"Datapackage wurde unter {output_path} erstellt.")



# Beispielaufrufe
input_folder_path = "input"
create_datapackage(input_folder_path, name="a-unique-human-readable-and-url-usable-identifier", description="Datapackage für XYZ", version="1.0.0")
