from datetime import datetime
import os
from frictionless import Package, Resource, describe, validate
import geopandas as gpd
import json
from omi.dialects.oep.parser import JSONParser

# --- Hier wird mit verschiedenen Funktionen und Möglichkeiten der Verarbeitung experimentiert ---

# Pfad zum Ordner mit den Dateien
ordner_pfad = "input"

# Liste aller Dateien im Ordner
alle_dateien = os.listdir(ordner_pfad)

# Filtere CSV- und GeoPackage-Dateien
csv_dateien = [f for f in alle_dateien if f.endswith(".csv")]
geopackage_dateien = [f for f in alle_dateien if f.endswith(".gpkg")]

# Erstelle Ressourcen für CSV-Dateien
csv_resources = [Resource(path=os.path.join(ordner_pfad, f)) for f in csv_dateien]

# Erstelle Ressourcen für GeoPackage-Dateien
geopackage_resources = [Resource(path=os.path.join(ordner_pfad, f)) for f in geopackage_dateien]

date = datetime.now()
# Erstelle ein Datapackage
datapackage = Package(name="my-datapackge", description="Datapackage -- via erweitertem Frictionless Framework erstellt!", created=str(datetime.now()), version="0.0.1", resources=csv_resources + geopackage_resources)

# Durchlaufe alle Ressourcen im Datapackage
for resource in datapackage.resources:
    resource.infer(stats=True)
    # if resource.format == "gpkg":
    #     gdf = gpd.read_file(resource.path)
    #     resource.custom["crs"] = str(gdf.crs)
    #     fields= {"fields": []}
    #     for column in gdf.columns:
    #         field = {"name": column, "type": str(gdf[column].dtype)}
    #         fields["fields"].append(field)
    #     resource.custom["schema"] = fields

# Speichere das Datapackage
datapackage_path = "datapackage.json"
datapackage.to_json(datapackage_path)

print(f"Datapackage wurde unter {datapackage_path} erstellt.")

print(validate("input/bnetza_mastr_pv_ground_agg_region.gpkg"))

# with open(datapackage_path, "r", encoding="utf-8") as f:
#     metadata = json.load(f)

# parser = JSONParser()
# parser.validate(metadata)

# # check if your metadata is valid for the given schmea
# parser.is_valid(metadata, schema=False)


from shapely.geometry import shape

def detailed_validation(filepath):
    gdf = gpd.read_file(filepath)
    for geom in gdf.geometry:
        shapely_geom = shape(geom)
        # Beispiel: Überprüfe, ob die Geometrie gültig ist
        if not shapely_geom.is_valid:
            print("Ungültige Geometrie gefunden:", shapely_geom)
        # Weitere Validierungen wie Überlappungen, Topologie usw. können durchgeführt werden
            
detailed_validation("input/bnetza_mastr_pv_ground_agg_region.gpkg")

def validate_attributes(filepath):
    gdf = gpd.read_file(filepath)
    # Überprüfe die Attributfelder auf fehlende oder inkonsistente Werte
    for column in gdf.columns:
        null_values = gdf[gdf[column].isnull()]
        if not null_values.empty:
            print(f"Fehlende Werte in Spalte {column}: {null_values}")
    # Weitere Validierungen wie unerwartete Werte oder Datentypen sind möglich

validate_attributes("input/bnetza_mastr_biomass_agg_region.gpkg")
