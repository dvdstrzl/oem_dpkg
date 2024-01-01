import os
from frictionless import Package, Resource

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

# Erstelle ein Datapackage
datapackage = Package(resources=csv_resources + geopackage_resources)

# Durchlaufe alle Ressourcen im Datapackage
for resource in datapackage.resources:
    resource.infer(stats=True)

# Speichere das Datapackage
datapackage_path = "output/datapackage.json"
datapackage.to_json(datapackage_path)

print(f"Datapackage wurde unter {datapackage_path} erstellt.")
