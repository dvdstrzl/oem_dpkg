import os
from pathlib import Path
import fiona
import json

def get_folder_name(file_path):
    return Path(file_path).parent.name

def find_files(start_path, ignore_files=None, ignore_exts=None, specific_files=None):
    found_files = []
    start_path = Path(start_path)

    if ignore_files is None:
        ignore_files = []
    if ignore_exts is None:
        ignore_exts = ['gitkeep']
    if specific_files is None:
        specific_files = []

    for item in start_path.rglob('*'):
        if item.is_file():
            if specific_files and item.name in specific_files:
                found_files.append(item)
                continue
            if item.name in ignore_files or any(item.name.endswith(ext) for ext in ignore_exts):
                continue
            found_files.append(item)

    return found_files

# def find_metadata_json_files(start_path):
#     return list(find_files(start_path, ignore_exts=[], ignore_files=[], specific_files=['metadata.json']))

def find_metadata_json_files(start_path):
    metadata_file_paths = []  # List to store paths to metadata.json files

    # Recursively walk through all directories starting from start_path
    for dirpath, dirnames, filenames in os.walk(start_path):
        # Check if 'metadata.json' is among the files in the current directory
        if 'metadata.json' in filenames:
            # Add the path to the metadata.json file to the list
            metadata_file_paths.append(os.path.join(dirpath, 'metadata.json'))

    return metadata_file_paths


def get_crs_from_gpkg(gpkg_path):
    with fiona.open(gpkg_path) as src:
        return src.crs

def load_json(path):
    with open(path, 'r') as file:
        return json.load(file)

def save_json(data, path):
    with open(path, 'w') as file:
        json.dump(data, file, indent=4)


def find_dataset_paths(start_path):
    file_paths = []
    # Prüfe, ob start_path ein Verzeichnis ist
    if os.path.isdir(start_path):
        # Durchlaufe rekursiv alle Verzeichnisse ab start_path
        for dirpath, _, filenames in os.walk(start_path):
            for filename in filenames:
                if not filename.endswith(".gitkeep") and filename != 'datapackage.json':
                    file_paths.append(os.path.join(dirpath, filename))
    else:
        # Wenn start_path kein Verzeichnis ist, füge den Pfad direkt hinzu, falls er gültig ist
        if os.path.exists(start_path) and not start_path.endswith(".gitkeep") and start_path != 'datapackage.json':
            file_paths.append(start_path)
    return file_paths


