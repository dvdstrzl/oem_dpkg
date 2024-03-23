import os
from pathlib import Path
from typing import Any, Dict, List
import fiona
import json

import pandas as pd
import geopandas as gpd

def get_folder_name(file_path: Path) -> str:    
    return Path(file_path).parent.name

def find_files(start_path: Path, ignore_files: List[str] = None, ignore_exts: List[str] = None, specific_files: List[str] = None) -> List[Path]:
    """
    Finds and returns a list of file paths under the given start path, 
    applying various filters based on file names, extensions, and a list of specific files.

    Parameters:
    - start_path (Path): The directory to start searching from.
    - ignore_files (List[str], optional): Filenames to ignore.
    - ignore_exts (List[str], optional): File extensions to ignore.
    - specific_files (List[str], optional): Specific filenames to include.

    Returns:
    - List[Path]: A list of paths to files that match the search criteria.
    """    
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

def find_metadata_json_files(start_path: str) -> List[str]:
    """
    Finds and returns a list of paths to 'metadata.json' files within the given directory.

    Parameters:
    - start_path (str): The directory to start searching from.

    Returns:
    - List[str]: A list of paths to 'metadata.json' files found within the directory.
    """
    metadata_file_paths = []  # List to store paths to metadata.json files

    # Recursively walk through all directories starting from start_path
    for dirpath, dirnames, filenames in os.walk(start_path):
        # Check if 'metadata.json' is among the files in the current directory
        if 'metadata.json' in filenames:
            # Add the path to the metadata.json file to the list
            metadata_file_paths.append(os.path.join(dirpath, 'metadata.json'))

    return metadata_file_paths


def get_crs_from_gpkg(gpkg_path: str) -> Dict[str, Any]:
    """
    Extracts and returns the Coordinate Reference System (CRS) from a GeoPackage file.

    Parameters:
    - gpkg_path (str): The path to the GeoPackage file.

    Returns:
    - Dict[str, Any]: The CRS of the GeoPackage.
    """
    with fiona.open(gpkg_path) as src:
        return src.crs

def load_json(path: str) -> Any:
    """
    Loads and returns the content of a JSON file.

    Parameters:
    - path (str): The path to the JSON file.

    Returns:
    - Any: The content of the JSON file.
    """
    with open(path, 'r') as file:
        return json.load(file)

def save_json(data: Any, path: str) -> None:
    """
    Saves the given data to a JSON file at the specified path.

    Parameters:
    - data (Any): The data to save.
    - path (str): The path to the JSON file where the data will be saved.
    """
    with open(path, 'w') as file:
        json.dump(data, file, indent=4)


def find_dataset_paths(start_path: str) -> List[str]:
    """
    Finds and returns a list of dataset file paths, excluding '.gitkeep' and 'datapackage.json', within the given directory.

    Parameters:
    - start_path (str): The directory to start searching from.

    Returns:
    - List[str]: A list of paths to dataset files found within the directory.
    """
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


# Utils for OEP Data Handler

def prepare_csv_data(resource_abs_path: Path) -> List[Dict[str, Any]]:
    """
    Reads a CSV file and returns its content as a list of dictionaries, with column names converted to lowercase.

    Parameters:
    - resource_abs_path (Path): The path to the CSV file.

    Returns:
    - List[Dict[str, Any]]: The content of the CSV file as a list of dictionaries.
    """
    df = pd.read_csv(resource_abs_path, encoding="utf8", sep=",", dtype={"RS": "str"})
    df.columns = map(str.lower, df.columns)
    return json.loads(df.to_json(orient="records"))

def prepare_json_data(resource_abs_path: Path) -> Dict[str, Any]:
    """
    Loads and returns the content of a JSON file.

    Parameters:
    - resource_abs_path (Path): The path to the JSON file.

    Returns:
    - Dict[str, Any]: The content of the JSON file.
    """
    with open(resource_abs_path) as json_data:
        return json.load(json_data)

def prepare_gpkg_data(resource_abs_path: Path) -> List[Dict[str, Any]]:
    """
    Reads a GeoPackage file and returns its content as a list of dictionaries, including geometries converted to WKT format.

    Parameters:
    - resource_abs_path (Path): The path to the GeoPackage file.

    Returns:
    - List[Dict[str, Any]]: The content of the GeoPackage file as a list of dictionaries.
    """    
    gdf = gpd.read_file(resource_abs_path)
    feature_list = []
    for _, row in gdf.iterrows():
        feature = row.to_dict()
        if 'geometry' in feature and feature['geometry'] is not None:
            feature['geometry'] = feature['geometry'].wkt
        feature_list.append(feature)
    return feature_list


