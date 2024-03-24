from setuptools import setup, find_packages
import os

with open("README.md", "r") as fh:
    long_description = fh.read()

lib_folder = os.path.dirname(os.path.realpath(__file__))
requirement_path = f"{lib_folder}/requirements.txt"
install_requires = []
if os.path.isfile(requirement_path):
    with open(requirement_path) as f:
        install_requires = f.read().splitlines()

setup(
    name="oem_datapkg",
    version="1.0",
    author="David Starzl",
    author_email="davidstarzl@posteo.net",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dvdstrzl/ba_datapackage",
    packages=find_packages(),
    install_requires=install_requires,
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "oemdp=cli:cli",
        ],
    },
)
