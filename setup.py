from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="mydatapackage",
    version="0.1.0",
    author="David Starzl",
    author_email="davidstarzl@posteo.net",
    description="Currently no description.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dvdstrzl/ba_datapackage",
    packages=find_packages(),
    install_requires=[
        # List of dependencies
        "pandas>=1.0",
        "frictionless>=5.0.0",
    ],
    python_requires=">=3.6",
)
