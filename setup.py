import pathlib

from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).resolve().parent

# The text of the README file is used as a description
README = (HERE / "README.md").read_text()

setup(
    name = "osc-ingest-tools",
    version = "0.1.0",
    description = "python tools to assist with standardized data ingestion workflows for the OS-Climate project",
    long_description = README,
    long_description_content_type = "text/markdown",
    url = "https://github.com/os-climate/osc-ingest-tools",
    author = "OS-Climate",
    author_email = "eje@redhat.com",
    license = "Apache-2.0",
    classifiers = [
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
    ],
    packages = ["osc_ingest_trino"],
    include_package_data = True,
    install_requires = [],
    entry_points = {
        "console_scripts": []
    },
)

