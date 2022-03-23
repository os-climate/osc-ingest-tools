import os
import pathlib

from dotenv import load_dotenv

__all__ = [
    "load_credentials_dotenv",
]


def load_credentials_dotenv():
    # Load some standard environment variables from a dot-env file, if it exists.
    # If no such file can be found, does not fail, and so allows these environment vars to
    # be populated in some other way
    dotenv_dir = os.environ.get("CREDENTIAL_DOTENV_DIR", os.environ.get("PWD", "/opt/app-root/src"))
    dotenv_path = pathlib.Path(dotenv_dir) / "credentials.env"
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path, override=True)
