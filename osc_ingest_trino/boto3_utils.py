"""AWS S3 interoperability functions."""

import os
from pathlib import Path
from typing import Union

import boto3
from mypy_boto3_s3.service_resource import Bucket

__all__ = [
    "upload_directory_to_s3",
    "attach_s3_bucket",
]


def upload_directory_to_s3(path: Union[Path, str], bucket: Bucket, prefix: str, verbose: bool = False) -> None:
    """Upload files to an S3 bucket.

    path -- the directory containing all files to be uploaded.
    bucket -- the S3 bucket.
    prefix -- the prefix prepended to each filename before uploading.
    verbose -- if True, print each file uploaded (with its prefix).
    """
    path = str(path)
    prefix = str(prefix)
    for subdir, dirs, files in os.walk(path):
        for f in files:
            pfx = subdir.replace(path, prefix)
            src = os.path.join(subdir, f)
            dst = os.path.join(pfx, f)
            if verbose:
                print(f"{src}  -->  {dst}")
            bucket.upload_file(src, dst)


def attach_s3_bucket(env_var_prefix: str) -> Bucket:
    """Return the S3 Bucket resource asscoiated with env_var_prefix (typically from `credentials.env`)."""
    s3 = boto3.resource(
        service_name="s3",
        endpoint_url=os.environ[f"{env_var_prefix}_ENDPOINT"],
        aws_access_key_id=os.environ[f"{env_var_prefix}_ACCESS_KEY"],
        aws_secret_access_key=os.environ[f"{env_var_prefix}_SECRET_KEY"],
    )
    return s3.Bucket(os.environ[f"{env_var_prefix}_BUCKET"])
