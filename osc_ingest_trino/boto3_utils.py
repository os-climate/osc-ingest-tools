import os

import boto3

__all__ = [
    "upload_directory_to_s3",
    "attach_s3_bucket",
]


def upload_directory_to_s3(path, bucket, prefix, verbose=False):
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


def attach_s3_bucket(env_var_prefix):
    s3 = boto3.resource(
        service_name="s3",
        endpoint_url=os.environ[f"{env_var_prefix}_ENDPOINT"],
        aws_access_key_id=os.environ[f"{env_var_prefix}_ACCESS_KEY"],
        aws_secret_access_key=os.environ[f"{env_var_prefix}_SECRET_KEY"],
    )
    return s3.Bucket(os.environ[f"{env_var_prefix}_BUCKET"])
