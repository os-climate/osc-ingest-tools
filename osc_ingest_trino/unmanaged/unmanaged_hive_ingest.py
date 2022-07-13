import shutil
import uuid

import pandas as pd

from osc_ingest_trino import create_table_schema_pairs, upload_directory_to_s3

__all__ = [
    "drop_unmanaged_table",
    "drop_unmanaged_data",
    "ingest_unmanaged_parquet",
    "unmanaged_parquet_tabledef",
]

_default_prefix = "trino/{schema}/{table}"


def _remove_trailing_slash(s):
    s = str(s)
    if len(s) == 0:
        return s
    if s[-1] != "/":
        return s
    return _remove_trailing_slash(s[:-1])


def _prefix(pfx, schema, table):
    return _remove_trailing_slash(pfx).format(schema=schema, table=table)


def drop_unmanaged_table(catalog, schema, table, engine, bucket, prefix=_default_prefix, verbose=False):
    sql = f"drop table if exists {catalog}.{schema}.{table}"
    qres = engine.execute(sql)
    dres = bucket.objects.filter(Prefix=f"{_prefix(prefix, schema, table)}/").delete()
    if verbose:
        print(dres)
    return qres


def drop_unmanaged_data(schema, table, bucket, prefix=_default_prefix, verbose=False):
    dres = bucket.objects.filter(Prefix=f"{_prefix(prefix, schema, table)}/").delete()
    if verbose:
        print(dres)
    return dres


def ingest_unmanaged_parquet(
    df, schema, table, bucket, partition_columns=[], append=True, workdir="/tmp", prefix=_default_prefix, verbose=False
):
    if not isinstance(df, pd.DataFrame):
        raise ValueError("df must be a pandas DataFrame")
    if not isinstance(partition_columns, list):
        raise ValueError("partition_columns must be list of column names")

    s3pfx = _prefix(prefix, schema, table)

    if not append:
        dres = bucket.objects.filter(Prefix=f"{s3pfx}/").delete()
        if verbose:
            print(dres)

    if len(partition_columns) > 0:
        # tell pandas to write a directory tree, using partitions
        tmp = f"{workdir}/{table}"
        # pandas does not clean out destination directory for you:
        shutil.rmtree(tmp, ignore_errors=True)
        df.to_parquet(tmp, partition_cols=partition_columns, index=False)
        # upload the tree onto S3
        upload_directory_to_s3(tmp, bucket, s3pfx, verbose=verbose)
    else:
        # do not use partitions: a single parquet file is created
        parquet = f"{uuid.uuid4().hex}.parquet"
        tmp = f"{workdir}/{parquet}"
        df.to_parquet(tmp, index=False)
        dst = f"{s3pfx}/{parquet}"
        if verbose:
            print(f"{tmp}  -->  {dst}")
        bucket.upload_file(tmp, dst)


def unmanaged_parquet_tabledef(
    df, catalog, schema, table, bucket, partition_columns=[], typemap={}, colmap={}, verbose=False
):
    if not isinstance(df, pd.DataFrame):
        raise ValueError("df must be a pandas DataFrame")
    if not isinstance(partition_columns, list):
        raise ValueError("partition_columns must be list of column names")

    columnschema = create_table_schema_pairs(df, typemap=typemap, colmap=colmap)

    tabledef = f"create table if not exists {catalog}.{schema}.{table} (\n"
    tabledef += f"{columnschema}\n"
    tabledef += ") with (\n    format = 'parquet',\n"
    if len(partition_columns) > 0:
        tabledef += f"    partitioned_by = array{partition_columns},\n"
    tabledef += f"    external_location = 's3a://{bucket.name}/trino/{schema}/{table}/'\n)"

    if verbose:
        print(tabledef)
    return tabledef
