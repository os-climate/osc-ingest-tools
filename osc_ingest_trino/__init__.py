"""
osc-ingest-tools

python tools to assist with standardized data ingestion workflows for the OS-Climate project
"""

# defines the release version for this python package
__version__ = "0.3.0"

from .boto3_utils import attach_s3_bucket, upload_directory_to_s3
from .dotenv_utils import load_credentials_dotenv
from .sqlcols import enforce_partition_column_order, enforce_sql_column_names, sql_compliant_name
from .sqltypes import create_table_schema_pairs, pandas_type_to_sql
from .trino_utils import TrinoBatchInsert, attach_trino_engine

__all__ = [
    "sql_compliant_name",
    "enforce_sql_column_names",
    "enforce_partition_column_order",
    "pandas_type_to_sql",
    "create_table_schema_pairs",
    "attach_s3_bucket",
    "upload_directory_to_s3",
    "load_credentials_dotenv",
    "attach_trino_engine",
    "TrinoBatchInsert",
]
