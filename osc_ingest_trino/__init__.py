"""
osc-ingest-tools

python tools to assist with standardized data ingestion workflows for the OS-Climate project
"""

# defines the release version for this python package
__version__ = "0.2.2"

from .sqlcols import *
from .sqltypes import *
from .boto3_utils import *
from .dotenv_utils import *
from .trino_utils import *

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
    "drop_unmanaged_table",
    "drop_unmanaged_data",
    "ingest_unmanaged_parquet",
    "unmanaged_parquet_tabledef",
]

