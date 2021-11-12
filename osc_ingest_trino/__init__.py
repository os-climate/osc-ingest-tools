"""
osc-ingest-tools

python tools to assist with standardized data ingestion workflows for the OS-Climate project
"""

# defines the release version for this python package
__version__ = "0.1.1"

from .sqlcols import *
from .sqltypes import *

__all__ = [
    "sql_compliant_name",
    "enforce_sql_column_names",
    "pandas_type_to_sql",
    "create_table_schema_pairs",
]

