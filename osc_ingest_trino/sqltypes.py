"""Functions to translate Pandas dataframes to SQL equivalents."""

from typing import Dict

import pandas as pd

__all__ = [
    "pandas_type_to_sql",
    "create_table_schema_pairs",
]

_p2smap = {
    "string": "varchar",
    "float32": "real",
    "Float32": "real",
    "float64": "double",
    "Float64": "double",
    "int32": "integer",
    "Int32": "integer",
    "int64": "bigint",
    "Int64": "bigint",
    "bool": "boolean",
    "boolean": "boolean",
    "category": "varchar",
    "datetime64[ns, UTC]": "timestamp",
}


def pandas_type_to_sql(pt: str, typemap: Dict[str, str] = {}):
    """Return the SQL type corresponding to the pandas type `pt` (using special mappings, if any, from `typemap`)."""
    if not isinstance(typemap, dict):
        raise ValueError("typemap must be a dict")
    # user defined typemap overrides _p2smap
    st = typemap.get(pt, _p2smap.get(pt))
    if st is not None:
        return st
    raise ValueError("unexpected pandas column type '{pt}'".format(pt=pt))


def create_table_schema_pairs(
    df: pd.DataFrame,
    typemap: Dict[str, str] = {},
    colmap: Dict[str, str] = {},
    indent: int = 4,
) -> str:
    """Create SQL column, type pairs that can appear in a CREATE TABLE operation.

    df -- the dataframe to be rendered as a SQL table
    typemap -- mappings from dtypes to SQL types above and beyond our defaults
    colmap -- mappings of df column names to SQL column names if not using defaults
    indent -- how many spaces of indent to make our SQL declarations pretty
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("df must be a pandas DataFrame")
    if not isinstance(colmap, dict):
        raise ValueError("colmap must be a dict")
    columns = df.columns.to_list()
    try:
        types = [colmap.get(col, pandas_type_to_sql(str(df[col].dtype), typemap=typemap)) for col in columns]
    except ValueError as exc:
        raise ValueError(f"df.dtypes\n{df.dtypes}\nraised {exc}")
    return ",\n".join([f"{' '*indent}{e[0]} {e[1]}" for e in zip(columns, types)])
