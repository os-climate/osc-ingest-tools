import pandas as pd

__all__ = [
    "pandas_type_to_sql",
    "create_table_schema_pairs",
]

_p2smap = {
    'string': 'varchar',
    'float32': 'real',
    'Float32': 'real',
    'float64': 'double',
    'Float64': 'double',
    'int32': 'integer',
    'Int32': 'integer',
    'int64': 'bigint',
    'Int64': 'bigint',
    'bool': 'boolean',
    'category': 'varchar',
    'datetime64[ns, UTC]': 'timestamp(6)',
}

def pandas_type_to_sql(pt, typemap={}):
    if not isinstance(typemap, dict):
        raise ValueError("typemap must be a dict")
    # user defined typemap overrides _p2smap
    st = typemap.get(pt, _p2smap.get(pt))
    if st is not None:
        return st
    raise ValueError("unexpected pandas column type '{pt}'".format(pt=pt))

def create_table_schema_pairs(df, typemap = {}, indent = 4):
    if not isinstance(df, pd.DataFrame):
        raise ValueError("df must be a pandas DataFrame")
    ptypes = [str(e) for e in df.dtypes.to_list()]
    stypes = [pandas_type_to_sql(e, typemap=typemap) for e in ptypes]
    pz = list(zip(df.columns.to_list(), stypes))
    return ",\n".join([f"{' '*indent}{e[0]} {e[1]}" for e in pz])

