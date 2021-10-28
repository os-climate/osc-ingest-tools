import re
import pandas as pd

_wsdedup = re.compile(r"\s+")
_usdedup = re.compile(r"__+")
_rmpunc = re.compile(r"[,.()&$/+-]+")

# 63 seems to be a common max column name length
def valid_sql_column_name(name, maxlen=63):
    if isinstance(name, list):
        return [valid_sql_column_name(e) for e in name]
    w = str(name).casefold().rstrip().lstrip()
    w = w.replace("-", "_")
    w = _rmpunc.sub("", w)
    w = _wsdedup.sub("_", w)
    w = _usdedup.sub("_", w)
    w = w.replace("average", "avg")
    w = w.replace("maximum", "max")
    w = w.replace("minimum", "min")
    w = w.replace("absolute", "abs")
    w = w.replace("source", "src")
    w = w.replace("distribution", "dist")
    # these are common in the sample names but unsure of standard abbv
    #w = w.replace("inference", "inf")
    #w = w.replace("emissions", "emis")
    #w = w.replace("intensity", "int")
    #w = w.replace("reported", "rep")
    #w = w.replace("revenue", "rev")
    w = w[:maxlen] 
    return w

def valid_sql_pandas_columns(df, inplace=False, maxlen=63):
    if not isinstance(df, pd.DataFrame):
        raise ValueError("df must be a pandas DataFrame")
    icols = df.columns.to_list()
    ocols = valid_sql_column_name(icols, maxlen=maxlen)
    if (len(set(ocols)) < len(ocols)):
        raise ValueError("remapped column names were not unique!")
    rename_map = dict(list(zip(icols, ocols))))
    return df.rename(columns=rename_map, inplace=inplace)

