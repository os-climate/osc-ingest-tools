"""Trino interoperability functions."""

import math
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
import sqlalchemy
import trino
from mypy_boto3_s3.service_resource import Bucket
from sqlalchemy import Connection, Engine, Row, Table, create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text

import osc_ingest_trino as osc
import osc_ingest_trino.unmanaged as oscu

# from sqlalchemy.sql.schema import Table as Table


__all__ = [
    "attach_trino_engine",
    "fast_pandas_ingest_via_hive",
    "TrinoBatchInsert",
    "_do_sql",
]


def attach_trino_engine(
    env_var_prefix: str = "TRINO",
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    verbose: Optional[bool] = False,
) -> Engine:
    """Return a SQLAlchemy engine object representing a Trino instance.

    env_var_prefix -- a prefix for all environment variables related to the Trino instance.
    catalog -- the Trino catalog.
    schema -- the Trino schema.
    verbose -- if True, print the full string used to connect.
    """
    sqlstring = "trino://{user}@{host}:{port}".format(
        user=os.environ[f"{env_var_prefix}_USER"],
        host=os.environ[f"{env_var_prefix}_HOST"],
        port=os.environ[f"{env_var_prefix}_PORT"],
    )
    if catalog is not None:
        sqlstring += f"/{catalog}"
    if schema is not None:
        if catalog is None:
            raise ValueError("connection schema specified without a catalog")
        sqlstring += f"/{schema}"

    sqlargs = {"auth": trino.auth.JWTAuthentication(os.environ[f"{env_var_prefix}_PASSWD"]), "http_scheme": "https"}

    if verbose:
        print(f"using connect string: {sqlstring}")

    engine = create_engine(sqlstring, connect_args=sqlargs)
    engine.connect()
    return engine


def _do_sql(
    sql: Union[sqlalchemy.sql.elements.TextClause, str], engine: Engine, verbose: bool = False
) -> Optional[Sequence[Row[Any]]]:
    """Execute SQL query, returning the query result.

    sql -- the SQL query.
    engine -- the SQLAlchemy engine representing the Trino database.
    verbose -- if True, print the values returned from executing the string.
    """
    if type(sql) is not sqlalchemy.sql.elements.TextClause:
        sql = text(str(sql))
    if verbose:
        print(sql)
    with engine.begin() as cxn:
        qres = cxn.execute(sql)
    res = None
    if qres.returns_rows:
        res = qres.fetchall()
        if verbose:
            print(res)
    return res


def fast_pandas_ingest_via_hive(  # noqa: C901
    df: pd.DataFrame,
    engine: Engine,
    catalog: str,
    schema: str,
    table: str,
    hive_bucket: Bucket,
    hive_catalog: str,
    hive_schema: str,
    partition_columns: List[str] = [],
    overwrite: bool = False,
    typemap: Dict[str, str] = {},
    colmap: Dict[str, str] = {},
    verbose: bool = False,
) -> None:
    """Efficiently export a dataframe into a Trino database.

    df -- the dataframe to export.
    engine -- the SQLAlchemy engine representing the Trino database.
    catalog -- the Trino catalog.
    schema -- the Trino schema.
    table -- the name of the table created in the schema.
    hive_bucket -- the backing store of the Hive metastore.
    hive_catalog -- the Hive metastore catalog (where schemas are created).
    hive_schema -- the Hive metastore schema (where tables will be created).
    partition_columns -- if not empty, defines the partition columns of the table created.
    overwrite -- if True, an existing table will be overwritten.
    typemap -- used to format types that cannot otherwise be properly inferred.
    colmap -- used to format column names that cannot otherwise be properly inferred.
    verbose -- if True, print the queries being executed and the results of those queries.
    """
    uh8 = uuid.uuid4().hex[:8]
    hive_table = f"ingest_temp_{uh8}"

    dfw = df
    if len(partition_columns) > 0:
        if verbose:
            print("enforcing dataframe partition column order")
        dfw = osc.enforce_partition_column_order(dfw, partition_columns)

    # do this after any changes to DF column orderings above
    columnschema = osc.create_table_schema_pairs(dfw, typemap=typemap, colmap=colmap)

    # verify destination table first, to fail early and avoid creation of hive tables
    fq_tablename = ".".join([catalog, schema, table][(catalog is None) :])
    if verbose:
        print(f"\nverifying existence of table {fq_tablename}")
    tabledef = f"create table if not exists {fq_tablename} (\n"
    tabledef += f"{columnschema}\n"
    tabledef += ") with (\n    format = 'parquet'"
    if len(partition_columns) > 0:
        tabledef += ",\n"
        tabledef += f"    partitioning = array{partition_columns}"
    tabledef += "\n)"
    _do_sql(tabledef, engine, verbose=verbose)

    if verbose:
        print(f"\nstaging dataframe parquet to s3 {hive_bucket.name}")
    oscu.ingest_unmanaged_parquet(
        dfw, hive_schema, hive_table, hive_bucket, partition_columns=partition_columns, verbose=verbose
    )

    try:
        if verbose:
            print(f"\ndeclaring intermediate hive table {hive_catalog}.{hive_schema}.{hive_table}")
        tabledef = f"create table if not exists {hive_catalog}.{hive_schema}.{hive_table} (\n"
        tabledef += f"{columnschema}\n"
        tabledef += ") with (\n    format = 'parquet',\n"
        if len(partition_columns) > 0:
            tabledef += f"    partitioned_by = array{partition_columns},\n"
        tabledef += f"    external_location = 's3a://{hive_bucket.name}/trino/{hive_schema}/{hive_table}/'\n)"
        _do_sql(tabledef, engine, verbose=verbose)

        if verbose:
            print("\nsyncing partition metadata on intermediate hive table")
        if len(partition_columns) > 0:
            sql = text(f"call {hive_catalog}.system.sync_partition_metadata('{hive_schema}', '{hive_table}', 'FULL')")
            _do_sql(sql, engine, verbose=verbose)

        if overwrite:
            if verbose:
                print(f"\noverwriting data in {fq_tablename}")
            sql = text(f"delete from {fq_tablename}")
            _do_sql(sql, engine, verbose=verbose)

        if verbose:
            print(f"\ntransferring data: {hive_catalog}.{hive_schema}.{hive_table} -> {fq_tablename}")
        sql = text(f"insert into {fq_tablename}\nselect * from {hive_catalog}.{hive_schema}.{hive_table}")
        _do_sql(sql, engine, verbose=verbose)

        if verbose:
            print(f"\ndeleting table and data for intermediate table {hive_catalog}.{hive_schema}.{hive_table}")
        oscu.drop_unmanaged_table(hive_catalog, hive_schema, hive_table, engine, hive_bucket, verbose=verbose)
    except SQLAlchemyError:
        # Clean up table that will otherwise be orphaned
        if verbose:
            print(f"\ndeleting table and data for intermediate table {hive_catalog}.{hive_schema}.{hive_table}")
        oscu.drop_unmanaged_table(hive_catalog, hive_schema, hive_table, engine, hive_bucket, verbose=verbose)
        raise


class TrinoBatchInsert(object):
    """A class used to bundle together basic Trino parameters."""

    def __init__(
        self,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        batch_size: int = 1000,
        optimize: bool = False,
        verbose: bool = False,
    ):
        """Initialize TrinoBatchInsert objects."""
        self.catalog = catalog
        self.schema = schema
        self.batch_size = batch_size
        self.optimize = optimize
        self.verbose = verbose

    # conforms to signature expected by pandas 'callable' value for method kw arg
    # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
    # https://pandas.pydata.org/docs/user_guide/io.html#io-sql-method
    def __call__(self, sqltbl: Table, dbcxn: Connection, columns: List[str], data_iter: List[Tuple]) -> None:
        """Implement `callable` interface for row-by-row insertion."""
        fqname = self._full_table_name(sqltbl)
        batch: List[str] = []
        self.ninserts = 0
        for r in data_iter:
            # each row of data_iter is a python tuple
            # I cannot currently make good use of sqlalchemy ':params'
            # and so I have to do my own "manual" value formatting for insertions
            row = ", ".join([TrinoBatchInsert._sqlform(e) for e in r])
            row = f"({row})"
            batch.append(row)
            # possible alternative: dispatch batches by total batch size in bytes
            if len(batch) >= self.batch_size:
                self._do_insert(dbcxn, fqname, batch)
                batch = []
        if len(batch) > 0:
            self._do_insert(dbcxn, fqname, batch)
        if self.optimize:
            if self.verbose:
                print("optimizing table files")
            sql = text(f"alter table {fqname} execute optimize")
            qres = dbcxn.execute(sql)
            x = qres.fetchall()
            if self.verbose:
                print(f"execute optimize: {x}")

    def _do_insert(self, dbcxn: Connection, fqname: str, batch: List[str]) -> None:
        """Implement actual row-by-row insertion of BATCH data into table FQNAME using DBCXN database connection."""
        if self.verbose:
            print(f"inserting {len(batch)} records")
            TrinoBatchInsert._print_batch(batch)
        # trino is not currently supporting sqlalchemy cursor.executemany()
        # and so I am generating an insert command with no ':params' that
        # includes all batch data as literal sql values
        valclause = ",\n".join(batch)
        # injecting raw sql strings is deprecated and will be illegal in sqlalchemy 2.x
        # using text() is the correct way:
        sql = text(f"insert into {fqname} values\n{valclause}")
        # if self.verbose: print(f'{sql}')
        qres = dbcxn.execute(sql)
        x = qres.fetchall()
        if self.verbose:
            print(f"batch insert result: {x}")

    def _full_table_name(self, sqltbl: Table) -> str:
        """Return fully qualified table name for SQLTBL table within this TrinoBatchInsert object."""
        # start with table name
        name: str = f"{sqltbl.name}"
        # prepend schema - allow override from this class
        name = f"{self.schema or sqltbl.schema}.{name}"
        # prepend catalog, if provided
        if self.catalog is not None:
            name = f"{self.catalog}.{name}"
        if self.verbose:
            print(f'constructed fully qualified table name as: "{name}"')
        return name

    @staticmethod
    def _sqlform(x: Any) -> str:
        """Format the value of x so it can appear in a SQL Values context."""
        if x is None:
            return "NULL"
        if isinstance(x, str):
            # escape any single quotes in the string
            t = x.replace("'", "''")
            # colons are mostly a problem for ':some_id_name', which is interpreted as
            # a parameter requiring binding, but just escaping them all works
            t = t.replace(":", "\\:")
            # enclose string with single quotes
            return f"'{t}'"
        if isinstance(x, datetime):
            return f"TIMESTAMP '{x}'"
        if isinstance(x, float):
            if math.isnan(x):
                return "nan()"
            if math.isinf(x):
                if x < 0:
                    return "-infinity()"
                return "infinity()"
        return str(x)

    @staticmethod
    def _print_batch(batch: List[str]) -> None:
        """For batch, a list of SQL query lines, print up to the first 5 such."""
        if len(batch) > 5:
            print("\n".join(f"  {e}" for e in batch[:3]))
            print("  ...")
            print(f"  {batch[-1]}")
        else:
            print("\n".join(f"  {e}" for e in batch))
