import os

import trino
from sqlalchemy.engine import create_engine
from sqlalchemy.sql import text

__all__ = [
    "attach_trino_engine",
    "TrinoBatchInsert",
]


def attach_trino_engine(env_var_prefix="TRINO", catalog=None, schema=None, verbose=False):
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


class TrinoBatchInsert(object):
    def __init__(self, catalog=None, schema=None, batch_size=1000, verbose=False):
        self.catalog = catalog
        self.schema = schema
        self.batch_size = batch_size
        self.verbose = verbose

    # conforms to signature expected by pandas 'callable' value for method kw arg
    # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
    # https://pandas.pydata.org/docs/user_guide/io.html#io-sql-method
    def __call__(self, sqltbl, dbcxn, columns, data_iter):
        batch = []
        for r in data_iter:
            # each row of data_iter is a python tuple
            # I cannot currently make good use of sqlalchemy ':params'
            # and so I have to do my own "manual" value formatting for insertions
            row = ", ".join([TrinoBatchInsert._sqlform(e) for e in r])
            row = f"({row})"
            batch.append(row)
            # possible alternative: dispatch batches by total batch size in bytes
            if len(batch) >= self.batch_size:
                self._do_insert(dbcxn, sqltbl, columns, batch)
                batch = []
        if len(batch) > 0:
            self._do_insert(dbcxn, sqltbl, columns, batch)

    def _do_insert(self, dbcxn, sqltbl, columns, batch):
        if self.verbose:
            print(f"inserting {len(batch)} records")
            TrinoBatchInsert._print_batch(batch)
        # trino is not currently supporting sqlalchemy cursor.executemany()
        # and so I am generating an insert command with no ':params' that
        # includes all batch data as literal sql values
        valclause = ",\n".join(batch)
        # injecting raw sql strings is deprecated and will be illegal in sqlalchemy 2.x
        # using text() is the correct way:
        sql = text(f"insert into {self._full_table_name(sqltbl)} values\n{valclause}")
        # if self.verbose: print(f'{sql}')
        qres = dbcxn.execute(sql)
        x = qres.fetchall()
        if self.verbose:
            print(f"batch insert result: {x}")

    def _full_table_name(self, sqltbl):
        # start with table name
        name = f"{sqltbl.name}"
        # prepend schema - allow override from this class
        name = f"{self.schema or sqltbl.schema}.{name}"
        # prepend catalog, if provided
        if self.catalog is not None:
            name = f"{self.catalog}.{name}"
        if self.verbose:
            print(f'constructed fully qualified table name as: "{name}"')
        return name

    @staticmethod
    def _sqlform(x):
        if isinstance(x, str):
            # escape any single quotes in the string
            t = x.replace("'", "''")
            # enclose string with single quotes
            return f"'{t}'"
        return str(x)

    @staticmethod
    def _print_batch(batch):
        if len(batch) > 5:
            print("\n".join(f"  {e}" for e in batch[:3]))
            print("  ...")
            print(f"  {batch[-1]}")
        else:
            print("\n".join(f"  {e}" for e in batch))
