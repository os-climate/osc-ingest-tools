import math
from datetime import datetime
from unittest import mock
from unittest.mock import patch

import boto3
import botocore
import moto
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from osc_ingest_trino import (
    TrinoBatchInsert,
    attach_trino_engine,
    pandas_type_to_sql,
    sql_compliant_name,
)
from osc_ingest_trino.unmanaged import unmanaged_parquet_tabledef

# from os_bucket import OSBucket


class MyModel:
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def save(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.put_object(Bucket="mybucket", Key=self.name, Body=self.value)


def test_sql_compliant_name():
    foo_name = sql_compliant_name("foo")
    assert foo_name == "foo"


def test_pandas_type_to_sql():
    integer_type = pandas_type_to_sql("int32")
    assert integer_type == "integer"


@mock.patch("osc_ingest_trino.trino_utils.trino.auth.JWTAuthentication")
@mock.patch("osc_ingest_trino.trino_utils.create_engine")
def test_attach_trino_engine(mock_engine, mock_trino_auth, monkeypatch):
    monkeypatch.setenv("TEST_USER", "tester")
    monkeypatch.setenv("TEST_PASSWD", "supersecret123")
    monkeypatch.setenv("TEST_HOST", "example")
    monkeypatch.setenv("TEST_PORT", "8000")

    fake_engine = mock.MagicMock()
    fake_engine.connect.return_value = None
    mock_engine.return_value = fake_engine
    mock_trino_auth.return_value = "yep"

    attach_trino_engine(env_var_prefix="TEST", catalog="ex_catalog", schema="ex_schema", verbose=True)

    mock_engine.assert_called_with(
        "trino://tester@example:8000/ex_catalog/ex_schema", connect_args={"auth": "yep", "http_scheme": "https"}
    )


# from os_bucket import OSBucket


def test_trino_batch_insert():
    # mock up an sqlalchemy table
    tbl = mock.MagicMock()
    tbl.name = "test"
    # mock up an sqlalchemy Connnection
    cxn = mock.MagicMock()
    # tuple data, in form supplied to __call__ as specified in 'method' param docs:
    # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
    rows = [("a", 4.5), ("b'c", math.nan), (None, math.inf), ("d", -math.inf), ("e", datetime(2022, 1, 1)), (":f", 1.0)]
    # invoke the __call__ method, simulating df.to_sql call
    tbi = TrinoBatchInsert(catalog="test", schema="test", batch_size=2, verbose=True, optimize=True)
    tbi(tbl, cxn, [], rows)

    assert cxn.execute.call_count == 4
    xcalls = cxn.execute.call_args_list
    assert xcalls[0].args[0].text == "insert into test.test.test values\n('a', 4.5),\n('b''c', nan())"
    assert xcalls[1].args[0].text == "insert into test.test.test values\n(NULL, infinity()),\n('d', -infinity())"
    assert (
        xcalls[2].args[0].text
        == "insert into test.test.test values\n('e', TIMESTAMP '2022-01-01 00:00:00'),\n('\\:f', 1.0)"
    )
    assert xcalls[3].args[0].text == "alter table test.test.test execute optimize"


def test_trino_pandas_insert():
    import pandas as pd

    # mock up an sqlalchemy table
    tbl = mock.MagicMock()
    tbl.name = "test_pandas"
    # mock up an sqlalchemy Connnection
    cxn = mock.MagicMock()
    df = pd.DataFrame(
        {"A": [4.5], "B'C": [math.nan], None: [math.inf], "D": [-math.inf], "E": [datetime(2022, 1, 1)], ":F": [1.0]}
    ).convert_dtypes()
    assert (df.dtypes == ["Float64", "Int64", "Float64", "Float64", "datetime64[ns]", "Int64"]).all()
    # This passes Mock test, but fails when used in Trino/Iceberg environment
    df.to_sql(
        tbl.name,
        con=cxn,
        schema="test",
        if_exists="append",
        index=False,
        method=TrinoBatchInsert(batch_size=5, verbose=True),
    )


@mock_aws
def test_unmanaged_parquet_tabledef():
    import pandas as pd

    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).convert_dtypes()

    conn = boto3.resource("s3", region_name="us-east-1")
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    bucket = conn.Bucket("mybucket")
    bucket.create()

    tabledef = unmanaged_parquet_tabledef(df, "catalog", "schema", "table", bucket, partition_columns=["a", "b"])
    print(tabledef)
