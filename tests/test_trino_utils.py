import math
from datetime import datetime
from unittest import mock

from osc_ingest_trino import TrinoBatchInsert, attach_trino_engine


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


def test_trino_batch_insert():
    # mock up an sqlalchemy table
    tbl = mock.MagicMock()
    tbl.name = "test"
    # mock up an sqlalchemy Connnection
    cxn = mock.MagicMock()
    # tuple data, in form supplied to __call__ as specified in 'method' param docs:
    # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
    rows = [("a", 4.5), ("b'c", math.nan), (None, math.inf), ("d", -math.inf), ("e", datetime(2022, 1, 1))]
    # invoke the __call__ method, simulating df.to_sql call
    tbi = TrinoBatchInsert(catalog="test", schema="test", batch_size=2, verbose=True)
    tbi(tbl, cxn, [], rows)

    assert cxn.execute.call_count == 3
    xcalls = cxn.execute.call_args_list
    assert xcalls[0].args[0].text == "insert into test.test.test values\n('a', 4.5),\n('b''c', nan())"
    assert xcalls[1].args[0].text == "insert into test.test.test values\n(NULL, infinity()),\n('d', -infinity())"
    assert xcalls[2].args[0].text == "insert into test.test.test values\n('e', TIMESTAMP '2022-01-01 00:00:00')"
