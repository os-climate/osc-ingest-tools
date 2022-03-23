import mock

from osc_ingest_trino import attach_trino_engine


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
