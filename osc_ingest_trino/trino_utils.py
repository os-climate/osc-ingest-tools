import os

import trino
from sqlalchemy.engine import create_engine

__all__ = [
    "attach_trino_engine",
]

def attach_trino_engine(env_var_prefix = 'TRINO', catalog = None, schema = None, verbose = False):
    sqlstring = 'trino://{user}@{host}:{port}'.format(
        user = os.environ[f'{env_var_prefix}_USER'],
        host = os.environ[f'{env_var_prefix}_HOST'],
        port = os.environ[f'{env_var_prefix}_PORT']
    )
    if catalog is not None:
        sqlstring += f'/{catalog}'
    if schema is not None:
        if catalog is None:
            raise ValueError(f'connection schema specified without a catalog')
        sqlstring += f'/{schema}'

    sqlargs = {
        'auth': trino.auth.JWTAuthentication(os.environ[f'{env_var_prefix}_PASSWD']),
        'http_scheme': 'https'
    }

    if verbose: print(f'using connect string: {sqlstring}')

    engine = create_engine(sqlstring, connect_args = sqlargs)
    connection = engine.connect()
    return engine

