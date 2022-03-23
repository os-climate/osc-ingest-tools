# osc-ingest-tools
python tools to assist with standardized data ingestion workflows

### Install from PyPi

```
pip install osc-ingest-tools
```

### Examples

```python
>>> from osc_ingest_trino import *

>>> import pandas as pd

>>> data = [['tom', 10], ['nick', 15], ['juli', 14]]

>>> df = pd.DataFrame(data, columns = ['First Name', 'Age In Years']).convert_dtypes()

>>> df
  First Name  Age In Years
0        tom            10
1       nick            15
2       juli            14

>>> enforce_sql_column_names(df)
  first_name  age_in_years
0        tom            10
1       nick            15
2       juli            14

>>> enforce_sql_column_names(df, inplace=True)

>>> df
  first_name  age_in_years
0        tom            10
1       nick            15
2       juli            14

>>> df.info(verbose=True)
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 3 entries, 0 to 2
Data columns (total 2 columns):
 #   Column        Non-Null Count  Dtype 
---  ------        --------------  ----- 
 0   first_name    3 non-null      string
 1   age_in_years  3 non-null      Int64 
dtypes: Int64(1), string(1)
memory usage: 179.0 bytes

>>> p = create_table_schema_pairs(df)

>>> print(p)
    first_name varchar,
    age_in_years bigint

>>> 
```

#### Adding custom type mappings to `create_table_schema_pairs`
```python
>>> df = pd.DataFrame(data, columns = ['First Name', 'Age In Years'])

>>> enforce_sql_column_names(df, inplace=True)

>>> df.info(verbose=True)
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 3 entries, 0 to 2
Data columns (total 2 columns):
 #   Column        Non-Null Count  Dtype 
---  ------        --------------  ----- 
 0   first_name    3 non-null      object
 1   age_in_years  3 non-null      int64 
dtypes: int64(1), object(1)
memory usage: 176.0+ bytes

>>> p = create_table_schema_pairs(df, typemap={'object':'varchar'})

>>> print(p)
    first_name varchar,
    age_in_years bigint

>>>
```

### Development

Patches may be contributed via pull requests to
https://github.com/os-climate/osc-ingest-tools.

All changes must pass the automated test suite, along with various static
checks.

[Black](https://black.readthedocs.io/) code style and
[isort](https://pycqa.github.io/isort/) import ordering are enforced.

Enabling automatic formatting via [pre-commit](https://pre-commit.com/) is
recommended:
```
pip install black isort pre-commit
pre-commit install
```

To ensure compliance with static check tools, developers may wish to run;
```
pip install black isort
# auto-sort imports
isort .
# auto-format code
black .
```

Code can then be tested using tox.
```
# run static checks and tests
tox
# run only tests
tox -e py3
# run only static checks
tox -e static
# run tests and produce a code coverage report
tox -e cov
```

### build and upload a new release

- update all occurrences of `__version__`
- `python3 setup.py clean`
- `python3 setup.py sdist`
- `twine check dist/*`
- `twine upload dist/*`
- push latest to repo
- create new release on github

upload test or release candidate:
- twine upload --repository-url https://test.pypi.org/legacy/ dist/*

### python packaging resources

- https://packaging.python.org/
- https://packaging.python.org/tutorials/packaging-projects/
- https://realpython.com/pypi-publish-python-package/
