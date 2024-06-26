> [!IMPORTANT]
> On June 26 2024, Linux Foundation announced the merger of its financial services umbrella, the Fintech Open Source Foundation ([FINOS](https://finos.org)), with OS-Climate, an open source community dedicated to building data technologies, modeling, and analytic tools that will drive global capital flows into climate change mitigation and resilience; OS-Climate projects are in the process of transitioning to the [FINOS governance framework](https://community.finos.org/docs/governance); read more on [finos.org/press/finos-join-forces-os-open-source-climate-sustainability-esg](https://finos.org/press/finos-join-forces-os-open-source-climate-sustainability-esg)

# osc-ingest-tools

python tools to assist with standardized data ingestion workflows

## Installation, Usage, and Release Management

### Install from PyPi

```console
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
<https://github.com/os-climate/osc-ingest-tools>.

All changes must pass the automated test suite, along with various static
checks.

[Black](https://black.readthedocs.io/) code style and
[isort](https://pycqa.github.io/isort/) import ordering are enforced.

Enabling automatic formatting via [pre-commit](https://pre-commit.com/) is
recommended:

```console
pip install black isort pre-commit
pre-commit install
```

To ensure compliance with static check tools, developers may wish to run;

```console
pip install black isort
# auto-sort imports
isort .
# auto-format code
black .
```

Code can then be tested using tox:

```console
=======
# run static checks and tests
tox
# run only tests
tox -e py3
# run only static checks
tox -e static
# run tests and produce a code coverage report
tox -e cov
```

### Releasing

To release a new version of this library, authorized developers should;

- Prepare a signed release commit updating `version` in setup.py
- Tag the commit using [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
  prepended with "v"
- Push the tag

E.g.,

```console
git commit -sm "Release v0.3.4"
git tag v0.3.4
git push --follow-tags
```

A Github workflow will then automatically release the version to PyPI.
