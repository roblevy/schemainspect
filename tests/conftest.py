import pytest
from sqlbag import temporary_database


@pytest.yield_fixture()
def postgres_db():
    with temporary_database(host="localhost") as dburi:
        yield dburi


@pytest.yield_fixture()
def redshift_db():
    with temporary_database(dialect="redshift") as dburi:
        yield dburi
