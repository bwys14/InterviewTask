from logging import getLogger, Logger

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame, SparkSession
from pytest import fixture


@fixture
def session() -> SparkSession:
    test_session: SparkSession = SparkSession.builder.master('local').appName('RunTests').getOrCreate()
    return test_session


@fixture
def logg() -> Logger:
    return getLogger('log_steps')


@fixture
def input_schema() -> list:
    return ['id', 'city', 'year']


@fixture
def input_dataset() -> list:
    return [
        (1, 'Warsaw', 1990),
        (2, 'Gdansk', 1990),
        (3, 'Gdansk', 1991),
        (4, 'Krakow', 1993),
        (5, 'Warsaw', 1996),
        (6, 'Warsaw', 1996),
        (7, 'Warsaw', 1996),
        (8, 'Gdansk', 2004),
        (9, 'Wroclaw', 2005),
        (10, 'Gdansk', 2007),
        (11, 'Krakow', 2007),
        (12, 'Warsaw', 2009),
        (13, 'Warsaw', 2011),
        (14, 'Warsaw', 2011),
        (15, 'Gdansk', 2017)
    ]


@fixture
def create_df(session: SparkSession, input_schema: list, input_dataset: list) -> DataFrame:
    return session.createDataFrame(data=input_dataset, schema=input_schema)
