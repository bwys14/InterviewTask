from chispa.dataframe_comparer import assert_df_equality
from logging import Logger, getLogger
from pytest import fixture
from pyspark.sql import SparkSession, DataFrame
from utils.business_logic import df_filter, df_rename_columns


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
def create_df(session, input_schema, input_dataset) -> DataFrame:
    return session.createDataFrame(data=input_dataset, schema=input_schema)


def test_filter_existing_column(session: SparkSession, input_schema: list, create_df: DataFrame, logg: Logger):
    expected_data = [
        (2, 'Gdansk', 1990),
        (10, 'Gdansk', 2007),
        (11, 'Krakow', 2007)]
    expected_df = session.createDataFrame(data=expected_data, schema=input_schema)
    flt = {'city': ['Gdansk', 'Krakow'], 'year': ['1990', '2007', '2011', '1996']}
    filtered_df = df_filter(create_df, flt, logg)
    assert_df_equality(expected_df, filtered_df)


def test_filter_invalid_column(session: SparkSession, input_schema: list, create_df: DataFrame, logg: Logger):
    expected_data = [
        (1, 'Warsaw', 1990),
        (2, 'Gdansk', 1990)]
    expected_df = session.createDataFrame(data=expected_data, schema=input_schema)
    flt = {'department': ['it', 'hr'], 'year': 1990}
    filtered_df = df_filter(expected_df, flt, logg)
    assert_df_equality(expected_df, filtered_df)


def test_rename_column(session: SparkSession, input_dataset: list, create_df: DataFrame, logg: Logger):
    expected_schema = ['no', 'location', 'AD']
    rdict = {'city': 'location', 'director': 'supervisor', 'id': 'no', 'year': 'AD'}
    expected_df = session.createDataFrame(data=input_dataset, schema=expected_schema)
    renamed_df = df_rename_columns(expected_df, rdict, logg)
    assert_df_equality(expected_df, renamed_df)
