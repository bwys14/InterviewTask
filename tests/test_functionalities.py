from logging import Logger

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame, SparkSession

from utils.business_logic import df_filter, df_rename_columns


def test_filter_existing_column(session: SparkSession, input_schema: list, create_df: DataFrame, logg: Logger) -> None:
    expected_data = [
        (2, 'Gdansk', 1990),
        (10, 'Gdansk', 2007),
        (11, 'Krakow', 2007)]
    expected_df = session.createDataFrame(data=expected_data, schema=input_schema)
    flt = {'city': ['Gdansk', 'Krakow'], 'year': ['1990', 2007, 2011, '1996']}
    filtered_df = df_filter(create_df, flt, logg)
    assert_df_equality(expected_df, filtered_df, ignore_row_order=True)


def test_filter_invalid_column(session: SparkSession, input_schema: list, create_df: DataFrame, logg: Logger) -> None:
    expected_data = [
        (1, 'Warsaw', 1990),
        (2, 'Gdansk', 1990)]
    expected_df = session.createDataFrame(data=expected_data, schema=input_schema)
    flt = {'department': ['it', 'hr'], 'year': 1990}
    filtered_df = df_filter(create_df, flt, logg)
    assert_df_equality(expected_df, filtered_df, ignore_row_order=True)


def test_empty_filter(session: SparkSession, create_df: DataFrame, logg: Logger) -> None:
    filtered_df = df_filter(create_df, {}, logg)
    assert_df_equality(create_df, filtered_df, ignore_row_order=True)


def test_rename_column(session: SparkSession, input_dataset: list, create_df: DataFrame, logg: Logger) -> None:
    expected_schema = ['no', 'location', 'AD']
    rdict = {'city': 'location', 'director': 'supervisor', 'id': 'no', 'year': 'AD'}
    expected_df = session.createDataFrame(data=input_dataset, schema=expected_schema)
    renamed_df = df_rename_columns(create_df, rdict, logg)
    assert_df_equality(expected_df, renamed_df, ignore_row_order=True)


def test_empty_rename_input(session: SparkSession, create_df: DataFrame, logg: Logger) -> None:
    renamed_df = df_rename_columns(create_df, {}, logg)
    assert_df_equality(create_df, renamed_df, ignore_row_order=True)
