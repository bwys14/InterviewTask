from logging import getLogger

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

log = getLogger('log_steps')


def df_filter(input_df: DataFrame, filter_dict: dict) -> DataFrame:
    """
    The function filters input dataframe as per column_name-values pair determined by input dictionary
    :param DataFrame input_df: the data frame to be filtered
    :param dict filter_dict: provides column name (dict keys) and values to be used for filtering (dict values)
    :return: DataFrame, filtered as per information in dict
    """
    log.info(f'Start dataset filtering')
    for f_col, f_val in filter_dict.items():
        if f_col not in input_df.columns:
            log.warning(f'No column named {f_col} was detected')
        else:
            log.info(f'Filter {f_col} column using {f_val} values')
            input_df = input_df.filter(col(f_col).isin(f_val))
    return input_df


def df_rename_columns(input_df: DataFrame, columns_aliases_dict: dict) -> DataFrame:
    """
    The function renames columns of input dataframe using old_name-new_name pair defined by input dictionary
    :param DataFrame input_df: the dataframe which columns need to be renamed
    :param dict columns_aliases_dict: defines old column name (dict keys) and new name (dict values)
    :return: DataFrame, dataframe with renamed columns
    """
    for old_name, new_name in columns_aliases_dict.items():
        if old_name not in input_df.columns:
            log.warning(f'No column named {old_name} was detected')
        else:
            log.info(f'Renaming column {old_name} to {new_name}')
            input_df = input_df.withColumnRenamed(old_name, new_name)
    return input_df
