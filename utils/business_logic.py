from logging import Logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def df_filter(input_df: DataFrame, filter_dict: dict, log: Logger) -> DataFrame:
    """
    The function filters input dataframe as per column_name-values pair determined by input dictionary
    :param DataFrame input_df: the data frame to be filtered
    :param dict filter_dict: provides column name (dict keys) and values to be used for filtering (dict values)
    :param Logger log: provide Logger object to be used for logging
    :return: DataFrame, filtered as per information in dict
    """
    log.info(f'Start dataset filtering')
    for fc, fv in filter_dict.items():
        log.info(f'Filter {fc} column using {fv} values')
        input_df = input_df.filter(col(fc).isin(fv))
    return input_df


def df_rename_columns(input_df: DataFrame, columns_aliases_dict: dict, log: Logger) -> DataFrame:
    """
    The function renames columns of input dataframe as per column_old_name-column_new_name pair determined by input dictionary
    :param DataFrame input_df: the dataframe which columns need to be renamed
    :param dict columns_aliases_dict: defines old column name (dict keys) and value to be used for renaming (dict values)
    :param Logger log: provide Logger object to be used for logging
    :return: DataFrame, dataframe with renamed columns
    """

    log.info(f'Renaming columns: {columns_aliases_dict}')
    return input_df.select([col(c).alias(columns_aliases_dict.get(c, c)) for c in input_df.columns])
