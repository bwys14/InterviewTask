from argparse import ArgumentParser, Namespace
from config.definitions import ROOT_DIR
from pyspark.sql import DataFrame, SparkSession
from time import strftime
import logging
from logging.handlers import RotatingFileHandler
from os.path import join as path_join


def read_df(dataset_path: str, spark_session: SparkSession) -> DataFrame:
    """
    This function reads csv dataset from provided path using provided spark_session instance
    :param dataset_path: str, the path from which csv should be read
    :param spark_session: SparkSession, determines spark session to use for reading the data
    :return: DataFrame, containing data included in csv stated as source
    """
    return spark_session.read.format('csv').option('header', 'true').load(dataset_path)


def save_output(input_df: DataFrame):
    """
    This function saves output DataFrame as csv file in client_data directory under project root
    :param input_df: DataFrame, dataframe to be saved as csv
    """
    output_path = path_join(ROOT_DIR, 'client_data', 'output' + strftime("%Y%m%d_%H%M%S"))
    print(output_path)
    input_df.write.option('header', 'true').format('csv').mode('overwrite').save(output_path)


def parse_args() -> Namespace:
    """
    The function parses the input arguments
    :return: Namespace, argparse object with parsed arguments
    """
    parser = ArgumentParser()
    parser.add_argument('-cds', '--customers_data_path', type=str, required=True,
                        help='Path to file with customers data')
    parser.add_argument('-fds', '--finance_data_path', type=str, required=True, help='Path to file with finance data')
    parser.add_argument('-rdict', '--rename_dict', type=dict, required=False,
                        default={'id': 'client_identifier', 'btc_a': 'bitcoin_address', 'cc_t': 'credit_card_type'},
                        help='Provides old column name - new column name mapping')
    parser.add_argument('-cdict', '--cust_filter_dict', type=dict, required=False,
                        default={'country': ['Netherlands', 'United Kingdom']},
                        help='Provides column-filtering values mapping')
    parsed_args = parser.parse_args()
    return parsed_args


def init_logg(log_path='logs/applog.log', log_size=2500,
                       log_format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                       log_time_format='%Y-%m-%d %H:%M:%S') -> RotatingFileHandler:
    logger = logging.getLogger('log_operations')

    logger.setLevel(logging.INFO)

    handler = RotatingFileHandler(log_path, maxBytes=log_size, backupCount=10)
    handler.setLevel(logging.INFO)

    formatter = logging.Formatter(log_format, log_time_format)

    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger
