from argparse import ArgumentParser, Namespace
from config.definitions import ROOT_DIR
import logging
from logging.handlers import RotatingFileHandler
from os.path import join as path_join
from pyspark.sql import DataFrame, SparkSession
from time import strftime


def read_df(dataset_path: str, spark_session: SparkSession, log: logging.Logger) -> DataFrame:
    """
    This function reads csv dataset from provided path using provided spark_session instance
    :param dataset_path: str, the path from which csv should be read
    :param spark_session: SparkSession, determines spark session to use for reading the data
    :param log: Logger, provide Logger object to be used for steps logging
    :return: DataFrame, containing data included in csv stated as source
    """
    log.info(f'Reading dataset from path {dataset_path}')
    return spark_session.read.format('csv').option('header', 'true').load(dataset_path)


def save_output(input_df: DataFrame, log: logging.Logger) -> None:
    """
    This function saves output DataFrame as csv file in client_data directory under project root
    :param input_df: DataFrame, dataframe to be saved as csv
    :param log: Logger, provide Logger object to be used for steps logging
    """
    log.info('Creating output path')
    output_path = path_join(ROOT_DIR, 'client_data', 'output' + strftime("%Y%m%d_%H%M%S"))
    log.info(f'Saving output to {output_path}')
    input_df.write.option('header', 'true').format('csv').mode('overwrite').save(output_path)


def parse_args(log: logging.Logger) -> Namespace:
    """
    The function parses the input arguments
    log.info(f'Creating output path')
    :return: Namespace, argparse object with parsed arguments
    """

    log.info('Parsing arguments')
    parser = ArgumentParser()
    parser.add_argument('-cds', '--customers_data_path', type=str, required=True,
                        help='Path to file with customers data')
    parser.add_argument('-fds', '--finance_data_path', type=str, required=True, help='Path to file with finance data')
    parser.add_argument('-rdict', '--rename_dict', type=str, required=False,
                        default='{"id": "client_identifier", "btc_a": "bitcoin_address", "cc_t": "credit_card_type"}',
                        help='Provides old column name - new column name mapping')
    parser.add_argument('-fdict', '--filter_dict', type=str, required=False,
                        default='{"country": ["Netherlands", "United Kingdom"]}',
                        help='Provides column-filtering values mapping')
    parsed_args = parser.parse_args()
    return parsed_args


def init_logg(log_path: str = 'logs/applog.log', log_size: int = 3000,
                       log_format: str = '%(asctime)s - %(levelname)s - %(message)s',
                       log_time_format='%Y-%m-%d %H:%M:%S') -> logging.Logger:
    """
    This function generates logger object to be used for processing control and debug
    :param log_path: str, the path (up to filename) where log will be stored
    :param log_size: int, maximum number of bytes stored in a log
    :param log_format: str, the general  format of log entry
    :param log_time_format: str, the format of timestamp in log entry
    :return: Logger, object used for logging the processing steps
    """
    logger = logging.getLogger('log_steps')

    logger.setLevel(logging.INFO)

    handler = RotatingFileHandler(log_path, maxBytes=log_size, backupCount=10)
    handler.setLevel(logging.INFO)

    formatter = logging.Formatter(log_format, log_time_format)

    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger
