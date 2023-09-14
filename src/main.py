from json import loads

from pyspark.sql import SparkSession

from src.utils import *

if __name__ == "__main__":
    logg = init_logg()

    logg.info('Initializing Spark session')
    spark_sess = SparkSession.builder.master("local").appName("KommatiParaApp").getOrCreate()

    args = parse_args()

    finance_df = read_df(args.finance_data_path, spark_sess)
    customers_df = read_df(args.customers_data_path, spark_sess)

    logg.info('Dropping personal info from customers data')
    customers_df = customers_df.drop('first_name', 'last_name')

    logg.info('Dropping personal info from finance data')
    finance_df = finance_df.drop('cc_n')

    logg.info('Joining customer and finance data')
    joined_data = customers_df.join(finance_df, 'id', 'inner')

    joined_data = df_filter(joined_data, loads(args.filter_dict))

    output = df_rename_columns(joined_data, loads(args.rename_dict))

    save_output(output)
