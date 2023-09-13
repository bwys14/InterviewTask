from json import loads
from pyspark.sql import SparkSession
from utils.business_logic import df_filter, df_rename_columns
from utils.tech_utils import parse_args, read_df, save_output, init_logg

if __name__ == "__main__":
    
    logg = init_logg()

    logg.info('Initializing Spark session')
    spark_sess = SparkSession.builder.master("local").appName("KommatiParaApp").getOrCreate()

    args = parse_args(logg)

    finance_df = read_df(args.finance_data_path, spark_sess, logg)
    customers_df = read_df(args.customers_data_path, spark_sess, logg)

    logg.info('Dropping personal info from customers data')
    customers_df = customers_df.drop('first_name', 'last_name')

    logg.info('Dropping personal info from finance data')
    finance_df = finance_df.drop('cc_n')

    logg.info('Joining customer and finance data')
    joined_data = customers_df.join(finance_df, 'id', 'inner')

    joined_data = df_filter(joined_data, loads(args.filter_dict), logg)

    output = df_rename_columns(joined_data, loads(args.rename_dict), logg)

    save_output(output, logg)
