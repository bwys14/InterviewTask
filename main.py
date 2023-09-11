from utils.business_logic import df_filter, df_rename_columns
from utils.tech_utils import parse_args, read_df, save_output, init_logg
from pyspark.sql import SparkSession
if __name__ == "__main__":
    spark_sess = SparkSession.builder.master("local").appName("KommatiParaApp").getOrCreate()

    args = parse_args()

    logg = init_logg()

    logg.info(f'Reading finance dataset from {args.finance_data_path}')
    finance_df = read_df(args.finance_data_path, spark_sess)
    logg.info(f'Reading customer dataset from {args.customer_data_path}')
    customers_df = read_df(args.customers_data_path, spark_sess)

    logg.info('Dropping personal info from customers data')
    customers_df.drop('firstname', 'lastname')
    logg.info('Dropping personal info from finance data')
    finance_df.drop('cc_n')

    logg.info('Joining customer and finance data')
    joined_data = customers_df.join(finance_df, 'id', 'inner')

    logg.info('Filtering joined data')
    joined_data = df_filter(joined_data, args.cust_filter_dict)

    logg.info('Renaming columns')
    output = df_rename_columns(joined_data, args.rename_dict)

    save_output(output)

    logg.info("Output saved")
