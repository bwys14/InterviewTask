
from utils.business_logic import df_filter, df_rename_columns
from utils.tech_utils import parse_args, read_df, save_output
from pyspark.sql import SparkSession, DataFrame
if __name__ == "__main__":
    spark_sess = SparkSession.builder.master("local").appName("KommatiParaApp").getOrCreate()

    args = parse_args()

    finance_df = read_df(args.finance_data_path, spark_sess)
    customers_df = read_df(args.customers_data_path, spark_sess)

    customers_df.drop('firstname','lastname')
    finance_df.drop('cc_n')

    joined_data = customers_df.join(finance_df, 'id','inner')

    joined_data = df_filter(joined_data, args.cust_filter_dict)

    output = df_rename_columns(joined_data, args.rename_dict)

    save_output(output)