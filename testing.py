from util import download_main_data, execute_initial_data_ingestion
from transform_data_to_df import transform
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

load_dotenv(dotenv_path='.env')
SQL_ALCHEMY_CONN_STRING = os.getenv('SQL_ALCHEMY_CONN_STRING')
# import transform_data_to_df as trf

# df = trf.transform('mls_temp.json')
# var = [1128684, 1249295, 1251754]
# print(df.query("mls_num == [1128684, 1249295, 1251754]"))
# # print(df.head())


# # print(util.is_mls_num_data_unchanged())

# download_main_data()
execute_initial_data_ingestion()
# util.load_transformed_data_to_sql_table()

