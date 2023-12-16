# from urllib import response
# from util import download_main_data, execute_initial_data_ingestion
# from transform_data_to_df import transform
# from dotenv import load_dotenv
# import os
# from sqlalchemy import create_engine
# from scd import add_new_or_update_listing_status
# import requests
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from get_all_listing_urls import download_all_urls_to_text_file
from extract_data_from_urls import download_data_for_each_url

# download_all_urls_to_text_file() 
download_data_for_each_url("test_file.txt")




# load_dotenv(dotenv_path='.env')
# SQL_ALCHEMY_CONN_STRING = os.getenv('SQL_ALCHEMY_CONN_STRING')
# import transform_data_to_df as trf

# df = trf.transform('mls_temp.json')
# var = [1128684, 1249295, 1251754]
# print(df.query("mls_num == [1128684, 1249295, 1251754]"))
# # print(df.head())


# # print(util.is_mls_num_data_unchanged())

# download_main_data()
# execute_initial_data_ingestion()
# util.load_transformed_data_to_sql_table()

# add_new_or_update_listing_status()