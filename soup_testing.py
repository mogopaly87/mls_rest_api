# import pandas as pd
# from bs4 import BeautifulSoup
# from datetime import datetime
# import asyncio
# import aiohttp
# import json


from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

driver = webdriver.Chrome()
options = webdriver.ChromeOptions()
options.add_argument('headless')
driver.get("https://www.remax.ca/nl?pageNumber=1")
elem = driver.find_element(By.CLASS_NAME, "gallery-results-count_count__lt1pw")
word = elem.text
num_of_listings = int(elem.text.split(" ")[0].replace(",", ""))


# HEADERS = ({'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
#             'Accept-Language': 'en-US, en;q=0.5'})

# async def get_all_pages():
#     async with aiohttp.ClientSession() as session:
#         first_page = "https://www.remax.ca/nl/?pageNumber=1"
#         async with session.get(first_page, headers = HEADERS) as resp:
#             body_of_first_page = await resp.text()
#             soup_fo_first_page = BeautifulSoup(body_of_first_page, "html.parser")
#             links = soup_fo_first_page.find("span", attrs = {'class':'gallery-results-count_count__lt1pw'})
#             # for link in links:
#                     # current_href = link.get("href")
#             print(soup_fo_first_page.prettify())
                    

# async def main():
#     await asyncio.gather(get_all_pages())
    
# if __name__ == '__main__':
#     asyncio.run(main())