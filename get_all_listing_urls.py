
import ssl
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import asyncio
import aiohttp
import json

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from datetime import datetime


HEADERS = ({'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Accept-Language': 'en-US, en;q=0.5'})

def get_num_of_pages():
    page = "https://www.remax.ca/ab/calgary-real-estate?pageNumber=1&view=gallery"
        
    driver = webdriver.Chrome()
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    driver.get(page)
    elem = driver.find_element(By.CLASS_NAME, "page-control_buttonRowContainer__wfw6_")
    word = elem.text
    num_of_pages = int(word.split("\n")[-1])
    
    return num_of_pages


# Async requests
async def get_a_tags_per_page():
    list_of_href_to_details_page = []
    
    async with aiohttp.ClientSession() as session:
        print("Starting detailed link extraction....")
        # TODO: Use Beautiful soup to get the number of pages there are and use that number as the range below.
        
            # FIRST_PAGE_URL = f"https://www.remax.ca/{province}/?pageNumber=1"
            # async with session.get(FIRST_PAGE_URL, headers = HEADERS) as resp:
            #     body_of_first_page = await resp.text()
            #     soup_fo_first_page = BeautifulSoup(body_of_first_page, "html.parser")
        num_of_pages = get_num_of_pages()    
        for i in range(1, num_of_pages + 1):
            # For each page, do the following:
            PAGE_URL = f"https://www.remax.ca/ab/calgary-real-estate?pageNumber={i}"
            async with session.get(PAGE_URL, headers = HEADERS) as resp:
                # Get the page convert it to a text format
                body = await resp.text()
                # Use BS4 to parse the text into HTML format that can be read by BS4.
                soup = BeautifulSoup(body, "html.parser")
                # Get all the a-tag links with this specific class name.
                # N/B: This a-tag link is the link to the details page of a specific lisiting
                # 'links_per_page' is a list of all the links to the details page
                links_per_page = soup.find_all("a", attrs = {'class':'listing-card_listingCard__G6M8g'})
                
                # For each a-tag containing link to a details page, get the 'href' (specific link) to that page.
                # Append each 'href' to a list of hrefs for each listing. 
                # A typical href to a details page looks like this:
                # .......https://www.remax.ca/nl/st-john-s-real-estate/33-lynch-place-wp_idm73000004-25241747-lst
                for link in links_per_page:
                    current_href = link.get("href")
                    if current_href in list_of_href_to_details_page:
                        continue
                    else:
                        list_of_href_to_details_page.append(current_href)
                        print(f" >>>>> Adding {current_href} ")
                        with open("test_file.txt", "a", newline='\n') as file:
                            file.write(f"{current_href}\n")
                            
        print("Ended detailed links extraction!")
        print(f"Extracted {len(list_of_href_to_details_page)} links\n")
        print("Started data extraction from details pages...")
        # await ingest_data_from_details_page(list_of_href_to_details_page, destination_file)
        print("\n")     
        print("COMPLETED data extraction from details pages!")
        # print(data)
        

async def main():
    await asyncio.gather(get_a_tags_per_page())


def download_all_urls_to_text_file():
    """_summary_
    """
    start_time = datetime.now()
    asyncio.run(main())
    finish_time = datetime.now()
    print(f"Start Time : {start_time}\nFinish Time : {finish_time}")
    
    