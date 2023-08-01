
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import asyncio
import aiohttp
import json

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

HEADERS = ({'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US, en;q=0.5'})

provinces = ['nb']
# provinces = ['nl', 'sk', 'on', 'ns', 'ab', 'pe', 'bc', 'mb']
provinces = ['nl', 'sk', 'on', 'ns', 'ab', 'pe', 'bc', 'mb']
dict_of_listing_count_by_province = {}

    
driver = webdriver.Chrome()
options = webdriver.ChromeOptions()
options.add_argument('headless')
driver.get(f"https://www.remax.ca/nl/st-johns-real-estate?pageNumber=1")
elem = driver.find_element(By.CLASS_NAME, "gallery-results-count_count__lt1pw")
word = elem.text
num_of_listings = int(elem.text.split(" ")[0].replace(",", ""))
num_of_pages = round(num_of_listings / 20)
    
    
# driver = webdriver.Chrome()
# options = webdriver.ChromeOptions()
# options.add_argument('headless')
# driver.get(f"https://www.remax.ca/nb/?pageNumber=1")
# elem = driver.find_element(By.CLASS_NAME, "gallery-results-count_count__lt1pw")
# word = elem.text
# num_of_listings = int(elem.text.split(" ")[0].replace(",", ""))
# num_of_pages = round(num_of_listings / 20)
# print(f"There are {num_of_pages} pages")
# Async requests
async def get_a_tags_per_page(destination_file):
    list_of_href_to_details_page = []
    
    async with aiohttp.ClientSession() as session:
        print("Starting detailed link extraction....")
        # TODO: Use Beautiful soup to get the number of pages there are and use that number as the range below.
        
            # FIRST_PAGE_URL = f"https://www.remax.ca/{province}/?pageNumber=1"
            # async with session.get(FIRST_PAGE_URL, headers = HEADERS) as resp:
            #     body_of_first_page = await resp.text()
            #     soup_fo_first_page = BeautifulSoup(body_of_first_page, "html.parser")
            
        for i in range(1, num_of_pages + 1):
            # For each page, do the following:
            PAGE_URL = f"https://www.remax.ca/nl/st-johns-real-estate?pageNumber={i}"
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
        print("Ended detailed links extraction!")
        print(f"Extracted {len(list_of_href_to_details_page)} links\n")
        print("Started data extraction from details pages...")
        await ingest_data_from_details_page(list_of_href_to_details_page, destination_file)
        print("\n")
        print("COMPLETED data extraction from details pages!")
        # print(data)
        
        
async def ingest_data_from_details_page(list_of_href_to_details_page, destination_file):
    listings = []  
    async with aiohttp.ClientSession() as session:
        # For each href that links to a detailed page for a specific lisitng:
        for link in list_of_href_to_details_page:
            print(f"Processing:====> {link}")
            if "https://www.remax.ca/commercial" in link:
                continue
            else:
            # Get the page in a response object
                try:
                    async with session.get(link, headers = HEADERS) as resp:
                        # convert the page to a text format
                        body = await resp.text()
                        # Use BS4 to parse the text into HTML format that can be read by BS4.
                        soup_detailed = BeautifulSoup(body, "html.parser")
                        
                        price = soup_detailed.find("div", attrs = {'class': 'listing-summary_listPrice__PJawt'}).text
                        mls_num = soup_detailed.find("div", attrs = {'class': 'listing-summary_mlsNum__1PbDv'}).text 
                        address_street = soup_detailed.find("span", attrs = {'class': 'listing-address_splitLines__pLZIy'}).text 
                        city_postal_code = soup_detailed.find("span", 
                                                                attrs = {'class': 'listing-summary_cityLine__YxXgL listing-address_splitLines__pLZIy'}).text 
                        num_of_beds = soup_detailed.find("span", 
                                                            attrs = {'data-cy': 'property-beds'}).find("span", 
                                                            attrs = {'class': 'listing-summary_propertyDetailValue__UOUcR'}).text
                        num_of_baths = soup_detailed.find("span", 
                                                            attrs = {'data-cy': 'property-baths'}).find("span", 
                                                            attrs = {'class': 'listing-summary_propertyDetailValue__UOUcR'}).text
                        
                        # If the property has a sqft value, assign that value to the variable 'sqft'
                        if len(soup_detailed.find("section", 
                                                    attrs = {'id': 'details'}).find_all("ul")[1].find_all("li")) > 1:
                            sqft = soup_detailed.find("section", 
                                                        attrs = {'id': 'details'}).find_all("ul")[1].find_all("li")[1].find_all("span")[1].text
                        else:
                            # If the property does not have a sqft value, assign the value of 'N/A' to the variable 'sqft'
                            sqft = "N/A"
                        
                        property_type = soup_detailed.find("section", 
                                                            attrs = {'id': 'details'}).find_all("ul")[1].find_all("li")[0].find_all("span")[1].text
                        last_updated = soup_detailed.find("section", 
                                                            attrs = {'id': 'details'}).find_all("ul")[0].find_all("li")[1].find_all("span")[1].text
                            
                        d = {"mls_num": mls_num, 
                            "address_street": address_street, 
                            "city_postal_code": city_postal_code, 
                            "num_of_beds": num_of_beds, 
                            "num_of_baths": num_of_baths, 
                            "sqft": sqft, 
                            "property_type": property_type, 
                            "price": price,
                            "last_updated": last_updated,
                            "link": link,
                            "status": "Active"}
                        listings.append(d)
                        print(f"Completed:====> {link}")
                        
                except AttributeError as e:
                    print(f"Error with >>>>> {link}")
                    continue
                
                
    with open(destination_file, "w") as file:
        json.dump(listings, file)
    

async def main(destination_file):
    await asyncio.gather(get_a_tags_per_page(destination_file))
    
def download_temp_file():
    asyncio.run(main('mls_temp_data.json'))
    

def download_main_data_file():
    asyncio.run(main('mls_main_data.json'))
    
# if __name__ == "__main__":
#     asyncio.run(main("mls_listing.json"))
    