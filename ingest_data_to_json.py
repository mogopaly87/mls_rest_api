import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
import pyspark as spark
from datetime import datetime
import asyncio
import aiohttp
import json


HEADERS = ({'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US, en;q=0.5'})


data = []
listing = []
# Async requests
async def get_a_tags_per_page():
    
    async with aiohttp.ClientSession() as session:
        print("Started detailed link extraction....")
        for i in range(1, 3):
            PAGE_URL = f"https://www.remax.ca/nl/st-johns-real-estate?pageNumber={i}"
            async with session.get(PAGE_URL, headers = HEADERS) as resp:
                body = await resp.text()
                soup = BeautifulSoup(body, "html.parser")
                links_per_page = soup.find_all("a", attrs = {'class':'listing-card_listingCard__G6M8g'})
                
                for link in links_per_page:
                    current_href = link.get("href")
                    data.append(current_href)
        print("Ended detailed links extraction!")
        print(f"Extracted {len(data)} links\n")
        print("Started data extraction from details pages...")
        await ingest()
        print("\n")
        print("COMPLETED data extraction from details pages!")
        # print(data)
async def ingest():  
    async with aiohttp.ClientSession() as session:                  
        for link in data:
            async with session.get(link, headers = HEADERS) as resp:
                body = await resp.text()
                soup_detailed = BeautifulSoup(body, "html.parser")
                price = soup_detailed.find("div", attrs = {'class': 'listing-summary_listPrice__PJawt'}).text
                mls_num = soup_detailed.find("div", attrs = {'class': 'listing-summary_mlsNum__1PbDv'}).text 
                address_street = soup_detailed.find("span", attrs = {'class': 'listing-address_splitLines__pLZIy'}).text 
                city_postal_code = soup_detailed.find("span", attrs = {'class': 'listing-summary_cityLine__YxXgL listing-address_splitLines__pLZIy'}).text 
                num_of_beds = soup_detailed.find("span", attrs = {'data-cy': 'property-beds'}).find("span", attrs = {'class': 'listing-summary_propertyDetailValue__UOUcR'}).text
                num_of_baths = soup_detailed.find("span", attrs = {'data-cy': 'property-baths'}).find("span", attrs = {'class': 'listing-summary_propertyDetailValue__UOUcR'}).text
                
                # If the property has a sqft value, assign that value to the variable 'sqft'
                if len(soup_detailed.find("section", attrs = {'id': 'details'}).find_all("ul")[1].find_all("li")) > 1:
                    sqft = soup_detailed.find("section", attrs = {'id': 'details'}).find_all("ul")[1].find_all("li")[1].find_all("span")[1].text
                else:
                    # If the property does not have a sqft value, assign the value of 'N/A' to the variable 'sqft'
                    sqft = "N/A"
                
                property_type = soup_detailed.find("section", attrs = {'id': 'details'}).find_all("ul")[1].find_all("li")[0].find_all("span")[1].text
                last_updated = soup_detailed.find("section", attrs = {'id': 'details'}).find_all("ul")[0].find_all("li")[1].find_all("span")[1].text
                    
                d = {"mls_num": mls_num, 
                    "address_street": address_street, 
                    "city_postal_code": city_postal_code, 
                    "num_of_beds": num_of_beds, 
                    "num_of_baths": num_of_baths, 
                    "sqft": sqft, 
                    "property_type": property_type, 
                    "price": price,
                    "last_updated": last_updated}
                print(f"Completed:====> {link}")
                listing.append(d)
                
    with open("mls_listing.json", "w") as file:
        json.dump(listing, file)
    

async def main():
    await asyncio.gather(get_a_tags_per_page())
    
if __name__ == "__main__":
    asyncio.run(main())
    