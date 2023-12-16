import asyncio
import aiohttp
import json
from bs4 import BeautifulSoup
from datetime import datetime
import boto3
        
# async def ingest_data_from_details_page(list_of_href_to_details_page, destination_file):
async def ingest_data_from_details_page(source_text_file):
    listings = []  
    async with aiohttp.ClientSession() as session:
        # For each href that links to a detailed page for a specific lisitng:
        # for link in list_of_href_to_details_page:
        #     print(f"Processing:====> {link}")
        with open(source_text_file) as file:
            links = file.readlines()
            for link in links:
                # print(link)
                if "https://www.remax.ca/commercial" in link:
                    continue
                else:
                # Get the page in a response object
                    try:
                        async with session.get(link) as resp:
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
                            if len(listings) == 10:
                                break
                            print(f"Completed:====> {link}")
                            
                    except AttributeError as e:
                        print(f"Error with >>>>> {link}")
                        continue
                
    current_datetime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")  
    str_current_datetime = str(current_datetime)
    file_name = f"mls_listing_Calgary_{str_current_datetime}.json"
    
    
    s3 = boto3.resource('s3')
    json_str = json.dumps(listings)
    
    s3.put_object(Bucket='s3://mogononso-demo-s3', Key=file_name, Body=json_str)
    # with open(file_name, "w") as file:
    #     json.dump(listings, file)


async def main(source_text_file):
    await asyncio.gather(ingest_data_from_details_page(source_text_file))
    
def download_data_for_each_url(source_text_file):
    start_time = datetime.now()
    asyncio.run(main(source_text_file))
    finish_time = datetime.now()
    print(f"Start Time : {start_time}\nFinish Time : {finish_time}")