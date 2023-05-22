import pandas as pd
import ingest_data_to_json as ingest
import asyncio



# df = pd.read_json("mls_listing.json")

# # DATA TRANSFORMATION
# # ================================================================================================================

# df[['mls_escape','mls_num']] = df.mls_num.str.split(":", expand=True)
# df[['city', 'province', 'postal_code']] = df.city_postal_code.str.split(",", expand=True)
# df['last_updated'] = df.last_updated.str[4:]
# df.drop(columns=['city_postal_code', 'mls_escape',], axis=1, inplace=True)
# df['price'] = df.price.str[1:]
# df['sqft'] = df['sqft'].str.replace("SQFT", "")
# df['last_updated'] = df['last_updated'].str.replace(',', '').str.strip().str.replace(" ", "-")
# df['last_updated'] = pd.to_datetime(df['last_updated'], format="%B-%d-%Y")
# df['price'] = df['price'].str.replace(',', '')
# df['city'] = df['city'].str.title()
# df['city'] = df['city'].str.replace("'S", "'s")
# df['address_street'] = df['address_street'].str.title()
# df['postal_code'] = df['postal_code'].str.upper()
# df['sqft'] = df['sqft'].str.replace(',', '')
# df[['sqft', 'num_of_beds']] = df[['sqft', 'num_of_beds']].replace('N/A', '0')
# df[['num_of_baths', 'num_of_beds', 'sqft', 'price']] = df[['num_of_baths', 'num_of_beds', 'sqft', 'price']].astype(int)


asyncio.run(ingest.main())