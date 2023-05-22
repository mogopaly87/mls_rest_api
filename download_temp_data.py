import ingest_data_to_json as ingest
import asyncio
    
if __name__ == '__main__':
    asyncio.run(ingest.main('mls_temp.json'))