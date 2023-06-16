import asyncio
import json

from engine import Engine
from asset_fetcher import AssetFetcher
import logging

# Logging
logger = logging.getLogger("app_log")
formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("logger.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


async def run_test():

    rerun_data = {
        "Issuer": "rKiNWUkVsq1rb9sWForfshDSEQDSUncwEu",
        "NFTokenID": "000803E8CEC1EB1B331D8A55E39D451DE8E13F59CF5509D5B34D5959000002BD",
        "URI": "68747470733A2F2F6E667473746F726167652E6C696E6B2F697066732F6261666B726569623566616C6166346D70696332656171686D73706135366B63356362666B716A36627568707633656F63326E7A6C656C6B763269",
        "NFTokenTaxon": 1,
        "Source": "xummapp-frontend"
    }
    event1 = {
        "pathParameters": {
            "token_id": "0000000006573034BE857B870D6ABEFC24721C29AACBEB8B16E5DA9E00000001"
        },
        "queryStringParameters": {
            "height": "200"
        }
    }
    metadata_event = {
        "pathParameters": {
            "issuer": "rpbjkoncKiv1LkPWShzZksqYPzKXmUhTW7",
            "taxon": 52
        },
        "queryStringParameters": {
            "page": "1"
        }
    }
    # test_data = json.load(open("data/test-data.json", "r"))  # noqa
    # engine = Engine(rerun_data)
    # await engine.retry(rerun_data)
    fetcher = AssetFetcher(metadata_event)
    result = fetcher.fetch_project_metadata()
    __import__("ipdb").set_trace()
    print(result)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_test())
