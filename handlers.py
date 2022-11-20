import asyncio
import logging
import json
from engine import Engine

logger = logging.getLogger("app_log")

formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


def nft_data_handler(event, context):
    payload = json.loads(context)
    engine = Engine(payload)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(engine.run())
