import asyncio
import json

from engine import Engine
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
    test_data = json.load(open("data/test-data.json", "r"))  # noqa
    engine = Engine(test_data['result'])
    await engine.run()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_test())
