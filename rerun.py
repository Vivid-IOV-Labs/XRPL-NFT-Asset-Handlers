import asyncio
import json

from engine import Engine, Config
from utils import fetch_failed_objects, chunks
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


async def rerun(path):
    engine = Engine({"URI": ""})
    await engine.retry(path)


async def main():
    failed_objects = fetch_failed_objects(Config)
    # for path in failed_objects[20:30]:
    #     await rerun(path)
    # await rerun("notfound/000000007EFD66D7DA6C495613C3ABE122007097122045BBFE25BCDE00000043.json")
    for chunk in chunks(failed_objects[1:], 50):
        await asyncio.gather(*[rerun(path) for path in chunk])


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    # await main()