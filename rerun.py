import asyncio
import json

from engine import Engine, Config
from utils import chunks, fetch_text_objects, read_json, delete_from_s3
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

async def text_rerun(path):
    try:
        path_split = path.split("/")
        token_id = path_split[2]
        data = await read_json(Config.DATA_DUMP_BUCKET, path, Config)
        engine = Engine(data)
        await engine.retry_text_metadata(data, token_id)
        await delete_from_s3(Config.DATA_DUMP_BUCKET, path, Config)
    except json.JSONDecodeError:
        await delete_from_s3(Config.DATA_DUMP_BUCKET, path, Config)
    except Exception as e:
        print(e)

async def multiple_texts_reruns():
    paths = fetch_text_objects(Config)
    for chunk in chunks(paths, 100):
        await asyncio.gather(*[text_rerun(path) for path in chunk])


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    # loop.run_until_complete(multiple_texts_reruns())
    loop.run_until_complete(rerun("error/00083A98425D5408873C7C141C270879D254C84AC1CC10F0A15E6442000003A7.json"))
