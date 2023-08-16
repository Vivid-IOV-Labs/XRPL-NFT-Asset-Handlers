import asyncio
import argparse
import json

from engine import AssetExtractionEngine, RetryEngine, PublicRetryEngine, TextMetadataRerunEngine, Config
from utils import fetch_text_objects
from asset_fetcher import AssetFetcher
import logging


if __name__ == "__main__":
    # Argument Parsers
    parser = argparse.ArgumentParser(
        prog="peerkat",
        description='''
            Management Command for XLS20 Asset Fetching and Extraction.
            Samples:
                python main.py --command=extract --stage=nft-mint
            '''
    )
    parser.add_argument("--command", required=True, help="Command type to run. Accepts `extract` or `fetch`")
    parser.add_argument("--stage", help="For asset extraction, we have `nft-mint`, `retry`, `public-retry` and `projects-retry`")
    parser.add_argument("--data_path", help="Path to the json file for input")
    parser.add_argument("--token_id", help="NFT token id")
    args = parser.parse_args()

    # Initialize Loggers
    logger = logging.getLogger("app_log")
    formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")  # noqa
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler("logger.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Extract CLI Arguments
    command = args.command
    stage = args.stage

    # Execute the command
    if command == "extract":
        if stage == "nft-mint":
            path = args.data_path
            data = json.load(open(path, "r"))
            engine = AssetExtractionEngine(data)
            engine.run()

        elif stage == "retry":
            path = args.data_path
            engine= RetryEngine(path=path)
            if "s3" in path:
                path = path.replace("s3://", "")
                engine.path = path
            else:
                data = json.load(open(path, "r"))
                engine.path = None
                engine.data = data
            engine.run()

        elif stage == "text-metadata":
            paths = fetch_text_objects(Config)
            engine = TextMetadataRerunEngine(paths=paths)
            engine.run()

        elif stage == "public-retry":
            token_id = args.token_id
            engine = PublicRetryEngine(token_id=token_id)
            engine.run()
