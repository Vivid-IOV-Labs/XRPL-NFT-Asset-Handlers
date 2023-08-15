import asyncio
import logging
from engine import AssetExtractionEngine, RetryEngine, PublicRetryEngine
from asset_fetcher import AssetFetcher

logger = logging.getLogger("app_log")

formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


def nft_data_handler(event, context):
    data = event["result"]
    engine = AssetExtractionEngine(data)
    engine.run()


def fetch_images_handler(event, context):
    fetcher = AssetFetcher(event)
    return fetcher.fetch(asset_type="image")

def fetch_thumbnail_handler(event, context):
    fetcher = AssetFetcher(event)
    return fetcher.fetch(asset_type="thumbnail")

def fetch_animation_handler(event, context):
    fetcher = AssetFetcher(event)
    return fetcher.fetch(asset_type="animation")

def fetch_metadata_handler(event, context):
    fetcher = AssetFetcher(event)
    return fetcher.fetch(asset_type="metadata")

def fetch_audio_handler(event, context):
    fetcher = AssetFetcher(event)
    return fetcher.fetch(asset_type="audio")

def fetch_video_handler(event, context):
    fetcher = AssetFetcher(event)
    return fetcher.fetch(asset_type="video")

def fetch_project_metadata(event, context):
    fetcher = AssetFetcher(event)
    return fetcher.fetch_project_metadata()

def retry(event, context):
    path = event["Records"][0]["s3"]["object"]["key"]
    engine = RetryEngine(path=path)
    engine.run()

def public_retry(event, content):
    token_id = event["pathParameters"].get("token_id", None)
    if token_id is None:
        return {"statusCode": 400, "body": "token_id required"}
    engine = PublicRetryEngine(token_id=token_id)
    engine.run()
    return {"statusCode": 200}
