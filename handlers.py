import logging
import requests
import time
import json
from enum import Enum

from engine import AssetExtractionEngine, RetryEngine, PublicRetryEngine
from config import Config
from asset_fetcher import AssetFetcher

logger = logging.getLogger("app_log")

formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


class EventName(Enum):
    METADATA = "metadata"
    COLLECTIONS = "metadata_collections"
    THUMBNAILS = "metadata_thumbnails"
    IMAGES = "metadata_images"
    ANIMATIONS = "metadata_animations"


def mixpanel_tracking(event: EventName, ip_address: str, token_id: str):
    payload = {
        "event": event.value(),
        "properties": {
            "time": int(time.time()),
            "ip_address": ip_address,
            "token_id": token_id,
            "token": Config.MIXPANEL_PROJECT_TOKEN
        }
    }
    requests.post(
        "https://api.mixpanel.com/track",
        params={"strict": "1"},
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload)
    )


def nft_data_handler(event, context):
    data = event["result"]
    engine = AssetExtractionEngine(data)
    engine.run()


def fetch_images_handler(event, context):
    ip_address = event['headers']['x-forwarded-for']
    token_id = event['pathParameters']['token_id']
    fetcher = AssetFetcher(event)
    result = fetcher.fetch(asset_type="image")
    mixpanel_tracking(EventName.IMAGES, ip_address, token_id)
    return result

def fetch_thumbnail_handler(event, context):
    ip_address = event['headers']['x-forwarded-for']
    token_id = event['pathParameters']['token_id']
    fetcher = AssetFetcher(event)
    result = fetcher.fetch(asset_type="thumbnail")
    mixpanel_tracking(EventName.THUMBNAILS, ip_address, token_id)
    return result

def fetch_animation_handler(event, context):
    ip_address = event['headers']['x-forwarded-for']
    token_id = event['pathParameters']['token_id']
    fetcher = AssetFetcher(event)
    result = fetcher.fetch(asset_type="animation")
    mixpanel_tracking(EventName.ANIMATIONS, ip_address, token_id)
    return result

def fetch_metadata_handler(event, context):
    ip_address = event['headers']['x-forwarded-for']
    token_id = event['pathParameters']['token_id']
    fetcher = AssetFetcher(event)
    result = fetcher.fetch(asset_type="metadata")
    mixpanel_tracking(EventName.METADATA, ip_address, token_id)
    return result


def fetch_audio_handler(event, context):
    fetcher = AssetFetcher(event)
    result = fetcher.fetch(asset_type="audio")
    return result

def fetch_video_handler(event, context):
    fetcher = AssetFetcher(event)
    return fetcher.fetch(asset_type="video")

def fetch_project_metadata(event, context):
    ip_address = event['headers']['x-forwarded-for']
    token_id = event['pathParameters']['token_id']
    fetcher = AssetFetcher(event)
    result = fetcher.fetch_project_metadata()
    mixpanel_tracking(EventName.COLLECTIONS, ip_address, token_id)
    return result

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
