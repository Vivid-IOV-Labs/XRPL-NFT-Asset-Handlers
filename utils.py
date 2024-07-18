from typing import Optional
import boto3
import aioboto3
import aiohttp
import logging
import json
import requests

logger = logging.getLogger("app_log")

JSON_RPC_URL = "https://s2.ripple.com:51234/"


def hex_to_text(hex_str: str) -> str:
    return bytes.fromhex(hex_str).decode("utf-8")


def is_ipfs(url: str):
    return True if url.startswith("ipfs://") else False


def is_normal_url(url: str):
    return True if url.startswith("https://") else False


def get_path_from_ipfs_url(url: str):
    return url.replace("ipfs://", "").replace("ipfs/", "")


def get_file_extension(file_path: str) -> Optional[str]:
    split = file_path.split(".")
    return split[-1] if split else None


def get_last_file_dump(access_key: str, secret_key: str, bucket: str):
    s3 = boto3.resource(
        "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
    )
    s3_bucket = s3.Bucket(bucket)
    last_file = sorted(
        [obj.key for obj in s3_bucket.objects.filter(Prefix="NFTokenMint")]
    )[-1]
    obj = s3.Object(bucket, last_file)
    return json.load(obj.get()["Body"])


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def fetch_account_info(address: str):
    payload = {
        "method": "account_info",
        "params": [
            {
                "account": address,
                "strict": True,
                "ledger_index": "current",
                "queue": True,
            }
        ],
    }
    response = requests.post(JSON_RPC_URL, data=json.dumps(payload))
    if response.status_code == 200:
        return response.json()["result"]["account_data"]
    return None


async def fetch_account_info_async(address: str):
    payload = {
        "method": "account_info",
        "params": [
            {
                "account": address,
                "strict": True,
                "ledger_index": "current",
                "queue": True,
            }
        ],
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(JSON_RPC_URL, data=json.dumps(payload)) as response:
            if response.status == 200:
                content = await response.content.read()
                data = json.loads(content)["result"]
                return data["account_data"]
            return None


async def delete_from_s3(bucket, key, config):
    session = aioboto3.Session(
        aws_access_key_id=config.ACCESS_KEY_ID,
        aws_secret_access_key=config.SECRET_ACCESS_KEY,
    )
    async with session.client("s3") as s3:
        await s3.delete_object(Bucket=bucket, Key=key)
    logger.info(f"{bucket}/{key} Deleted")


async def read_file(key, config, bucket):
    session = aioboto3.Session(
        aws_access_key_id=config.ACCESS_KEY_ID,
        aws_secret_access_key=config.SECRET_ACCESS_KEY,
    )
    async with session.client("s3") as s3:
        try:
            res = await s3.get_object(Bucket=bucket, Key=key)
        except Exception:  # noqa
            return None
        body = res["Body"]
        data = await body.read()
        return data


async def read_json(bucket, key, config):
    data = await read_file(key, config, bucket)
    return json.loads(data)


def fetch_failed_objects(config):
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=config.ACCESS_KEY_ID,
        aws_secret_access_key=config.SECRET_ACCESS_KEY,
    )
    bucket = s3.Bucket(config.CACHE_FAILED_LOG_BUCKET)
    return [obj.key for obj in bucket.objects.filter(Prefix="notfound/")]


def fetch_s3_folder_contents(config, prefix, bucket):
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=config.ACCESS_KEY_ID,
        aws_secret_access_key=config.SECRET_ACCESS_KEY,
    )
    bucket = s3.Bucket(bucket)
    return [obj.key for obj in bucket.objects.filter(Prefix=prefix)]


def fetch_text_objects(config):
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=config.ACCESS_KEY_ID,
        aws_secret_access_key=config.SECRET_ACCESS_KEY,
    )
    bucket = s3.Bucket(config.DATA_DUMP_BUCKET)
    return [obj.key for obj in bucket.objects.filter(Prefix="assets/text")]
