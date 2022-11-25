from typing import Optional
import boto3
import json


def hex_to_text(hex_str: str) -> str:
    return bytes.fromhex(hex_str).decode('utf-8')


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
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    s3_bucket = s3.Bucket(bucket)
    last_file = sorted([obj.key for obj in s3_bucket.objects.filter(Prefix="NFTokenMint")])[-1]
    obj = s3.Object(bucket, last_file)
    return json.load(obj.get()['Body'])
