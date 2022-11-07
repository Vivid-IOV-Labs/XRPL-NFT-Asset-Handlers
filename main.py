from typing import Dict, Optional
import json
import requests
from io import BytesIO
from PIL import Image
from writers import S3FileWriter
import aiohttp
import asyncio


def convert_hex_to_text(hex_str: str) -> str:
    return bytes.fromhex(hex_str).decode('utf-8')


def get_nft_token_uri(txn_data: Dict) -> str:
    token_uri_hex = txn_data["URI"]
    return convert_hex_to_text(token_uri_hex)


def get_nft_token_id(txn_data: Dict) -> str:
    affected_nodes = txn_data["meta"]["AffectedNodes"]
    modified_nodes = [node for node in affected_nodes if node.get("ModifiedNode", False)]
    token_page_node = [node for node in modified_nodes if node["ModifiedNode"]["LedgerEntryType"] == "NFTokenPage"][0]
    final_fields = token_page_node["ModifiedNode"]["FinalFields"]["NFTokens"]
    previous_fields = token_page_node["ModifiedNode"]["PreviousFields"]["NFTokens"]
    target_nft = [field for field in final_fields if field not in previous_fields][0]
    return target_nft["NFToken"]["NFTokenID"]


def fetch_token_metadata(token_uri: str) -> Optional[Dict]:
    url = f"https://ipfs.io/ipfs/{token_uri.replace('ipfs://', '')}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None


def process_image(img_url: str):
    resp = requests.get(img_url)
    img = Image.open(BytesIO(resp.content))
    width, height = img.size
    aspect_ratio = width / height
    new_height = 200
    new_width = int(aspect_ratio * new_height)
    new_image = img.resize((new_width, new_height))
    return img, new_image


if __name__ == "__main__":
    input_data = json.load(open("data/nft-text-tx-data.json", "r"))
    issuer = input_data["Issuer"]
    token_uri = get_nft_token_uri(input_data)
    nft_token_id = get_nft_token_id(input_data)
    meta_data = fetch_token_metadata(token_uri)
    image_url = meta_data["image"]
    full, resized = process_image(image_url)
    writer = S3FileWriter()
    writer.write_image(f"assets/images/{issuer}/{nft_token_id}/200px/{nft_token_id}.jpg", resized)
    writer.write_image(f"assets/images/{issuer}/{nft_token_id}/full/{nft_token_id}.jpg", full)
    writer.write_json(f"assets/metadata/{issuer}/{nft_token_id}/metadata.json", meta_data)


