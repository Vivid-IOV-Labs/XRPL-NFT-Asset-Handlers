import json
from typing import List

import boto3
import base64
from image_processor import resize_image
from config import Config
import psycopg2

class AssetFetcher:

    ACCESS_CONTROL_ALLOW_HEADERS = "Content-Type, Authorization, X-Amz-Date, X-Api-Key, X-Amz-Security-Token"
    ACCESS_CONTROL_ALLOW_METHODS = "GET, OPTIONS, HEAD"
    ACCESS_CONTROL_ALLOW_CREDENTIALS = True

    def __init__(self, event: dict):
        self.event = event
        self.asset_to_content_type_mapping = {
            'image': 'image/jpeg',
            'video': 'video/mp4',
            'audio': None,
            'animation': None,
            'thumbnail': 'image/jpeg',
            'metadata': 'application/json',
        }

    def get_success_response(self, content_type: str, content):
        return {
            "headers": {
                "Content-Type": content_type,
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": self.ACCESS_CONTROL_ALLOW_CREDENTIALS,
                "Access-Control-Allow-Methods": self.ACCESS_CONTROL_ALLOW_METHODS,
                "Access-Control-Allow-Headers": self.ACCESS_CONTROL_ALLOW_HEADERS
            },
            "statusCode": 200,
            "body": base64.b64encode(content).decode("utf-8"),
            "isBase64Encoded": True
        }

    @staticmethod
    def _get_possible_image_keys(token_id: str) -> List[str]:
        return [f"assets/images/{token_id}/full/image", f"assets/images/{token_id}/full/image.jpeg"]

    @staticmethod
    def _get_possible_thumbnail_keys(token_id: str) -> List[str]:
        return [f"assets/images/{token_id}/200px/image", f"assets/images/{token_id}/200px/image.jpeg"]

    @staticmethod
    def _get_possible_animation_keys(token_id: str) -> List[str]:
        return [
            f"assets/animations/{token_id}/animation", f"assets/animations/{token_id}/animation.mp4",
            f"assets/animations/{token_id}/animation.png", f"assets/animations/{token_id}/animation.gif"
        ]

    @staticmethod
    def _get_possible_video_keys(token_id: str) -> List[str]:
        return [f"assets/video/{token_id}/video", f"assets/video/{token_id}/video.mp4"]

    @staticmethod
    def _get_possible_audio_keys(token_id: str) -> List[str]:
        return [
            f"assets/audio/{token_id}/audio", f"assets/audio/{token_id}/audio.mpeg",
            f"assets/audio/{token_id}/audio.wav"
        ]

    @staticmethod
    def _get_possible_metadata_keys(token_id: str) -> List[str]:
        return [f"assets/metadata/{token_id}/metadata", f"assets/metadata/{token_id}/metadata.json"]

    def _get_possible_keys_for_asset(self, asset_type: str, token_id: str) -> List[str]:
        if asset_type == "image":
            return self._get_possible_image_keys(token_id)

        if asset_type == "thumbnail":
            return self._get_possible_thumbnail_keys(token_id)

        if asset_type == "animation":
            return self._get_possible_animation_keys(token_id)

        if asset_type == "video":
            return self._get_possible_video_keys(token_id)

        if asset_type == "metadata":
            return self._get_possible_metadata_keys(token_id)

        if asset_type == "audio":
            return self._get_possible_audio_keys(token_id)

    @staticmethod
    def _get_content_type_from_asset_key(key: str, asset_type: str) -> str:
        file_name = key.split("/")[-1]
        content_type = file_name.replace(f"{asset_type}.", "")
        return content_type

    @staticmethod
    def _resize_image(content, target_height, target_width):
        output_buffer = resize_image(content, target_height, target_width)
        output = output_buffer.getvalue()
        return output


    def fetch(self, asset_type: str):
        if asset_type not in self.asset_to_content_type_mapping.keys():
            return {"statusCode": 400}
        session = boto3.Session()
        s3 = session.client("s3")
        bucket = Config.DATA_DUMP_BUCKET

        params = self.event["pathParameters"]
        query_params = self.event.get("queryStringParameters")
        req_height = None
        req_width = None
        if query_params is not None:
            req_height = query_params.get("height")
            req_width = query_params.get("width")
        token_id = params.get("token_id")

        keys = self._get_possible_keys_for_asset(asset_type, token_id)
        content_type = self.asset_to_content_type_mapping[asset_type]

        for key in keys:
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                body = obj['Body']
                content = body.read()
                if content_type is None:
                    content_type = self._get_content_type_from_asset_key(key, asset_type)
                if asset_type == "image" and (req_height is not None or req_width is not None):
                    content = self._resize_image(content, req_height, req_width)
                return self.get_success_response(content_type, content)
            except Exception as e:
                print(e, key)
                continue
        return {"statusCode": 400}

    def fetch_project_metadata(self):
        session = boto3.Session()
        s3 = session.client("s3")
        bucket = Config.DATA_DUMP_BUCKET

        params = self.event["pathParameters"]
        query_params = self.event.get("queryStringParameters")

        issuer = params.get("issuer")
        taxon = params.get("taxon")
        page_num = int(query_params.get("page")) if query_params else 1

        if int(page_num) <= 0:
            return {"statusCode": 400, "body": "invalid page number"}

        query = ""
        if page_num == 1:
            query = f"SELECT nft_token_id FROM project_tracker WHERE issuer = '{issuer}' AND taxon = {taxon} LIMIT 10"
        else:
            offset = (page_num - 1) * 10
            query = f"SELECT nft_token_id FROM project_tracker WHERE issuer = '{issuer}' AND taxon = {taxon} LIMIT 10 OFFSET {offset}"
        count_query = f"SELECT COUNT(nft_token_id) FROM project_tracker WHERE issuer = '{issuer}' AND taxon = {taxon}"

        connection = psycopg2.connect(
            user=Config.RDS_USER,
            password=Config.RDS_PASSWORD,
            host=Config.DB_HOST,
            port=Config.RDS_PORT,
            database=Config.DB_NAME
        )
        cursor = connection.cursor()

        cursor.execute(query)
        token_ids = cursor.fetchall()
        cursor.execute(count_query)
        total_ids = cursor.fetchall()[0][0]
        keys = [(token_id[0], f"assets/metadata/{token_id[0]}/metadata") for token_id in token_ids]
        metadatas = []

        for (token_id, key) in keys:
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                body = obj['Body']
                content = body.read()
                to_dict = json.loads(content)
                metadatas.append({"token_id": token_id, "metadata": to_dict})
            except Exception as e:
                print(f"Error Fetching Metadata for TokenID: {token_id}\n{e}")
        results = {
            "data": metadatas,
            "count": total_ids,
            "current_page": page_num
        }
        content = bytes(json.dumps(results), "utf-8")
        content_type = "application/json"
        return self.get_success_response(content_type, content)
