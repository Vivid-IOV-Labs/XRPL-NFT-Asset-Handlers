import json

import boto3
import base64
from io import BytesIO
from image_processor import resize_image
from config import Config
import psycopg2

class AssetFetcher:

    ACCESS_CONTROL_ALLOW_HEADERS = "Content-Type, Authorization, X-Amz-Date, X-Api-Key, X-Amz-Security-Token"
    ACCESS_CONTROL_ALLOW_METHODS = "GET, OPTIONS, HEAD"
    ACCESS_CONTROL_ALLOW_CREDENTIALS = True

    def __init__(self, event: dict):
        self.event = event

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

    def fetch(self, asset_type: str):
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

        keys = []
        content_type = None
        if asset_type == "image":
            content_type = "image/jpeg"
            keys.append(f"assets/images/{token_id}/full/image")
            keys.append(f"assets/images/{token_id}/full/image.jpeg")

        if asset_type == "thumbnail":
            content_type = "image/jpeg"
            keys.append(f"assets/images/{token_id}/200px/image")
            keys.append(f"assets/images/{token_id}/200px/image.jpeg")

        if asset_type == "animation":
            keys.append(f"assets/animations/{token_id}/animation")
            keys.append(f"assets/animations/{token_id}/animation.mp4")
            keys.append(f"assets/animations/{token_id}/animation.png")
            keys.append(f"assets/animations/{token_id}/animation.gif")

        if asset_type == "video":
            content_type = "video/mp4"
            keys.append(f"assets/video/{token_id}/video")
            keys.append(f"assets/video/{token_id}/video.mp4")

        if asset_type == "metadata":
            content_type = "application/json"
            keys.append(f"assets/metadata/{token_id}/metadata")
            keys.append(f"assets/metadata/{token_id}/metadata.json")

        if asset_type == "audio":
            keys.append(f"assets/audio/{token_id}/audio")
            keys.append(f"assets/audio/{token_id}/audio.mpeg")
            keys.append(f"assets/audio/{token_id}/audio.wav")

        for key in keys:
            try:
                obj = s3.get_object(Bucket=bucket, Key=key)
                body = obj['Body']
                content = body.read()
                if content_type is None:
                    if asset_type == "animation":
                        content_type = key.split("/")[-1].replace("animation.", "")
                    if asset_type == "audio":
                        content_type = key.split("/")[-1].replace("audio.", "")
                if asset_type == "image":
                    if req_height is not None or req_width is not None:
                        output_buffer = resize_image(content, req_height, req_width)
                        content = output_buffer.getvalue()
                        return self.get_success_response(content_type, content)
                return self.get_success_response(content_type, content)
            except Exception as e:
                print(e, key)
                raise e
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
