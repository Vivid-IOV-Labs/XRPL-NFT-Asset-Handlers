import boto3
import base64
from image_processor import resize_image
from config import Config

class AssetFetcher:
    def __init__(self, event: dict):
        self.event = event

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
                        return {
                            "headers": {
                                "Content-Type": content_type,
                                "Access-Control-Allow-Origin" : "*",
                                "Access-Control-Allow-Credentials" : True
                            },
                            "statusCode": 200,
                            "body": base64.b64encode(output_buffer.getvalue()),
                            "isBase64Encoded": True
                        }

                return {
                    "headers": {
                        "Content-Type": content_type,
                        "Access-Control-Allow-Origin" : "*",
                        "Access-Control-Allow-Credentials" : True
                    },
                    "statusCode": 200,
                    "body": base64.b64encode(content),
                    "isBase64Encoded": True
                }
            except Exception as e:
                print(e, key)
                raise e
        return {"statusCode": 400}