import asyncio
import boto3
import base64
import logging
from engine import Engine
from config import Config
from utils import fetch_s3_folder_contents
from image_processor import resize_image

logger = logging.getLogger("app_log")

formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")  # noqa
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


def nft_data_handler(event, context):
    data = event["result"]
    engine = Engine(data)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(engine.run())


def fetch_asset_handler(event, context):
    session = boto3.Session()
    s3 = session.client("s3")
    bucket = Config.DATA_DUMP_BUCKET

    params = event["pathParameters"]
    req_height = event.get("queryStringParameters", {}).get('height')
    req_width = event.get("queryStringParameters", {}).get('width')
    issuer = params.get("issuer")
    asset = params.get("asset")

    keys = []
    content_type = None
    if asset == "image":
        content_type = "image/jpeg"
        keys.append(f"assets/images/{issuer}/full/image")
        keys.append(f"assets/images/{issuer}/full/image.jpeg")

    if asset == "thumbnail":
        content_type = "image/jpeg"
        keys.append(f"assets/images/{issuer}/200px/image")
        keys.append(f"assets/images/{issuer}/200px/image.jpeg")

    if asset == "animation":
        keys.append(f"assets/animations/{issuer}/animation")
        keys.append(f"assets/animations/{issuer}/animation.mp4")
        keys.append(f"assets/animations/{issuer}/animation.png")
        keys.append(f"assets/animations/{issuer}/animation.gif")

    if asset == "video":
        content_type = "video/mp4"
        keys.append(f"assets/video/{issuer}/video")
        keys.append(f"assets/video/{issuer}/video.mp4")

    if asset == "metadata":
        content_type = "application/json"
        keys.append(f"assets/metadata/{issuer}/metadata")
        keys.append(f"assets/metadata/{issuer}/metadata.json")

    if asset == "audio":
        keys.append(f"assets/audio/{issuer}/audio")
        keys.append(f"assets/audio/{issuer}/audio.mpeg")
        keys.append(f"assets/audio/{issuer}/audio.wav")

    for key in keys:
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            body = obj['Body']
            content = body.read()
            if content_type is None:
                if asset == "animation":
                    content_type = key.split("/")[-1].replace("animation.", "")
                if asset == "audio":
                    content_type = key.split("/")[-1].replace("audio.", "")
            if asset == "image":
                if req_height is not None or req_width is not None:
                    output_buffer = resize_image(content, req_height, req_width)
                    return {
                        "headers": { "Content-Type": content_type },
                        "statusCode": 200,
                        "body": base64.b64encode(output_buffer.getvalue()),
                        "isBase64Encoded": True
                    }

            return {
                "headers": { "Content-Type": content_type },
                "statusCode": 200,
                "body": base64.b64encode(content),
                "isBase64Encoded": True
            }
        except Exception as e:
            print(e, key)
            continue
    return {"statusCode": 400}
#
# def fetch_asset_content_type_handler(event, context):
#     bucket = Config.DATA_DUMP_BUCKET
#
#     params = event["pathParameters"]
#     issuer = params.get("issuer")
#     asset = params.get("asset")
#
#     if asset == "image":
#         return {"content_type": "image/jpeg"}
#         # keys.append(f"assets/images/{issuer}/full/image")
#         # keys.append(f"assets/images/{issuer}/full/image.jpeg")
#
#     if asset == "thumbnail":
#         return {"content_type": "image/jpeg"}
#         # keys.append(f"assets/images/{issuer}/200px/image")
#         # keys.append(f"assets/images/{issuer}/200px/image.jpeg")
#
#     if asset == "animation":
#         contents = fetch_s3_folder_contents(Config, f"assets/animations/{issuer}/", bucket)
#         return {"content_type": contents[-1].split("/")[-1].replace("animation.", "")}
#         # keys.append(f"assets/animations/{issuer}/animation")
#         # keys.append(f"assets/animations/{issuer}/animation.mp4")
#         # keys.append(f"assets/animations/{issuer}/animation.png")
#         # keys.append(f"assets/animations/{issuer}/animation.gif")
#
#     if asset == "video":
#         return {"content_type": "video/mp4"}
#         # keys.append(f"assets/video/{issuer}/video")
#         # keys.append(f"assets/video/{issuer}/video.mp4")
#
#     if asset == "metadata":
#         return {"content_type": "application/json"}
#         # keys.append(f"assets/metadata/{issuer}/metadata")
#         # keys.append(f"assets/metadata/{issuer}/metadata.json")
#
#     if asset == "audio":
#         contents = fetch_s3_folder_contents(Config, f"assets/audios/{issuer}/", bucket)
#         return {"content_type": contents[-1].split("/")[-1].replace("audio.", "")}
#         # keys.append(f"assets/audio/{issuer}/audio")
#         # keys.append(f"assets/audio/{issuer}/audio.mpeg")
#         # keys.append(f"assets/audio/{issuer}/audio.wav")
#     return {"statusCode": 400}

# def fetch_image_with_dimension(event, context):
#     from PIL import Image, ImageFile
#     from io import BytesIO
#
#     ImageFile.LOAD_TRUNCATED_IMAGES = True
#
#     session = boto3.Session()
#     s3 = session.client("s3")
#     bucket = Config.DATA_DUMP_BUCKET
#
#     params = event["pathParameters"]
#     token_id = params.get("token_id")
#
#     obj = s3.get_object(Bucket=bucket, Key=f"assets/images/{token_id}/full/image")
#     body = obj['Body']
#     content = body.read()
#
#     req_height = event["queryStringParameters"].get('height')
#     req_width = event["queryStringParameters"].get('width')
#
#     output_buffer = resize_image(content, req_height, req_width)
#     return {"statusCode": 200, "body": base64.b64encode(output_buffer.getvalue()), "isBase64Encoded": True}

def retry(event, context):
    engine = Engine({"URI": ""})
    path = event["Records"][0]["s3"]["object"]["key"]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(engine.retry(path))
