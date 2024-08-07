import aioboto3
from config import Config
from io import BytesIO
import json
import logging

logger = logging.getLogger("app_log")


class LocalFileWriter:
    pass


class AsyncS3FileWriter:
    def __init__(self):
        self.bucket = Config.DATA_DUMP_BUCKET
        self.access_key_id = Config.ACCESS_KEY_ID
        self.secret_access_key = Config.SECRET_ACCESS_KEY

    async def _write(self, path, buffer, content_type):
        logger.info(
            f"Uploading File to {self.bucket}/{path} with content Type {content_type}"
        )
        session = aioboto3.Session(
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
        )
        buffer.seek(0)
        async with session.client("s3") as s3:
            await s3.upload_fileobj(
                buffer, self.bucket, path, ExtraArgs={"ContentType": content_type}
            )
        logger.info(f"File Uploaded to {self.bucket}/{path}")

    async def write_image(self, path, image, content_type):
        buffer = BytesIO()
        fmt = content_type.split("/")[-1]
        image.save(buffer, format=fmt.upper())
        await self._write(path, buffer, content_type)

    async def write_json(self, path, obj):
        to_bytes = json.dumps(obj, indent=4).encode("utf-8")
        buffer = BytesIO()
        buffer.write(to_bytes)
        await self._write(path, buffer, "application/json")

    async def write_media(self, path, content, content_type):
        buffer = BytesIO()
        buffer.write(content)
        await self._write(path, buffer, content_type)
