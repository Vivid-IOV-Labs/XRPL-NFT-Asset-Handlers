import boto3
from config import Config
from io import BytesIO
import json


class LocalFileWriter:
    pass


class S3FileWriter:

    def __init__(self):
        self.bucket = Config.DATA_DUMP_BUCKET
        self.access_key_id = Config.ACCESS_KEY_ID
        self.secret_access_key = Config.SECRET_ACCESS_KEY

    def _get_s3_resource(self):
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key
        )
        return s3

    def _write(self, path, buffer):
        s3 = self._get_s3_resource()
        s3.Object(self.bucket, path).put(Body=buffer.getvalue())

    def write_image(self, path, image):
        buffer = BytesIO()
        image.save(buffer, format='PNG')
        self._write(path, buffer)

    def write_json(self, path, obj):
        to_bytes = json.dumps(obj, indent=4).encode('utf-8')
        buffer = BytesIO()
        buffer.write(to_bytes)
        self._write(path, buffer)
