import boto3
from config import Config


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

    def write(self, path, buffer):
        s3 = self._get_s3_resource()
        s3.Object(self.bucket, path).put(Body=buffer.getvalue())
