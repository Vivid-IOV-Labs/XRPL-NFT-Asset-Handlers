import json
import logging
import traceback

from writers import AsyncS3FileWriter, Config
from utils import delete_from_s3, read_json
from fetcher import Fetcher
from image_processor import process_image
from extractors import TokenIDExtractor, TokenURIExtractor, DomainURIExtractor
from exceptions import NoMetaDataException

logger = logging.getLogger("app_log")


class Engine:
    def __init__(self, data):
        self.data = data
        self.writer = AsyncS3FileWriter()
        self.fetcher = Fetcher()
        self.token_id_extractor = TokenIDExtractor(data)
        self.token_uri_extractor = TokenURIExtractor(data)

    async def _dump_image(self, content: bytes, token_id: str, content_type):
        full, resized = process_image(content)
        ext = content_type.split("/")[1]
        await self.writer.write_image(f"assets/images/{token_id}/200px/image", resized, "image/png")
        await self.writer.write_image(f"assets/images/{token_id}/200px/image.png", resized, "image/png")
        await self.writer.write_image(f"assets/images/{token_id}/full/image", full, content_type)
        await self.writer.write_image(f"assets/images/{token_id}/full/image.{ext}", full, content_type)

    async def _extract_assets(self, token_id, token_uri):
        if token_uri is None:
            logger.info(f"No Token URI Found For Object With ID: {self.data['ledger_index']}")
            return
        content, content_type = await self.fetcher.fetch(token_uri)
        if content:
            meta_data = {}
            # ext = content_type.split("/")[1]
            file_type = content_type.split("/")[0]
            if file_type == "application":
                meta_data = json.loads(content)
                await self.writer.write_json(f"assets/metadata/{token_id}/metadata", meta_data)
                await self.writer.write_json(f"assets/metadata/{token_id}/metadata.json", meta_data)
            elif file_type == "image":
                meta_data = {
                    "image": token_uri
                }
                await self.writer.write_json(f"assets/metadata/{token_id}/metadata", meta_data)
                await self.writer.write_json(f"assets/metadata/{token_id}/metadata.json", meta_data)
                logger.info("Processing possible image metadata")
                await self._dump_image(content, token_id, content_type)
                return
            else:
                logger.info(f"Got FileType {file_type} For Metadata")
                await self.writer.write_media(f"assets/{file_type}/{token_id}/{file_type}", content, content_type)
            image_url = meta_data.get("image", meta_data.get("image_url"))
            video_url = meta_data.get("video", meta_data.get("video_url"))
            file_url = meta_data.get("file", meta_data.get("file_url"))
            animation_url = meta_data.get("animation", meta_data.get("animation_url"))
            audio_url = meta_data.get("audio", meta_data.get("audio_url"))
            thumbnail_url = meta_data.get("thumbnail", meta_data.get("thumbnail_url"))
            if image_url:
                image_content, content_type = await self.fetcher.fetch(image_url)
                if image_content is not None:
                    await self._dump_image(image_content, token_id, content_type)
            if video_url:  # noqa
                video_content, content_type = await self.fetcher.fetch(video_url)
                ext = content_type.split("/")[1]
                if video_content:
                    await self.writer.write_media(
                        f"assets/videos/{token_id}/video",
                        video_content,
                        content_type
                    )
                    await self.writer.write_media(
                        f"assets/videos/{token_id}/video.{ext}",
                        video_content,
                        content_type
                    )
            if file_url:
                logger.info(f"Found File URL: {file_url}")
            if audio_url:
                logger.info(f"Found Audio URL: {audio_url}")
            if thumbnail_url:
                logger.info(f"Found Thumbnail URL: {thumbnail_url}")
            if animation_url:  # noqa
                animation_content, content_type = await self.fetcher.fetch(animation_url)
                ext = content_type.split("/")[1]
                if animation_content:
                    await self.writer.write_media(
                        f"assets/animations/{token_id}/animation",
                        animation_content,
                        content_type
                    )
                    await self.writer.write_media(
                        f"assets/animations/{token_id}/animation.{ext}",
                        animation_content,
                        content_type
                    )

            logger.info(f"Completed dump for Token ID -> {token_id}\n")
        else:
            raise NoMetaDataException(f"Could Not Fetch Metadata for {token_uri}")

    async def run(self):
        logger.info(f"Running for transaction with hash -> {self.data['hash']}")
        token_id = self.token_id_extractor.extract()
        if token_id is None:
            logger.info(f"No Token ID For Transaction with hash: {self.data['hash']}")
            return
        token_uri = None
        if "URI" not in self.data:
            token_uri = DomainURIExtractor.extract(self.data, token_id)
        else:
            token_uri = self.token_uri_extractor.extract()
        await self._extract_assets(token_id, token_uri)

    async def retry(self, path):
        data = await read_json(Config.CACHE_FAILED_LOG_BUCKET, path, Config)
        print(data)
        if type(data) != dict:
            data = json.loads(data)
        self.token_uri_extractor.data = data
        token_id = data.get("NFTokenID", "none")
        try:
            if "URI" not in data:
                token_uri = DomainURIExtractor.extract(data, token_id)
            else:
                token_uri = self.token_uri_extractor.extract()
            await self._extract_assets(token_id, token_uri)
            self.writer.bucket = Config.CACHE_FAILED_LOG_BUCKET
            await delete_from_s3(Config.CACHE_FAILED_LOG_BUCKET, f"notfound/{token_id}.json", Config)
            await self.writer.write_json(f"done/{token_id}.json", {"URI": self.data["URI"], "NFTokenID": token_id})
        except Exception as e: # noqa
            logger.error(traceback.format_exc())
            self.writer.bucket = Config.CACHE_FAILED_LOG_BUCKET
            await delete_from_s3(Config.CACHE_FAILED_LOG_BUCKET, f"notfound/{token_id}.json", Config)
            await self.writer.write_json(
                f"error/{token_id}.json",
                {"URI": self.data.get("URI"), "NFTokenID": token_id, "error": traceback.format_exc(), **data}
            )


