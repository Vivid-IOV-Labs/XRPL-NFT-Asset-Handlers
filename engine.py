import json
import logging

from writers import AsyncS3FileWriter
from fetcher import Fetcher
from image_processor import process_image
from extractors import TokenIDExtractor, TokenURIExtractor

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
        await self.writer.write_image(f"assets/images /{token_id}/200px/image", resized, "image/png")
        await self.writer.write_image(f"assets/images/{token_id}/full/image", full, content_type)

    async def run(self):
        logger.info(f"Running for transaction with hash -> {self.data['hash']}")
        token_id = self.token_id_extractor.extract()
        if token_id is None:
            logger.info(f"No Token ID For Transaction with hash: {self.data['hash']}")
            return
        token_uri = self.token_uri_extractor.extract()
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
                await self.writer.write_json(f"assets/metadata/{token_id}/metadata.json", meta_data)
            elif file_type == "image":
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
            if video_url:
                logger.info(f"Found Video URL: {video_url}")
            if file_url:
                logger.info(f"Found File URL: {file_url}")
            if audio_url:
                logger.info(f"Found Audio URL: {audio_url}")
            if thumbnail_url:
                logger.info(f"Found Thumbnail URL: {thumbnail_url}")
            if animation_url:
                animation_content, content_type = await self.fetcher.fetch(animation_url)
                if animation_content:
                    await self.writer.write_media(
                        f"assets/animations/{token_id}/animation",
                        animation_content,
                        content_type
                    )

            logger.info(f"Completed dump for transaction with hash -> {self.data['hash']}\n")
        else:
            logger.info(f"Could Not Fetch Metadata for {token_uri}")
