from typing import Dict
import json
import logging

from writers import AsyncS3FileWriter
from fetcher import Fetcher
from image import process_image
from utils import get_file_extension
from extractors import TokenIDExtractor, TokenURIExtractor

logger = logging.getLogger("app_log")


class Engine:
    def __init__(self, data: Dict):
        self.data = data
        self.writer = AsyncS3FileWriter()
        self.fetcher = Fetcher()
        self.token_id_extractor = TokenIDExtractor(data)
        self.token_uri_extractor = TokenURIExtractor(data)

    async def _dump_image(self, content: bytes, token_id: str):
        full, resized = process_image(content)
        await self.writer.write_image(f"assets/images/{token_id}/200px/image.jpg", resized)
        await self.writer.write_image(f"assets/images/{token_id}/full/image.jpg", full)

    async def run(self):
        logger.info(f"Running for object with id -> {self.data['_id']['$oid']}")
        token_id = self.token_id_extractor.extract()
        if token_id is None:
            logger.info(f"No Token ID Found For Object With ID: {self.data['_id']['$oid']}")
            return
        token_uri = self.token_uri_extractor.extract()
        if token_uri is None:
            logger.info(f"No Token URI Found For Object With ID: {self.data['_id']['$oid']}")
            return
        meta_data = await self.fetcher.fetch(token_uri)
        if meta_data:
            try:
                meta_data = json.loads(meta_data)
            except UnicodeDecodeError:
                logger.info("Processing possible image metadata")
                await self._dump_image(meta_data, token_id)
                return
            image_url = meta_data.get("image", meta_data.get("image_url"))
            video_url = meta_data.get("video", meta_data.get("video_url"))
            file_url = meta_data.get("file", meta_data.get("file_url"))
            animation_url = meta_data.get("animation", meta_data.get("animation_url"))
            audio_url = meta_data.get("audio", meta_data.get("audio_url"))
            thumbnail_url = meta_data.get("thumbnail", meta_data.get("thumbnail_url"))
            await self.writer.write_json(f"assets/metadata/{token_id}/metadata.json", meta_data)
            if image_url:
                image_content = await self.fetcher.fetch(image_url)
                if image_content is not None:
                    full, resized = process_image(image_content)
                    await self.writer.write_image(f"assets/images/{token_id}/200px/image.jpg", resized)
                    await self.writer.write_image(f"assets/images/{token_id}/full/image.jpg", full)
            if video_url:
                logger.info(f"Found Video URL: {video_url}")
            if file_url:
                logger.info(f"Found File URL: {file_url}")
            if audio_url:
                logger.info(f"Found Audio URL: {audio_url}")
            if thumbnail_url:
                logger.info(f"Found Thumbnail URL: {thumbnail_url}")
            if animation_url:
                animation_content = await self.fetcher.fetch(animation_url)
                if animation_content:
                    animation_file_ext = get_file_extension(animation_url)
                    await self.writer.write_animation(
                        f"assets/animations/{token_id}/animation.{animation_file_ext}",
                        animation_content
                    )

            logger.info(f"Completed dump for object with id -> {self.data['_id']['$oid']}\n")
        else:
            logger.info(f"Could Not Fetch Metadata for {token_uri}")
