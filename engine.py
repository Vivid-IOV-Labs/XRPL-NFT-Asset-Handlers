import json
import logging
import asyncio
import traceback
from abc import ABCMeta, abstractmethod

from writers import AsyncS3FileWriter, Config
from utils import delete_from_s3, read_json
from fetcher import Fetcher
from image_processor import process_image
from extractors import TokenIDExtractor, TokenURIExtractor, DomainURIExtractor
from exceptions import NoMetaDataException

logger = logging.getLogger("app_log")

class BaseAssetExtractionEngine(metaclass=ABCMeta):
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

    async def _dump_metadata(self, data, token_id):
        await self.writer.write_json(f"assets/metadata/{token_id}/metadata", data)
        await self.writer.write_json(f"assets/metadata/{token_id}/metadata.json", data)

    async def _dump_file(self, file_type, token_id, content, content_type, extension):
        await self.writer.write_media(f"assets/{file_type}s/{token_id}/{file_type}", content, content_type)
        await self.writer.write_media(f"assets/{file_type}s/{token_id}/{file_type}.{extension}", content, content_type)

    async def _extract_assets(self, token_id, token_uri):
        if token_uri is None:
            logger.info(f"No Token URI Found For Object With ID: {self.data['ledger_index']}")
            return
        content, content_type = await self.fetcher.fetch(token_uri)
        if content:
            meta_data = {}
            file_type = content_type.split("/")[0]
            if file_type == "application" or file_type == "text":
                meta_data = json.loads(content)
                content_exists = meta_data.get("content")
                if content_exists:
                    content, content_type = await self.fetcher.fetch(content_exists.replace("cid:", ""))
                    file_type = content_type.split("/")[0]
                    if file_type == "image":
                        await self._dump_image(content, token_id, content_type)
                    else:
                        ext = content_type.split("/")[1]
                        await self._dump_file(file_type, token_id, content, content_type, ext)
                await self._dump_metadata(meta_data, token_id)
            elif file_type == "image":
                meta_data = {
                    "image": token_uri
                }
                await self._dump_metadata(meta_data, token_id)
                logger.info("Processing possible image metadata")
                await self._dump_image(content, token_id, content_type)
                return
            else:
                logger.info(f"Got FileType {file_type} For Metadata")
                meta_data = {
                    file_type: token_uri
                }
                ext = content_type.split("/")[1]
                await self._dump_metadata(meta_data, token_id)
                await self._dump_file(file_type, token_id, content, content_type, ext)

            # Search for Other Assets in the MetaData Json and Upload to s3
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
                    await self._dump_file("video", token_id, video_content, content_type, ext)
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
                    await self._dump_file("animation", token_id, animation_content, content_type, ext)
            logger.info(f"Completed dump for Token ID -> {token_id}\n")
        else:
            raise NoMetaDataException(f"Could Not Fetch Metadata for {token_uri}")

    @abstractmethod
    async def _run(self):
        ...

    def run(self):
        asyncio.run(self._run())

class AssetExtractionEngine(BaseAssetExtractionEngine):
    async def _run(self):
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

class RetryEngine(BaseAssetExtractionEngine):
    def __init__(self, data=None, path=None):
        if path is None and data is None:
            raise Exception("Either path or data must be specified")
        super().__init__(data={"URI": ""} if data is None else data)
        self.path = path

    async def _run(self):
        data = await read_json(Config.CACHE_FAILED_LOG_BUCKET, self.path, Config) if self.path is not None else self.data
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
            await self.writer.write_json(f"done/{token_id}.json", {"URI": token_uri, "NFTokenID": token_id})
        except Exception as e: # noqa
            logger.error(traceback.format_exc())
            self.writer.bucket = Config.CACHE_FAILED_LOG_BUCKET
            await delete_from_s3(Config.CACHE_FAILED_LOG_BUCKET, f"notfound/{token_id}.json", Config)
            await self.writer.write_json(
                f"error/{token_id}.json",
                {"URI": self.data.get("URI"), "NFTokenID": token_id, "error": traceback.format_exc(), **data}
            )


class PublicRetryEngine(BaseAssetExtractionEngine):
    def __init__(self, token_id, data=None):
        super().__init__(data={"URI": ""})
        self.token_id = token_id

    async def _run(self):
        base_url = "https://bithomp.com/api/v2/nft"
        url = f"{base_url}/{self.token_id}?uri=true"
        response, _content_type = await self.fetcher.fetch(url, headers={"x-bithomp-token": Config.BITHOMP_TOKEN})
        response = json.loads(response)
        token_uri = response["uri"]
        self.token_uri_extractor.data = {"URI": token_uri}
        token_uri = self.token_uri_extractor.extract()
        try:
            await self._extract_assets(self.token_id, token_uri)
            self.writer.bucket = Config.CACHE_FAILED_LOG_BUCKET
            await self.writer.write_json(f"publicapinotfound/done/{self.token_id}.json", {"URI": token_uri, "NFTokenID": self.token_id})
        except Exception as e:  # noqa
            logger.error(traceback.format_exc())
            error = {"token_id": self.token_id, "uri": token_uri, "error": str(e)}
            self.writer.bucket = Config.CACHE_FAILED_LOG_BUCKET
            await self.writer.write_json(f"publicapinotfound/error/{self.token_id}.json", error)


# class NFTProjectsRetry:
#     async def retry_v2(self, token_id, token_uri, issuer, completed, errors):
#         logger.info(f"starting retry for {token_id}")
#         if type(token_uri) == float or token_uri is None:
#             data = {"Issuer": issuer}
#             token_uri = await DomainURIExtractor.async_extract(data, token_id)
#         else:
#             try:
#                 token_uri = TokenURIExtractor({"URI": token_uri}).extract()
#             except ValueError:
#                 token_uri = token_uri
#         try:
#             await self._extract_assets(token_id, token_uri)
#         except Exception as e:
#             logger.error(e)
#             errors.append({"token_id": token_id, "issuer": issuer, "uri": token_uri, "error": str(e)})
#         completed[token_id] = True
#         logger.error(f"completed retry for {token_id}")
