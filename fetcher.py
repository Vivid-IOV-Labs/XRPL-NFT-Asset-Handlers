from typing import Optional, Tuple
import aiohttp
import asyncio
from config import Config
from utils import is_ipfs, get_path_from_ipfs_url
import logging

logger = logging.getLogger("app_log")


class Fetcher:

    def __init__(self) -> None:
        self.ipfs_hosts = Config.IPFS_HOSTS

    async def _fetch(self, url) -> Tuple[Optional[bytes], Optional[str]]:  # noqa
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.content.read()
                    content_type = response.headers["Content-Type"]
                    return content, content_type
                logger.error(f"Fetch Failed for {url}")
                await asyncio.sleep(3)
                return None, None

    async def _fetch_from_ipfs(self, ipfs_hash: str, host: str) -> Tuple[Optional[bytes], Optional[str]]:
        response = await self._fetch(f"{host}/{ipfs_hash}")
        return response

    async def fetch(self, url: str) -> Tuple[Optional[bytes], Optional[str]]:
        if is_ipfs(url):
            ipfs_path = get_path_from_ipfs_url(url)
            done, _ = await asyncio.wait(
                [
                    self._fetch_from_ipfs(ipfs_path, host) for host in self.ipfs_hosts
                ],
                return_when=asyncio.FIRST_COMPLETED
            )
            result = None
            for task in done:
                if task.result() is not None:
                    result = task.result()
            return result
        else:
            response = await self._fetch(url)
            return response
