from typing import Optional
import aiohttp
import asyncio
from config import Config
from utils import is_ipfs, get_path_from_ipfs_url
import logging

logger = logging.getLogger("app_log")


class Fetcher:

    def __init__(self) -> None:
        self.ipfs_hosts = Config.IPFS_HOSTS

    async def _fetch(self, url) -> Optional[bytes]:  # noqa
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.content.read()
                    return content
                logger.error(f"Fetch Failed for {url}")
                await asyncio.sleep(3)
                return None

    async def _fetch_from_ipfs(self, ipfs_hash: str, host: str) -> Optional[bytes]:
        response = await self._fetch(f"{host}/{ipfs_hash}")
        return response

    async def fetch(self, url: str) -> Optional[bytes]:
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
