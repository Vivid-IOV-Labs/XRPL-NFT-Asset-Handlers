from typing import Optional

def hex_to_text(hex_str: str) -> str:
    return bytes.fromhex(hex_str).decode('utf-8')


def is_ipfs(url: str):
    return True if url.startswith("ipfs://") else False


def is_normal_url(url: str):
    return True if url.startswith("https://") else False


def get_path_from_ipfs_url(url: str):
    return url.replace("ipfs://", "").replace("ipfs/", "")


def get_file_extension(file_path: str) -> Optional[str]:
    split = file_path.split(".")
    return split[-1] if split else None
