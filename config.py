import os
from dataclasses import dataclass


try:
    from dotenv import load_dotenv
    load_dotenv(".env")
except ModuleNotFoundError:
    pass


@dataclass
class Config:
    ENVIRONMENT = os.getenv("ENVIRONMENT")
    ACCESS_KEY_ID = os.getenv("ACC_K_ID")
    SECRET_ACCESS_KEY = os.getenv("ASC_KY")
    DATA_DUMP_BUCKET = os.getenv("DATA_DUMP_BUCKET")
    NFT_MINT_DUMP_BUCKET = os.getenv("NFT_MINT_DUMP_BUCKET")
    IPFS_HOSTS = [
        "https://nftstorage.link/ipfs",
        "https://gateway.pinata.cloud/ipfs",
        "https://cloudflare-ipfs.com/ipfs",
        "https://dweb.link/ipfs"
    ]
