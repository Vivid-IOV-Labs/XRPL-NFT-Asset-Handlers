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
    CACHE_FAILED_LOG_BUCKET = os.getenv("CACHE_FAILED_LOG_BUCKET")
    BITHOMP_TOKEN = os.getenv("BITHOMP_TOKEN")
    DB_HOST = os.getenv("DB_HOST")
    RDS_PASSWORD = os.getenv("RDS_PASSWORD")
    RDS_USER = os.getenv("RDS_USER")
    RDS_PORT = os.getenv("RDS_PORT")
    DB_NAME = os.getenv("DB_NAME")
    IPFS_HOSTS = [
        "https://nftstorage.link/ipfs",
        "https://gateway.pinata.cloud/ipfs",
        "https://cloudflare-ipfs.com/ipfs",
        "https://dweb.link/ipfs"
    ]
