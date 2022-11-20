import json
from typing import List, Dict, Optional
import logging
from utils import hex_to_text, is_ipfs, is_normal_url


logger = logging.getLogger("app_log")


class TokenIDExtractor:
    def __init__(self, data: Dict):
        self.data = data

    @property
    def affected_nodes(self):
        return self.data["metaData"]["AffectedNodes"]

    def _get_token_page_modified_nodes(self) -> List[Dict]:
        modified_nodes = [node for node in self.affected_nodes if node.get("ModifiedNode", False)]
        if modified_nodes:
            return [node for node in modified_nodes if node["ModifiedNode"]["LedgerEntryType"] == "NFTokenPage"]
        return []

    def _get_token_page_created_nodes(self) -> List[Dict]:
        created_nodes = [node for node in self.affected_nodes if node.get("CreatedNode", False)]
        return [node for node in created_nodes if node["CreatedNode"]["LedgerEntryType"] == "NFTokenPage"]

    def _get_nft_tokens_from_modified_nodes(self) -> List[Dict]:
        modified_nodes = self._get_token_page_modified_nodes()
        result = []
        for node in modified_nodes:
            result.extend(node["ModifiedNode"]["FinalFields"]["NFTokens"])
            if node["ModifiedNode"]["PreviousFields"].get("NFTokens"):
                result.extend(node["ModifiedNode"]["PreviousFields"].get("NFTokens"))
        return result

    def _get_nft_tokens_from_created_nodes(self) -> List[Dict]:
        created_nodes = self._get_token_page_created_nodes()
        result = []
        for node in created_nodes:
            result.extend(node["CreatedNode"]["NewFields"]["NFTokens"])
        return result

    def _get_all_nft_tokens(self) -> List[Dict]:
        modified_nodes_tokens = self._get_nft_tokens_from_modified_nodes()
        created_nodes_tokens = self._get_nft_tokens_from_created_nodes()
        return modified_nodes_tokens + created_nodes_tokens

    def extract(self) -> Optional[str]:
        if self.data["metaData"]["TransactionResult"] != "tesSUCCESS":
            return None
        try:
            nft_tokens = self._get_all_nft_tokens()
            hash_map = {}
            for token in nft_tokens:
                token_hash = hash(str(token))
                if not hash_map.get(token_hash):
                    hash_map[token_hash] = {"count": 1, "token": token}
                else:
                    hash_map[token_hash]["count"] += 1
            target_tokens = [item["token"] for item in hash_map.values() if item["count"] == 1]
            if len(target_tokens) > 1:
                logger.info("Multiple NFTokens Returned")
                return None
            return target_tokens[0]["NFToken"]["NFTokenID"]
        except Exception as e:
            logger.error(f"Error getting nft-token-id: {e}")
        return None


class TokenURIExtractor:
    def __init__(self, data: Dict):
        self.data = data

    def extract(self):
        try:
            token_uri_hex = self.data["URI"]
            token_uri = hex_to_text(token_uri_hex)
            if is_ipfs(token_uri) is not True and is_normal_url(token_uri) is not True:
                return f"ipfs://{token_uri}"
            return token_uri
        except KeyError:
            logger.error("URI Field not present in data")
            return None
