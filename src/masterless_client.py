"""
Client for master-less key-value store with semantic search support
"""
import socket
import json
import logging
from typing import Any, Optional, List, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MasterlessClient:
    """Client for master-less distributed KV store"""

    def __init__(self, host: str = "localhost", port: int = 7000):
        self.host = host
        self.port = port
        self.socket = None
        self._connect()

    def _connect(self):
        """Connect to server"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            logger.info(f"Connected to {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Connection error: {e}")
            raise

    def _send_request(self, request: dict) -> dict:
        """Send request to server"""
        try:
            if not self.socket:
                self._connect()
            
            self.socket.sendall(json.dumps(request).encode('utf-8'))
            response = self.socket.recv(4096).decode('utf-8')
            return json.loads(response)
        except Exception as e:
            logger.error(f"Request error: {e}")
            self.socket = None
            raise

    def set(self, key: str, value: Any, debug_mode: bool = False, fail_chance: float = 0.01) -> bool:
        """Set a key-value pair"""
        request = {
            'command': 'SET',
            'key': key,
            'value': value,
            'debug_mode': debug_mode,
            'fail_chance': fail_chance
        }
        response = self._send_request(request)
        return response.get('status') == 'ok' and response.get('result')

    def get(self, key: str) -> Optional[Any]:
        """Get a value by key"""
        request = {
            'command': 'GET',
            'key': key
        }
        response = self._send_request(request)
        return response.get('result')

    def delete(self, key: str) -> bool:
        """Delete a key (using tombstone)"""
        request = {
            'command': 'DELETE',
            'key': key
        }
        response = self._send_request(request)
        return response.get('status') == 'ok' and response.get('result')

    def bulk_set(self, items: List[Tuple[str, Any]], debug_mode: bool = False, fail_chance: float = 0.01) -> bool:
        """Set multiple items"""
        request = {
            'command': 'BULK_SET',
            'items': items,
            'debug_mode': debug_mode,
            'fail_chance': fail_chance
        }
        response = self._send_request(request)
        return response.get('status') == 'ok' and response.get('result')

    def search_by_value(self, value: Any) -> List[str]:
        """Search by exact value"""
        request = {
            'command': 'SEARCH_VALUE',
            'value': value
        }
        response = self._send_request(request)
        return response.get('result', [])

    def search_fulltext(self, word: str) -> List[str]:
        """Full-text search"""
        request = {
            'command': 'SEARCH_FULLTEXT',
            'word': word
        }
        response = self._send_request(request)
        return response.get('result', [])

    def search_semantic(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """Semantic search using word embeddings"""
        request = {
            'command': 'SEARCH_SEMANTIC',
            'query': query,
            'top_k': top_k
        }
        response = self._send_request(request)
        return response.get('result', [])

    def get_all(self) -> dict:
        """Get all non-deleted data"""
        request = {'command': 'GET_ALL'}
        response = self._send_request(request)
        return response.get('result', {})

    def get_metadata(self) -> dict:
        """Get node metadata"""
        request = {'command': 'GET_METADATA'}
        response = self._send_request(request)
        return response.get('result', {})

    def close(self):
        """Close connection"""
        if self.socket:
            self.socket.close()
            self.socket = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
