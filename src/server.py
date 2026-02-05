"""
NoSQL Key-Value Store Server with clustering support
"""
import socket
import json
import threading
import logging
import os
import time
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path
import random
from enum import Enum

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class NodeRole(Enum):
    PRIMARY = "primary"
    SECONDARY = "secondary"


class KVStore:
    """Key-Value store with persistence, WAL, and replication support"""

    def __init__(self, data_dir: str = "data", node_id: int = 0, node_role: NodeRole = NodeRole.PRIMARY):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.node_id = node_id
        self.role = node_role
        self.data: Dict[str, Any] = {}
        self.lock = threading.RLock()
        
        # WAL (Write-Ahead Log) setup
        self.wal_file = self.data_dir / f"node_{node_id}_wal.log"
        self.data_file = self.data_dir / f"node_{node_id}_data.json"
        self.index_file = self.data_dir / f"node_{node_id}_index.json"
        self.fulltext_index_file = self.data_dir / f"node_{node_id}_fulltext.json"
        
        # Indexes
        self.value_index: Dict[str, List[str]] = {}  # value -> [keys]
        self.fulltext_index: Dict[str, List[str]] = {}  # word -> [keys]
        
        # Replication state
        self.replicas: List[Tuple[str, int]] = []  # List of (host, port) for replicas
        self.replication_offset = 0  # For master-less replication
        self.term = 0  # For leader election
        self.voted_for: Optional[int] = None  # For Raft voting
        self.last_heartbeat = time.time()
        
        # Load existing data
        self._load_data()
        self._load_indexes()
        
    def _load_data(self):
        """Load data from disk and replay WAL"""
        with self.lock:
            # Load main data file
            if self.data_file.exists():
                try:
                    with open(self.data_file, 'r') as f:
                        self.data = json.load(f)
                    logger.info(f"Loaded data from {self.data_file}: {len(self.data)} keys")
                except Exception as e:
                    logger.error(f"Error loading data: {e}")
            
            # Replay WAL
            if self.wal_file.exists():
                try:
                    with open(self.wal_file, 'r') as f:
                        for line in f:
                            if not line.strip():
                                continue
                            entry = json.loads(line)
                            self._replay_wal_entry(entry)
                    logger.info(f"Replayed WAL from {self.wal_file}")
                except Exception as e:
                    logger.error(f"Error replaying WAL: {e}")

    def _replay_wal_entry(self, entry: Dict):
        """Replay a WAL entry"""
        op = entry.get('op')
        key = entry.get('key')
        value = entry.get('value')
        
        if op == 'SET':
            self.data[key] = value
        elif op == 'DELETE':
            self.data.pop(key, None)

    def _load_indexes(self):
        """Load indexes from disk"""
        with self.lock:
            if self.index_file.exists():
                try:
                    with open(self.index_file, 'r') as f:
                        self.value_index = json.load(f)
                except Exception as e:
                    logger.error(f"Error loading index: {e}")
            
            if self.fulltext_index_file.exists():
                try:
                    with open(self.fulltext_index_file, 'r') as f:
                        self.fulltext_index = json.load(f)
                except Exception as e:
                    logger.error(f"Error loading fulltext index: {e}")

    def _write_wal(self, operation: str, key: str, value: Any = None):
        """Write operation to WAL synchronously (no simulation of failures)"""
        entry = {
            'op': operation,
            'key': key,
            'timestamp': datetime.now().isoformat()
        }
        if value is not None:
            entry['value'] = value
        
        try:
            with open(self.wal_file, 'a') as f:
                f.write(json.dumps(entry) + '\n')
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            logger.error(f"Error writing WAL: {e}")

    def _save_data(self, debug_mode: bool = False, fail_chance: float = 0.01):
        """Save data to disk with optional failure simulation"""
        if debug_mode and random.random() < fail_chance:
            logger.warning("Simulated write failure (debug mode)")
            return
        
        try:
            with open(self.data_file, 'w') as f:
                json.dump(self.data, f)
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            logger.error(f"Error saving data: {e}")

    def _save_indexes(self):
        """Save indexes to disk"""
        try:
            with open(self.index_file, 'w') as f:
                json.dump(self.value_index, f)
                f.flush()
                os.fsync(f.fileno())
            
            with open(self.fulltext_index_file, 'w') as f:
                json.dump(self.fulltext_index, f)
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            logger.error(f"Error saving indexes: {e}")

    def _update_value_index(self, key: str, old_value: Any = None, new_value: Any = None):
        """Update value index"""
        # Remove old value from index
        if old_value is not None:
            value_str = str(old_value)
            if value_str in self.value_index:
                self.value_index[value_str] = [k for k in self.value_index[value_str] if k != key]
                if not self.value_index[value_str]:
                    del self.value_index[value_str]
        
        # Add new value to index
        if new_value is not None:
            value_str = str(new_value)
            if value_str not in self.value_index:
                self.value_index[value_str] = []
            if key not in self.value_index[value_str]:
                self.value_index[value_str].append(key)

    def _update_fulltext_index(self, key: str, text: str = "", remove: bool = False):
        """Update full-text index with words from text"""
        # Simple word tokenization
        words = text.lower().split() if isinstance(text, str) else []
        
        if remove:
            # Remove key from all word indexes
            for word_list in self.fulltext_index.values():
                if key in word_list:
                    word_list.remove(key)
            # Clean up empty entries
            self.fulltext_index = {w: keys for w, keys in self.fulltext_index.items() if keys}
        else:
            # Add key to word indexes
            for word in words:
                if word not in self.fulltext_index:
                    self.fulltext_index[word] = []
                if key not in self.fulltext_index[word]:
                    self.fulltext_index[word].append(key)

    def set(self, key: str, value: Any, debug_mode: bool = False, fail_chance: float = 0.01) -> bool:
        """Set a key-value pair"""
        with self.lock:
            old_value = self.data.get(key)
            
            # Write to WAL first (synchronous, no failure simulation)
            self._write_wal('SET', key, value)
            
            # Update in-memory data
            self.data[key] = value
            
            # Update indexes
            self._update_value_index(key, old_value, value)
            self._update_fulltext_index(key, str(value))
            
            # Save to disk (with optional failure simulation)
            self._save_data(debug_mode, fail_chance)
            self._save_indexes()
            
            return True

    def get(self, key: str) -> Optional[Any]:
        """Get a value by key"""
        with self.lock:
            return self.data.get(key)

    def delete(self, key: str) -> bool:
        """Delete a key"""
        with self.lock:
            if key not in self.data:
                return False
            
            old_value = self.data[key]
            
            # Write to WAL
            self._write_wal('DELETE', key)
            
            # Delete from memory
            del self.data[key]
            
            # Update indexes
            self._update_value_index(key, old_value, None)
            self._update_fulltext_index(key, remove=True)
            
            # Save to disk
            self._save_data()
            self._save_indexes()
            
            return True

    def bulk_set(self, items: List[Tuple[str, Any]], debug_mode: bool = False, fail_chance: float = 0.01) -> bool:
        """Set multiple key-value pairs atomically"""
        with self.lock:
            # Write all to WAL first (synchronous)
            for key, value in items:
                self._write_wal('SET', key, value)
            
            # Update all in-memory data
            for key, value in items:
                old_value = self.data.get(key)
                self.data[key] = value
                self._update_value_index(key, old_value, value)
                self._update_fulltext_index(key, str(value))
            
            # Save to disk (with optional failure simulation)
            self._save_data(debug_mode, fail_chance)
            self._save_indexes()
            
            return True

    def search_by_value(self, value: Any) -> List[str]:
        """Search keys by exact value"""
        with self.lock:
            value_str = str(value)
            return self.value_index.get(value_str, [])

    def search_fulltext(self, word: str) -> List[str]:
        """Search keys by word in value"""
        with self.lock:
            word = word.lower()
            return self.fulltext_index.get(word, [])

    def get_all(self) -> Dict[str, Any]:
        """Get all data"""
        with self.lock:
            return dict(self.data)

    def clear(self):
        """Clear all data"""
        with self.lock:
            self.data.clear()
            self.value_index.clear()
            self.fulltext_index.clear()
            self._save_data()
            self._save_indexes()


class KVServer:
    """TCP Server for Key-Value store with clustering support"""

    def __init__(self, host: str = "localhost", port: int = 5000, node_id: int = 0, data_dir: str = "data"):
        self.host = host
        self.port = port
        self.node_id = node_id
        self.socket = None
        self.running = False
        self.store = KVStore(data_dir=data_dir, node_id=node_id)
        self.clients = []
        self.client_lock = threading.Lock()

    def start(self):
        """Start the server"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, self.port))
        self.socket.listen(5)
        self.running = True
        logger.info(f"Server started on {self.host}:{self.port} (Node {self.node_id})")
        
        try:
            while self.running:
                try:
                    client_socket, address = self.socket.accept()
                    logger.info(f"Client connected: {address}")
                    with self.client_lock:
                        self.clients.append(client_socket)
                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, address),
                        daemon=True
                    )
                    client_thread.start()
                except Exception as e:
                    if self.running:
                        logger.error(f"Error accepting connection: {e}")
        except KeyboardInterrupt:
            logger.info("Server interrupted")
        finally:
            self.stop()

    def _handle_client(self, client_socket: socket.socket, address: Tuple):
        """Handle a client connection"""
        try:
            while self.running:
                data = client_socket.recv(4096)
                if not data:
                    break
                
                try:
                    request = json.loads(data.decode('utf-8'))
                    response = self._process_request(request)
                    client_socket.sendall(json.dumps(response).encode('utf-8'))
                except Exception as e:
                    logger.error(f"Error processing request: {e}")
                    response = {'status': 'error', 'message': str(e)}
                    client_socket.sendall(json.dumps(response).encode('utf-8'))
        except Exception as e:
            logger.error(f"Client error: {e}")
        finally:
            client_socket.close()
            with self.client_lock:
                if client_socket in self.clients:
                    self.clients.remove(client_socket)
            logger.info(f"Client disconnected: {address}")

    def _process_request(self, request: Dict) -> Dict:
        """Process a client request"""
        command = request.get('command')
        
        if command == 'SET':
            key = request.get('key')
            value = request.get('value')
            debug_mode = request.get('debug_mode', False)
            fail_chance = request.get('fail_chance', 0.01)
            success = self.store.set(key, value, debug_mode, fail_chance)
            return {'status': 'ok' if success else 'error', 'result': success}
        
        elif command == 'GET':
            key = request.get('key')
            value = self.store.get(key)
            return {'status': 'ok', 'result': value}
        
        elif command == 'DELETE':
            key = request.get('key')
            success = self.store.delete(key)
            return {'status': 'ok' if success else 'error', 'result': success}
        
        elif command == 'BULK_SET':
            items = request.get('items', [])
            debug_mode = request.get('debug_mode', False)
            fail_chance = request.get('fail_chance', 0.01)
            success = self.store.bulk_set(items, debug_mode, fail_chance)
            return {'status': 'ok' if success else 'error', 'result': success}
        
        elif command == 'SEARCH_VALUE':
            value = request.get('value')
            result = self.store.search_by_value(value)
            return {'status': 'ok', 'result': result}
        
        elif command == 'SEARCH_FULLTEXT':
            word = request.get('word')
            result = self.store.search_fulltext(word)
            return {'status': 'ok', 'result': result}
        
        elif command == 'GET_ALL':
            result = self.store.get_all()
            return {'status': 'ok', 'result': result}
        
        else:
            return {'status': 'error', 'message': f'Unknown command: {command}'}

    def stop(self):
        """Stop the server"""
        self.running = False
        with self.client_lock:
            for client in self.clients:
                try:
                    client.close()
                except:
                    pass
            self.clients.clear()
        if self.socket:
            self.socket.close()
        logger.info("Server stopped")


if __name__ == "__main__":
    server = KVServer(host="localhost", port=5000, node_id=0)
    server.start()
