"""
Master-less (peer-to-peer) replication with eventual consistency
Supports word embeddings for semantic search
"""
import socket
import json
import threading
import logging
import os
import time
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional, Set
from pathlib import Path
import random
from dataclasses import dataclass, asdict
import hashlib
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


@dataclass
class VectorClock:
    """Vector clock for causal ordering in distributed system"""
    clock: Dict[int, int]
    
    def increment(self, node_id: int):
        """Increment clock for this node"""
        self.clock[node_id] = self.clock.get(node_id, 0) + 1
    
    def happens_before(self, other: 'VectorClock') -> bool:
        """Check if this clock happens before other"""
        less = False
        for node_id in set(list(self.clock.keys()) + list(other.clock.keys())):
            my_time = self.clock.get(node_id, 0)
            other_time = other.clock.get(node_id, 0)
            if my_time > other_time:
                return False
            if my_time < other_time:
                less = True
        return less
    
    def merge(self, other: 'VectorClock'):
        """Merge with another clock"""
        for node_id, timestamp in other.clock.items():
            self.clock[node_id] = max(self.clock.get(node_id, 0), timestamp)


class SimpleEmbedding:
    """Simple word embedding using character n-grams"""
    
    @staticmethod
    def embed(text: str, dim: int = 50) -> np.ndarray:
        """Convert text to embedding using hash-based method"""
        # Use multiple hash functions to create embedding
        embedding = np.zeros(dim)
        text_lower = text.lower().strip()
        
        if not text_lower:
            return embedding
        
        # Create trigrams
        trigrams = [text_lower[i:i+3] for i in range(len(text_lower)-2)]
        if not trigrams:
            trigrams = [text_lower]
        
        # Hash each trigram to embedding dimensions
        for trigram in trigrams:
            hash_val = int(hashlib.md5(trigram.encode()).hexdigest(), 16)
            for dim_idx in range(dim):
                embedding[dim_idx] += (hash_val >> dim_idx) & 1
        
        # Normalize
        norm = np.linalg.norm(embedding)
        if norm > 0:
            embedding /= norm
        
        return embedding
    
    @staticmethod
    def similarity(embedding1: np.ndarray, embedding2: np.ndarray) -> float:
        """Calculate cosine similarity between embeddings"""
        dot_product = np.dot(embedding1, embedding2)
        norm1 = np.linalg.norm(embedding1)
        norm2 = np.linalg.norm(embedding2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return float(dot_product / (norm1 * norm2))


class MasterlessNode:
    """Peer node in master-less replication system"""

    def __init__(self, node_id: int, host: str, port: int, data_dir: str = "masterless_data",
                 peers: List[Tuple[int, str, int]] = None):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        self.logger = logging.getLogger(f"Node-{node_id}")
        
        # Data storage with version vectors
        self.data: Dict[str, Dict[str, Any]] = {}  # key -> {value, vc, timestamp}
        self.lock = threading.RLock()
        
        # Persistent storage
        self.data_file = self.data_dir / f"node_{node_id}_data.json"
        self.wal_file = self.data_dir / f"node_{node_id}_wal.log"
        self.embedding_file = self.data_dir / f"node_{node_id}_embeddings.json"
        
        # Indexes
        self.value_index: Dict[str, List[str]] = {}
        self.fulltext_index: Dict[str, List[str]] = {}
        self.embedding_index: Dict[str, List[float]] = {}  # key -> embedding as list
        
        # Vector clock for causal ordering
        self.vector_clock = VectorClock({node_id: 0})
        
        # Peers in the cluster
        self.peers = peers or []
        self.peer_lock = threading.Lock()
        self.peer_vector_clocks: Dict[int, VectorClock] = {}
        
        # Conflict resolution
        self.last_write: Dict[str, Tuple[VectorClock, int]] = {}  # key -> (vc, timestamp)
        
        # Load existing data
        self._load_data()
        self._load_embeddings()

    def _load_data(self):
        """Load data with version vectors"""
        with self.lock:
            if self.data_file.exists():
                try:
                    with open(self.data_file, 'r') as f:
                        raw_data = json.load(f)
                        for key, item in raw_data.items():
                            vc_dict = item.get('vc', {self.node_id: 0})
                            self.data[key] = {
                                'value': item.get('value'),
                                'vc': VectorClock(vc_dict),
                                'timestamp': item.get('timestamp', time.time())
                            }
                    self.logger.info(f"Loaded {len(self.data)} keys")
                except Exception as e:
                    self.logger.error(f"Error loading data: {e}")

    def _save_data(self):
        """Save data with version vectors"""
        try:
            with open(self.data_file, 'w') as f:
                export_data = {}
                for key, item in self.data.items():
                    export_data[key] = {
                        'value': item['value'],
                        'vc': item['vc'].clock,
                        'timestamp': item['timestamp']
                    }
                json.dump(export_data, f)
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")

    def _load_embeddings(self):
        """Load word embeddings"""
        with self.lock:
            if self.embedding_file.exists():
                try:
                    with open(self.embedding_file, 'r') as f:
                        self.embedding_index = json.load(f)
                except Exception as e:
                    self.logger.error(f"Error loading embeddings: {e}")

    def _save_embeddings(self):
        """Save word embeddings"""
        try:
            with open(self.embedding_file, 'w') as f:
                json.dump(self.embedding_index, f)
        except Exception as e:
            self.logger.error(f"Error saving embeddings: {e}")

    def _write_wal(self, operation: str, key: str, value: Any = None):
        """Write to WAL"""
        entry = {
            'op': operation,
            'key': key,
            'timestamp': datetime.now().isoformat(),
            'vc': self.vector_clock.clock
        }
        if value is not None:
            entry['value'] = value
        
        try:
            with open(self.wal_file, 'a') as f:
                f.write(json.dumps(entry) + '\n')
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            self.logger.error(f"Error writing WAL: {e}")

    def _resolve_conflict(self, key: str, existing_vc: VectorClock, new_vc: VectorClock, 
                         existing_ts: int, new_ts: int) -> bool:
        """
        Resolve conflict between two versions
        Returns True if new version should win
        """
        # If one happens before the other, use that ordering
        if new_vc.happens_before(existing_vc):
            return False
        if existing_vc.happens_before(new_vc):
            return True
        
        # Concurrent writes - use timestamp as tiebreaker
        if new_ts > existing_ts:
            return True
        elif new_ts < existing_ts:
            return False
        
        # Same timestamp - use node ID
        return True  # Accept latest

    def set(self, key: str, value: Any, debug_mode: bool = False, fail_chance: float = 0.01) -> bool:
        """Set a key-value pair with version vector"""
        if debug_mode and random.random() < fail_chance:
            self.logger.warning("Simulated write failure (debug mode)")
            return False
        
        with self.lock:
            # Increment vector clock
            self.vector_clock.increment(self.node_id)
            current_time = int(time.time() * 1000)
            
            # Check for conflicts
            if key in self.data:
                existing_vc = self.data[key]['vc']
                existing_ts = int(self.data[key]['timestamp'] * 1000)
                
                if not self._resolve_conflict(key, existing_vc, self.vector_clock, 
                                             existing_ts, current_time):
                    return False
            
            # Store with version vector
            old_value = self.data.get(key, {}).get('value') if key in self.data else None
            self.data[key] = {
                'value': value,
                'vc': VectorClock(dict(self.vector_clock.clock)),
                'timestamp': current_time
            }
            
            # Update indexes
            self._update_indexes(key, old_value, value)
            
            # Persist
            self._write_wal('SET', key, value)
            self._save_data()
            self._save_embeddings()
            
            # Replicate to peers
            self._replicate_to_peers(key, value, 'SET', self.vector_clock)
            
            return True

    def get(self, key: str) -> Optional[Any]:
        """Get a value by key"""
        with self.lock:
            if key in self.data:
                return self.data[key]['value']
            return None

    def delete(self, key: str) -> bool:
        """Delete a key using tombstone (for eventual consistency)"""
        with self.lock:
            if key not in self.data:
                return False
            
            # Increment vector clock
            self.vector_clock.increment(self.node_id)
            
            old_value = self.data[key]['value']
            
            # Use tombstone (value=None with VC)
            self.data[key] = {
                'value': None,
                'vc': VectorClock(dict(self.vector_clock.clock)),
                'timestamp': int(time.time() * 1000)
            }
            
            # Update indexes
            self._update_indexes(key, old_value, None, is_delete=True)
            
            # Persist
            self._write_wal('DELETE', key)
            self._save_data()
            self._save_embeddings()
            
            # Replicate
            self._replicate_to_peers(key, None, 'DELETE', self.vector_clock)
            
            return True

    def bulk_set(self, items: List[Tuple[str, Any]], debug_mode: bool = False, fail_chance: float = 0.01) -> bool:
        """Bulk set with vector clocks"""
        if debug_mode and random.random() < fail_chance:
            self.logger.warning("Simulated bulk write failure (debug mode)")
            return False
        
        with self.lock:
            for key, value in items:
                self.vector_clock.increment(self.node_id)
                current_time = int(time.time() * 1000)
                
                old_value = self.data.get(key, {}).get('value') if key in self.data else None
                self.data[key] = {
                    'value': value,
                    'vc': VectorClock(dict(self.vector_clock.clock)),
                    'timestamp': current_time
                }
                
                self._update_indexes(key, old_value, value)
                self._write_wal('SET', key, value)
            
            self._save_data()
            self._save_embeddings()
            
            # Replicate all at once
            for key, value in items:
                self._replicate_to_peers(key, value, 'SET', self.vector_clock)
            
            return True

    def _update_indexes(self, key: str, old_value: Any = None, new_value: Any = None, is_delete: bool = False):
        """Update all indexes"""
        # Value index
        if old_value is not None:
            value_str = str(old_value)
            if value_str in self.value_index:
                self.value_index[value_str] = [k for k in self.value_index[value_str] if k != key]
                if not self.value_index[value_str]:
                    del self.value_index[value_str]
        
        if new_value is not None and not is_delete:
            value_str = str(new_value)
            if value_str not in self.value_index:
                self.value_index[value_str] = []
            if key not in self.value_index[value_str]:
                self.value_index[value_str].append(key)
        
        # Full-text index
        if is_delete:
            for word_list in self.fulltext_index.values():
                if key in word_list:
                    word_list.remove(key)
            self.fulltext_index = {w: keys for w, keys in self.fulltext_index.items() if keys}
        else:
            words = str(new_value).lower().split() if new_value else []
            for word in words:
                if word not in self.fulltext_index:
                    self.fulltext_index[word] = []
                if key not in self.fulltext_index[word]:
                    self.fulltext_index[word].append(key)
        
        # Embedding index
        if new_value and not is_delete:
            text = str(new_value)
            embedding = SimpleEmbedding.embed(text)
            self.embedding_index[key] = embedding.tolist()
        elif is_delete and key in self.embedding_index:
            del self.embedding_index[key]

    def _replicate_to_peers(self, key: str, value: Any, operation: str, vc: VectorClock):
        """Replicate to all peers"""
        replication_msg = {
            'command': 'REPLICATE',
            'operation': operation,
            'key': key,
            'value': value,
            'from_node': self.node_id,
            'vc': vc.clock,
            'timestamp': int(time.time() * 1000)
        }
        
        for peer_id, peer_host, peer_port in self.peers:
            threading.Thread(
                target=self._send_to_peer,
                args=(peer_host, peer_port, replication_msg),
                daemon=True
            ).start()

    def _send_to_peer(self, peer_host: str, peer_port: int, message: Dict):
        """Send message to peer"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(2)
                sock.connect((peer_host, peer_port))
                sock.sendall(json.dumps(message).encode('utf-8'))
        except Exception as e:
            self.logger.debug(f"Failed to replicate to {peer_host}:{peer_port}: {e}")

    def handle_replication(self, message: Dict) -> bool:
        """Handle replication from peer"""
        operation = message.get('operation')
        key = message.get('key')
        value = message.get('value')
        from_node = message.get('from_node')
        vc_dict = message.get('vc', {})
        timestamp = message.get('timestamp', int(time.time() * 1000))
        
        peer_vc = VectorClock(vc_dict)
        
        with self.lock:
            # Update our vector clock
            self.vector_clock.merge(peer_vc)
            
            # Check for conflict
            if key in self.data:
                existing_vc = self.data[key]['vc']
                existing_ts = int(self.data[key]['timestamp'])
                
                if not self._resolve_conflict(key, existing_vc, peer_vc, existing_ts, timestamp):
                    return False
            
            # Apply operation
            if operation == 'SET':
                old_value = self.data.get(key, {}).get('value') if key in self.data else None
                self.data[key] = {
                    'value': value,
                    'vc': peer_vc,
                    'timestamp': timestamp
                }
                self._update_indexes(key, old_value, value)
            elif operation == 'DELETE':
                if key in self.data:
                    old_value = self.data[key]['value']
                    self.data[key] = {
                        'value': None,
                        'vc': peer_vc,
                        'timestamp': timestamp
                    }
                    self._update_indexes(key, old_value, None, is_delete=True)
            
            self._save_data()
            self._save_embeddings()
            return True

    def search_by_value(self, value: Any) -> List[str]:
        """Search by value"""
        with self.lock:
            value_str = str(value)
            return self.value_index.get(value_str, [])

    def search_fulltext(self, word: str) -> List[str]:
        """Full-text search"""
        with self.lock:
            word = word.lower()
            return self.fulltext_index.get(word, [])

    def search_semantic(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """Semantic search using word embeddings"""
        with self.lock:
            query_embedding = SimpleEmbedding.embed(query)
            
            results = []
            for key, embedding_list in self.embedding_index.items():
                if key in self.data and self.data[key]['value'] is not None:
                    embedding = np.array(embedding_list)
                    similarity = SimpleEmbedding.similarity(query_embedding, embedding)
                    results.append((key, similarity))
            
            # Sort by similarity and return top_k
            results.sort(key=lambda x: x[1], reverse=True)
            return results[:top_k]

    def get_all(self) -> Dict[str, Any]:
        """Get all non-deleted data"""
        with self.lock:
            return {k: v['value'] for k, v in self.data.items() if v['value'] is not None}

    def get_metadata(self) -> Dict[str, Any]:
        """Get metadata about node state"""
        with self.lock:
            return {
                'node_id': self.node_id,
                'vector_clock': self.vector_clock.clock,
                'data_count': len([v for v in self.data.values() if v['value'] is not None]),
                'tombstone_count': len([v for v in self.data.values() if v['value'] is None])
            }


class MasterlessServer:
    """Server for master-less peer-to-peer replication"""

    def __init__(self, node_id: int, host: str, port: int, 
                 cluster_nodes: List[Tuple[int, str, int]], data_dir: str = "masterless_data"):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        self.logger = logging.getLogger(f"Server-{node_id}")
        
        # Create peer list (exclude self)
        peer_info = [(nid, h, p) for nid, h, p in cluster_nodes if nid != node_id]
        
        # Create masterless node
        self.node = MasterlessNode(node_id, host, port, data_dir=str(self.data_dir), peers=peer_info)
        
        self.socket = None
        self.running = False
        self.clients = []
        self.client_lock = threading.Lock()

    def start(self):
        """Start server"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, self.port))
        self.socket.listen(10)
        self.running = True
        
        self.logger.info(f"Master-less server started on {self.host}:{self.port}")
        
        try:
            while self.running:
                try:
                    self.socket.settimeout(1)
                    client_socket, address = self.socket.accept()
                    self.logger.info(f"Client connected: {address}")
                    with self.client_lock:
                        self.clients.append(client_socket)
                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, address),
                        daemon=True
                    )
                    client_thread.start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        self.logger.error(f"Error accepting connection: {e}")
        except KeyboardInterrupt:
            self.logger.info("Server interrupted")
        finally:
            self.stop()

    def _handle_client(self, client_socket: socket.socket, address: Tuple):
        """Handle client connection"""
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
                    self.logger.error(f"Error: {e}")
                    response = {'status': 'error', 'message': str(e)}
                    client_socket.sendall(json.dumps(response).encode('utf-8'))
        except Exception as e:
            self.logger.error(f"Client error: {e}")
        finally:
            client_socket.close()
            with self.client_lock:
                if client_socket in self.clients:
                    self.clients.remove(client_socket)

    def _process_request(self, request: Dict) -> Dict:
        """Process request"""
        command = request.get('command')
        
        if command == 'SET':
            key = request.get('key')
            value = request.get('value')
            debug_mode = request.get('debug_mode', False)
            fail_chance = request.get('fail_chance', 0.01)
            success = self.node.set(key, value, debug_mode, fail_chance)
            return {'status': 'ok' if success else 'error', 'result': success}
        
        elif command == 'GET':
            key = request.get('key')
            value = self.node.get(key)
            return {'status': 'ok', 'result': value}
        
        elif command == 'DELETE':
            key = request.get('key')
            success = self.node.delete(key)
            return {'status': 'ok' if success else 'error', 'result': success}
        
        elif command == 'BULK_SET':
            items = request.get('items', [])
            debug_mode = request.get('debug_mode', False)
            fail_chance = request.get('fail_chance', 0.01)
            success = self.node.bulk_set(items, debug_mode, fail_chance)
            return {'status': 'ok' if success else 'error', 'result': success}
        
        elif command == 'REPLICATE':
            success = self.node.handle_replication(request)
            return {'status': 'ok' if success else 'error', 'result': success}
        
        elif command == 'SEARCH_VALUE':
            value = request.get('value')
            result = self.node.search_by_value(value)
            return {'status': 'ok', 'result': result}
        
        elif command == 'SEARCH_FULLTEXT':
            word = request.get('word')
            result = self.node.search_fulltext(word)
            return {'status': 'ok', 'result': result}
        
        elif command == 'SEARCH_SEMANTIC':
            query = request.get('query')
            top_k = request.get('top_k', 10)
            result = self.node.search_semantic(query, top_k)
            return {'status': 'ok', 'result': result}
        
        elif command == 'GET_ALL':
            result = self.node.get_all()
            return {'status': 'ok', 'result': result}
        
        elif command == 'GET_METADATA':
            result = self.node.get_metadata()
            return {'status': 'ok', 'result': result}
        
        else:
            return {'status': 'error', 'message': f'Unknown command: {command}'}

    def stop(self):
        """Stop server"""
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
        self.logger.info("Server stopped")


if __name__ == "__main__":
    # Example 3-node masterless cluster
    cluster_nodes = [
        (0, "localhost", 7000),
        (1, "localhost", 7001),
        (2, "localhost", 7002),
    ]
    
    node_id = int(input("Enter node ID (0, 1, or 2): "))
    node_host, node_port = "localhost", 7000 + node_id
    
    server = MasterlessServer(node_id, node_host, node_port, cluster_nodes)
    server.start()
