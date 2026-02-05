"""
Clustered NoSQL Key-Value Store with Replication and Leader Election
Supports 3-node cluster with primary and secondaries
Implements Raft-like consensus for leader election
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
import queue

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class NodeRole(Enum):
    PRIMARY = "primary"
    SECONDARY = "secondary"
    CANDIDATE = "candidate"


class ClusterNode:
    """Individual node in the cluster"""

    def __init__(self, node_id: int, host: str, port: int, data_dir: str = "data", 
                 peer_info: List[Tuple[str, int]] = None):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        self.logger = logging.getLogger(f"Node-{node_id}")
        
        # State machine
        self.data: Dict[str, Any] = {}
        self.lock = threading.RLock()
        
        # Persistence
        self.wal_file = self.data_dir / f"node_{node_id}_wal.log"
        self.data_file = self.data_dir / f"node_{node_id}_data.json"
        self.state_file = self.data_dir / f"node_{node_id}_state.json"
        self.index_file = self.data_dir / f"node_{node_id}_index.json"
        self.fulltext_index_file = self.data_dir / f"node_{node_id}_fulltext.json"
        
        # Indexes
        self.value_index: Dict[str, List[str]] = {}
        self.fulltext_index: Dict[str, List[str]] = {}
        
        # Raft state
        self.current_term = 0
        self.voted_for: Optional[int] = None
        self.log: List[Dict[str, Any]] = []
        self.commit_index = 0
        self.last_applied = 0
        
        # Role and leader election
        self.role = NodeRole.SECONDARY
        self.leader_id: Optional[int] = None
        self.election_timeout = random.uniform(1.5, 3.0)  # seconds
        self.last_heartbeat = time.time()
        
        # Peer information
        self.peers = peer_info or []
        self.next_index: Dict[int, int] = {peer_id: 0 for peer_id, _, _ in self.peers}
        self.match_index: Dict[int, int] = {peer_id: -1 for peer_id, _, _ in self.peers}
        
        # Server
        self.socket = None
        self.running = False
        self.clients = []
        self.client_lock = threading.Lock()
        
        # Replication
        self.replica_connections: Dict[int, socket.socket] = {}
        self.replica_lock = threading.Lock()
        
        # Load persisted state
        self._load_state()
        self._load_data()
        self._load_indexes()

    def _load_state(self):
        """Load Raft state from disk"""
        with self.lock:
            if self.state_file.exists():
                try:
                    with open(self.state_file, 'r') as f:
                        state = json.load(f)
                        self.current_term = state.get('term', 0)
                        self.voted_for = state.get('voted_for')
                    self.logger.info(f"Loaded state: term={self.current_term}, voted_for={self.voted_for}")
                except Exception as e:
                    self.logger.error(f"Error loading state: {e}")

    def _save_state(self):
        """Save Raft state to disk"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump({
                    'term': self.current_term,
                    'voted_for': self.voted_for
                }, f)
        except Exception as e:
            self.logger.error(f"Error saving state: {e}")

    def _load_data(self):
        """Load data from disk and replay WAL"""
        with self.lock:
            if self.data_file.exists():
                try:
                    with open(self.data_file, 'r') as f:
                        self.data = json.load(f)
                    self.logger.info(f"Loaded data: {len(self.data)} keys")
                except Exception as e:
                    self.logger.error(f"Error loading data: {e}")
            
            if self.wal_file.exists():
                try:
                    with open(self.wal_file, 'r') as f:
                        for line in f:
                            if not line.strip():
                                continue
                            entry = json.loads(line)
                            self._replay_wal_entry(entry)
                except Exception as e:
                    self.logger.error(f"Error replaying WAL: {e}")

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
                    self.logger.error(f"Error loading index: {e}")
            
            if self.fulltext_index_file.exists():
                try:
                    with open(self.fulltext_index_file, 'r') as f:
                        self.fulltext_index = json.load(f)
                except Exception as e:
                    self.logger.error(f"Error loading fulltext index: {e}")

    def _write_wal(self, operation: str, key: str, value: Any = None):
        """Write operation to WAL"""
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
            self.logger.error(f"Error writing WAL: {e}")

    def _save_data(self, debug_mode: bool = False, fail_chance: float = 0.01):
        """Save data to disk"""
        if debug_mode and random.random() < fail_chance:
            self.logger.warning("Simulated write failure (debug mode)")
            return
        
        try:
            with open(self.data_file, 'w') as f:
                json.dump(self.data, f)
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")

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
            self.logger.error(f"Error saving indexes: {e}")

    def _update_value_index(self, key: str, old_value: Any = None, new_value: Any = None):
        """Update value index"""
        if old_value is not None:
            value_str = str(old_value)
            if value_str in self.value_index:
                self.value_index[value_str] = [k for k in self.value_index[value_str] if k != key]
                if not self.value_index[value_str]:
                    del self.value_index[value_str]
        
        if new_value is not None:
            value_str = str(new_value)
            if value_str not in self.value_index:
                self.value_index[value_str] = []
            if key not in self.value_index[value_str]:
                self.value_index[value_str].append(key)

    def _update_fulltext_index(self, key: str, text: str = "", remove: bool = False):
        """Update full-text index"""
        words = text.lower().split() if isinstance(text, str) else []
        
        if remove:
            for word_list in self.fulltext_index.values():
                if key in word_list:
                    word_list.remove(key)
            self.fulltext_index = {w: keys for w, keys in self.fulltext_index.items() if keys}
        else:
            for word in words:
                if word not in self.fulltext_index:
                    self.fulltext_index[word] = []
                if key not in self.fulltext_index[word]:
                    self.fulltext_index[word].append(key)

    def set(self, key: str, value: Any, debug_mode: bool = False, fail_chance: float = 0.01) -> bool:
        """Set a key-value pair (primary only)"""
        if self.role != NodeRole.PRIMARY:
            raise Exception("Only primary can accept writes")
        
        with self.lock:
            old_value = self.data.get(key)
            self._write_wal('SET', key, value)
            self.data[key] = value
            self._update_value_index(key, old_value, value)
            self._update_fulltext_index(key, str(value))
            self._save_data(debug_mode, fail_chance)
            self._save_indexes()
            
            # Replicate to secondaries
            self._replicate_to_peers(key, value, 'SET')
            
            return True

    def get(self, key: str) -> Optional[Any]:
        """Get a value by key"""
        with self.lock:
            return self.data.get(key)

    def delete(self, key: str) -> bool:
        """Delete a key (primary only)"""
        if self.role != NodeRole.PRIMARY:
            raise Exception("Only primary can accept writes")
        
        with self.lock:
            if key not in self.data:
                return False
            
            old_value = self.data[key]
            self._write_wal('DELETE', key)
            del self.data[key]
            self._update_value_index(key, old_value, None)
            self._update_fulltext_index(key, remove=True)
            self._save_data()
            self._save_indexes()
            
            # Replicate to secondaries
            self._replicate_to_peers(key, None, 'DELETE')
            
            return True

    def bulk_set(self, items: List[Tuple[str, Any]], debug_mode: bool = False, fail_chance: float = 0.01) -> bool:
        """Bulk set (primary only)"""
        if self.role != NodeRole.PRIMARY:
            raise Exception("Only primary can accept writes")
        
        with self.lock:
            for key, value in items:
                self._write_wal('SET', key, value)
                old_value = self.data.get(key)
                self.data[key] = value
                self._update_value_index(key, old_value, value)
                self._update_fulltext_index(key, str(value))
            
            self._save_data(debug_mode, fail_chance)
            self._save_indexes()
            
            # Replicate to secondaries
            for key, value in items:
                self._replicate_to_peers(key, value, 'SET')
            
            return True

    def _replicate_to_peers(self, key: str, value: Any, operation: str):
        """Replicate operation to peer nodes"""
        replication_data = {
            'command': 'REPLICATE',
            'operation': operation,
            'key': key,
            'value': value,
            'term': self.current_term
        }
        
        for peer_id, peer_host, peer_port in self.peers:
            threading.Thread(
                target=self._send_to_peer,
                args=(peer_id, peer_host, peer_port, replication_data),
                daemon=True
            ).start()

    def _send_to_peer(self, peer_id: int, peer_host: str, peer_port: int, data: Dict):
        """Send data to a peer"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(2)
                sock.connect((peer_host, peer_port))
                sock.sendall(json.dumps(data).encode('utf-8'))
        except Exception as e:
            self.logger.debug(f"Failed to replicate to node {peer_id}: {e}")

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

    def get_all(self) -> Dict[str, Any]:
        """Get all data"""
        with self.lock:
            return dict(self.data)

    def become_primary(self):
        """Become primary leader"""
        with self.lock:
            self.role = NodeRole.PRIMARY
            self.leader_id = self.node_id
            self.logger.info(f"Node {self.node_id} became PRIMARY in term {self.current_term}")

    def become_secondary(self, leader_id: int):
        """Become secondary"""
        with self.lock:
            self.role = NodeRole.SECONDARY
            self.leader_id = leader_id
            self.last_heartbeat = time.time()
            self.logger.info(f"Node {self.node_id} became SECONDARY (leader: {leader_id})")

    def clear(self):
        """Clear all data"""
        with self.lock:
            self.data.clear()
            self.value_index.clear()
            self.fulltext_index.clear()
            self._save_data()
            self._save_indexes()


class ClusterServer:
    """Server for clustered Key-Value store"""

    def __init__(self, node_id: int, host: str, port: int, cluster_nodes: List[Tuple[int, str, int]], 
                 data_dir: str = "cluster_data"):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.cluster_nodes = cluster_nodes
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        self.logger = logging.getLogger(f"Server-{node_id}")
        
        # Get peer info (all nodes except self)
        peer_info = [(nid, h, p) for nid, h, p in cluster_nodes if nid != node_id]
        
        # Create cluster node
        self.node = ClusterNode(node_id, host, port, data_dir=str(self.data_dir), peer_info=peer_info)
        
        # Server state
        self.socket = None
        self.running = False
        self.clients = []
        self.client_lock = threading.Lock()
        
        # Election timer
        self.election_thread = None
        self.heartbeat_thread = None
        
        # Initialize as secondary
        self.node.become_secondary(None)

    def start(self):
        """Start the server"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, self.port))
        self.socket.listen(10)
        self.running = True
        
        self.logger.info(f"Server started on {self.host}:{self.port} (Node {self.node_id})")
        
        # Start election and heartbeat threads
        self.election_thread = threading.Thread(target=self._election_loop, daemon=True)
        self.election_thread.start()
        
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
        
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

    def _election_loop(self):
        """Run leader election"""
        while self.running:
            try:
                time.sleep(0.1)
                
                # Check election timeout
                if self.node.role != NodeRole.PRIMARY:
                    if time.time() - self.node.last_heartbeat > self.node.election_timeout:
                        self._start_election()
            except Exception as e:
                self.logger.error(f"Election loop error: {e}")

    def _start_election(self):
        """Start leader election"""
        with self.node.lock:
            self.node.current_term += 1
            self.node.role = NodeRole.CANDIDATE
            self.node.voted_for = self.node_id
            self.node._save_state()
            
            votes = 1  # Vote for self
            
            self.logger.info(f"Starting election for term {self.node.current_term}")
            
            # Request votes from all peers
            request_data = {
                'command': 'REQUEST_VOTE',
                'term': self.node.current_term,
                'candidate_id': self.node_id,
                'last_log_index': len(self.node.log) - 1,
                'last_log_term': self.node.log[-1].get('term', 0) if self.node.log else 0
            }
            
            responses = []
            for peer_id, peer_host, peer_port in self.node.peers:
                threading.Thread(
                    target=self._request_vote,
                    args=(peer_id, peer_host, peer_port, request_data, responses),
                    daemon=True
                ).start()
            
            # Wait for responses
            time.sleep(0.5)
            
            for response in responses:
                if response.get('vote_granted'):
                    votes += 1
            
            # Check if won election (need majority)
            total_nodes = len(self.node.peers) + 1
            if votes > total_nodes // 2:
                self.logger.info(f"Won election! Votes: {votes}/{total_nodes}")
                self.node.become_primary()
            else:
                self.logger.info(f"Lost election. Votes: {votes}/{total_nodes}")
                self.node.become_secondary(None)

    def _request_vote(self, peer_id: int, peer_host: str, peer_port: int, data: Dict, responses: List):
        """Request vote from a peer"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(2)
                sock.connect((peer_host, peer_port))
                sock.sendall(json.dumps(data).encode('utf-8'))
                response = sock.recv(4096).decode('utf-8')
                responses.append(json.loads(response))
        except Exception as e:
            self.logger.debug(f"Failed to request vote from node {peer_id}: {e}")

    def _heartbeat_loop(self):
        """Send heartbeats if primary"""
        while self.running:
            try:
                if self.node.role == NodeRole.PRIMARY:
                    self._send_heartbeats()
                time.sleep(0.5)
            except Exception as e:
                self.logger.error(f"Heartbeat loop error: {e}")

    def _send_heartbeats(self):
        """Send heartbeats to all peers"""
        heartbeat = {
            'command': 'HEARTBEAT',
            'term': self.node.current_term,
            'leader_id': self.node_id,
            'commit_index': self.node.commit_index
        }
        
        for peer_id, peer_host, peer_port in self.node.peers:
            threading.Thread(
                target=self._send_to_peer,
                args=(peer_id, peer_host, peer_port, heartbeat),
                daemon=True
            ).start()

    def _send_to_peer(self, peer_id: int, peer_host: str, peer_port: int, data: Dict):
        """Send data to peer"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(2)
                sock.connect((peer_host, peer_port))
                sock.sendall(json.dumps(data).encode('utf-8'))
        except Exception as e:
            self.logger.debug(f"Failed to send to node {peer_id}: {e}")

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
                    self.logger.error(f"Error processing request: {e}")
                    response = {'status': 'error', 'message': str(e)}
                    client_socket.sendall(json.dumps(response).encode('utf-8'))
        except Exception as e:
            self.logger.error(f"Client error: {e}")
        finally:
            client_socket.close()
            with self.client_lock:
                if client_socket in self.clients:
                    self.clients.remove(client_socket)
            self.logger.info(f"Client disconnected: {address}")

    def _process_request(self, request: Dict) -> Dict:
        """Process request"""
        command = request.get('command')
        
        if command == 'REQUEST_VOTE':
            return self._handle_request_vote(request)
        elif command == 'HEARTBEAT':
            return self._handle_heartbeat(request)
        elif command == 'REPLICATE':
            return self._handle_replicate(request)
        elif command == 'SET':
            if self.node.role != NodeRole.PRIMARY:
                return {'status': 'error', 'message': f'Not primary. Leader: {self.node.leader_id}'}
            try:
                key = request.get('key')
                value = request.get('value')
                debug_mode = request.get('debug_mode', False)
                fail_chance = request.get('fail_chance', 0.01)
                self.node.set(key, value, debug_mode, fail_chance)
                return {'status': 'ok', 'result': True}
            except Exception as e:
                return {'status': 'error', 'message': str(e)}
        elif command == 'GET':
            result = self.node.get(request.get('key'))
            return {'status': 'ok', 'result': result}
        elif command == 'DELETE':
            if self.node.role != NodeRole.PRIMARY:
                return {'status': 'error', 'message': 'Not primary'}
            try:
                success = self.node.delete(request.get('key'))
                return {'status': 'ok', 'result': success}
            except Exception as e:
                return {'status': 'error', 'message': str(e)}
        elif command == 'BULK_SET':
            if self.node.role != NodeRole.PRIMARY:
                return {'status': 'error', 'message': 'Not primary'}
            try:
                items = request.get('items', [])
                debug_mode = request.get('debug_mode', False)
                fail_chance = request.get('fail_chance', 0.01)
                self.node.bulk_set(items, debug_mode, fail_chance)
                return {'status': 'ok', 'result': True}
            except Exception as e:
                return {'status': 'error', 'message': str(e)}
        elif command == 'GET_ALL':
            result = self.node.get_all()
            return {'status': 'ok', 'result': result}
        elif command == 'GET_LEADER':
            return {'status': 'ok', 'result': self.node.leader_id, 'role': self.node.role.value}
        else:
            return {'status': 'error', 'message': f'Unknown command: {command}'}

    def _handle_request_vote(self, request: Dict) -> Dict:
        """Handle vote request"""
        term = request.get('term')
        candidate_id = request.get('candidate_id')
        
        with self.node.lock:
            if term > self.node.current_term:
                self.node.current_term = term
                self.node.voted_for = None
                self.node._save_state()
            
            if term < self.node.current_term:
                return {'vote_granted': False}
            
            if self.node.voted_for is None or self.node.voted_for == candidate_id:
                self.node.voted_for = candidate_id
                self.node._save_state()
                return {'vote_granted': True}
            
            return {'vote_granted': False}

    def _handle_heartbeat(self, request: Dict) -> Dict:
        """Handle heartbeat"""
        term = request.get('term')
        leader_id = request.get('leader_id')
        
        with self.node.lock:
            if term > self.node.current_term:
                self.node.current_term = term
                self.node.voted_for = None
                self.node._save_state()
            
            if term >= self.node.current_term:
                self.node.become_secondary(leader_id)
                self.node.last_heartbeat = time.time()
                return {'status': 'ok'}
            
            return {'status': 'error'}

    def _handle_replicate(self, request: Dict) -> Dict:
        """Handle replication from primary"""
        operation = request.get('operation')
        key = request.get('key')
        value = request.get('value')
        
        with self.node.lock:
            if operation == 'SET':
                old_value = self.node.data.get(key)
                self.node.data[key] = value
                self.node._update_value_index(key, old_value, value)
                self.node._update_fulltext_index(key, str(value))
                self.node._write_wal('SET', key, value)
            elif operation == 'DELETE':
                if key in self.node.data:
                    old_value = self.node.data[key]
                    del self.node.data[key]
                    self.node._update_value_index(key, old_value, None)
                    self.node._update_fulltext_index(key, remove=True)
                    self.node._write_wal('DELETE', key)
            
            self.node._save_data()
            self.node._save_indexes()
            return {'status': 'ok'}

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
    # Example 3-node cluster
    cluster_nodes = [
        (0, "localhost", 6000),
        (1, "localhost", 6001),
        (2, "localhost", 6002),
    ]
    
    node_id = int(input("Enter node ID (0, 1, or 2): "))
    node_host, node_port = "localhost", 6000 + node_id
    
    server = ClusterServer(node_id, node_host, node_port, cluster_nodes)
    server.start()
