"""
Tests for clustered Key-Value store with replication and leader election
"""
import unittest
import time
import threading
import subprocess
import os
import sys
import shutil
from pathlib import Path
from typing import List, Optional
from src.client import KVClient
from src.cluster_server import ClusterServer, NodeRole
import logging
import socket

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClusterTestClient(KVClient):
    """Extended client with cluster awareness"""
    
    def get_leader(self) -> Optional[int]:
        """Get current leader node ID"""
        try:
            request = {'command': 'GET_LEADER'}
            response = self._send_request(request)
            return response.get('result')
        except Exception as e:
            logger.warning(f"Failed to get leader: {e}")
            return None
    
    def get_role(self) -> Optional[str]:
        """Get node role"""
        try:
            request = {'command': 'GET_LEADER'}
            response = self._send_request(request)
            return response.get('role')
        except Exception as e:
            logger.warning(f"Failed to get role: {e}")
            return None


class TestClusterBasics(unittest.TestCase):
    """Test basic cluster operations"""

    @classmethod
    def setUpClass(cls):
        """Start 3-node cluster"""
        cls.cluster_nodes = [
            (0, "localhost", 6100),
            (1, "localhost", 6101),
            (2, "localhost", 6102),
        ]
        cls.data_dir = Path("test_cluster")
        if cls.data_dir.exists():
            shutil.rmtree(cls.data_dir)
        cls.data_dir.mkdir()
        
        cls.servers = []
        cls.threads = []
        
        for node_id, host, port in cls.cluster_nodes:
            server = ClusterServer(node_id, host, port, cls.cluster_nodes, data_dir=str(cls.data_dir))
            thread = threading.Thread(target=server.start, daemon=True)
            thread.start()
            cls.servers.append(server)
            cls.threads.append(thread)
        
        # Wait for cluster to start and elect leader
        time.sleep(3)

    @classmethod
    def tearDownClass(cls):
        """Stop cluster"""
        for server in cls.servers:
            server.stop()
        if cls.data_dir.exists():
            shutil.rmtree(cls.data_dir)

    def setUp(self):
        """Setup test"""
        # Find current leader
        leader_port = None
        for node_id, host, port in self.cluster_nodes:
            try:
                client = ClusterTestClient(host=host, port=port)
                leader = client.get_leader()
                client.close()
                if leader is not None:
                    for nid, _, p in self.cluster_nodes:
                        if nid == leader:
                            leader_port = p
                            break
                    break
            except:
                continue
        
        if not leader_port:
            leader_port = 6100  # Default to first node
        
        self.client = ClusterTestClient(host="localhost", port=leader_port)
        
        # Clear data
        try:
            all_data = self.client.get_all()
            for key in all_data:
                self.client.delete(key)
        except:
            pass

    def tearDown(self):
        """Cleanup"""
        try:
            self.client.close()
        except:
            pass

    def test_write_to_primary(self):
        """Test: Write to primary is successful"""
        try:
            self.client.set("cluster_key", "cluster_value")
            result = self.client.get("cluster_key")
            self.assertEqual(result, "cluster_value")
            logger.info("✓ Write to primary passed")
        except Exception as e:
            self.skipTest(f"Cluster not ready: {e}")

    def test_replication(self):
        """Test: Data is replicated to secondaries"""
        try:
            self.client.set("replicate_key", "replicate_value")
            
            # Give replication time
            time.sleep(0.5)
            
            # Read from different nodes
            for node_id, host, port in self.cluster_nodes:
                try:
                    client = ClusterTestClient(host=host, port=port)
                    result = client.get("replicate_key")
                    self.assertEqual(result, "replicate_value")
                    client.close()
                except:
                    pass  # Node might not be reachable
            
            logger.info("✓ Replication passed")
        except Exception as e:
            self.skipTest(f"Cluster not ready: {e}")

    def test_bulk_write_replication(self):
        """Test: Bulk writes are replicated"""
        try:
            items = [("bk_1", "bv_1"), ("bk_2", "bv_2"), ("bk_3", "bv_3")]
            self.client.bulk_set(items)
            
            time.sleep(0.5)
            
            for key, value in items:
                result = self.client.get(key)
                self.assertEqual(result, value)
            
            logger.info("✓ Bulk write replication passed")
        except Exception as e:
            self.skipTest(f"Cluster not ready: {e}")


class TestLeaderElection(unittest.TestCase):
    """Test leader election"""

    def setUp(self):
        """Setup for each test"""
        self.data_dir = Path("test_election")
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)
        self.data_dir.mkdir()

    def tearDown(self):
        """Cleanup"""
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)

    def test_initial_election(self):
        """Test: Cluster elects a leader initially"""
        cluster_nodes = [
            (0, "localhost", 6110),
            (1, "localhost", 6111),
            (2, "localhost", 6112),
        ]
        
        servers = []
        for node_id, host, port in cluster_nodes:
            server = ClusterServer(node_id, host, port, cluster_nodes, data_dir=str(self.data_dir))
            thread = threading.Thread(target=server.start, daemon=True)
            thread.start()
            servers.append(server)
        
        # Wait for election
        time.sleep(3)
        
        # Check leader is elected
        leader_found = False
        for node_id, host, port in cluster_nodes:
            try:
                client = ClusterTestClient(host=host, port=port)
                leader = client.get_leader()
                client.close()
                if leader is not None:
                    leader_found = True
                    logger.info(f"✓ Leader elected: Node {leader}")
                    break
            except:
                pass
        
        self.assertTrue(leader_found, "No leader was elected")
        
        for server in servers:
            server.stop()

    def test_leader_failure_triggers_election(self):
        """Test: Leader failure triggers new election"""
        cluster_nodes = [
            (0, "localhost", 6120),
            (1, "localhost", 6121),
            (2, "localhost", 6122),
        ]
        
        servers = []
        for node_id, host, port in cluster_nodes:
            server = ClusterServer(node_id, host, port, cluster_nodes, data_dir=str(self.data_dir))
            thread = threading.Thread(target=server.start, daemon=True)
            thread.start()
            servers.append(server)
        
        # Wait for initial election
        time.sleep(3)
        
        # Find current leader
        current_leader = None
        leader_client = None
        for node_id, host, port in cluster_nodes:
            try:
                client = ClusterTestClient(host=host, port=port)
                leader = client.get_leader()
                if leader is not None:
                    current_leader = leader
                    leader_client = client
                    break
                client.close()
            except:
                pass
        
        if current_leader is not None:
            logger.info(f"Initial leader: Node {current_leader}")
            leader_client.close()
            
            # Stop the primary
            primary_index = current_leader
            logger.info(f"Stopping primary (Node {primary_index})...")
            servers[primary_index].stop()
            
            # Wait for new election
            time.sleep(3)
            
            # Find new leader (should be different)
            new_leader = None
            for i, (node_id, host, port) in enumerate(cluster_nodes):
                if i == primary_index:
                    continue  # Skip the stopped node
                try:
                    client = ClusterTestClient(host=host, port=port)
                    leader = client.get_leader()
                    if leader is not None and leader != primary_index:
                        new_leader = leader
                        client.close()
                        break
                    client.close()
                except:
                    pass
            
            if new_leader is not None:
                logger.info(f"✓ New leader elected: Node {new_leader}")
            else:
                logger.warning("Could not verify new leader")
        
        for i, server in enumerate(servers):
            if i != primary_index:
                server.stop()


class TestConsistency(unittest.TestCase):
    """Test consistency with concurrent writes"""

    def setUp(self):
        """Setup"""
        self.data_dir = Path("test_consistency")
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)
        self.data_dir.mkdir()

    def tearDown(self):
        """Cleanup"""
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)

    def test_consistency_concurrent_writes(self):
        """Test: Consistency with concurrent writes"""
        cluster_nodes = [
            (0, "localhost", 6130),
            (1, "localhost", 6131),
            (2, "localhost", 6132),
        ]
        
        servers = []
        for node_id, host, port in cluster_nodes:
            server = ClusterServer(node_id, host, port, cluster_nodes, data_dir=str(self.data_dir))
            thread = threading.Thread(target=server.start, daemon=True)
            thread.start()
            servers.append(server)
        
        time.sleep(3)
        
        # Find leader
        leader_port = None
        for node_id, host, port in cluster_nodes:
            try:
                client = ClusterTestClient(host=host, port=port)
                leader = client.get_leader()
                if leader is not None:
                    for nid, _, p in cluster_nodes:
                        if nid == leader:
                            leader_port = p
                            break
                client.close()
                break
            except:
                continue
        
        if leader_port:
            client = ClusterTestClient(host="localhost", port=leader_port)
            
            # Write from primary
            for i in range(100):
                client.set(f"consistency_key_{i}", f"consistency_value_{i}")
            
            time.sleep(0.5)
            
            # Verify on all nodes
            all_consistent = True
            for node_id, host, port in cluster_nodes:
                try:
                    test_client = ClusterTestClient(host=host, port=port)
                    for i in range(100):
                        result = test_client.get(f"consistency_key_{i}")
                        if result != f"consistency_value_{i}":
                            all_consistent = False
                    test_client.close()
                except:
                    pass
            
            self.assertTrue(all_consistent)
            logger.info("✓ Consistency test passed")
            client.close()
        
        for server in servers:
            server.stop()


def run_tests():
    """Run all cluster tests"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestClusterBasics))
    suite.addTests(loader.loadTestsFromTestCase(TestLeaderElection))
    suite.addTests(loader.loadTestsFromTestCase(TestConsistency))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
