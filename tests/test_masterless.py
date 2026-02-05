"""
Tests for master-less replication with semantic search
"""
import unittest
import time
import threading
import shutil
from pathlib import Path
import logging
import sys
from src.masterless_client import MasterlessClient
from src.masterless_server import MasterlessServer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestMasterlessBasics(unittest.TestCase):
    """Test basic master-less operations"""

    @classmethod
    def setUpClass(cls):
        """Start 3-node masterless cluster"""
        cls.cluster_nodes = [
            (0, "localhost", 7100),
            (1, "localhost", 7101),
            (2, "localhost", 7102),
        ]
        cls.data_dir = Path("test_masterless")
        if cls.data_dir.exists():
            shutil.rmtree(cls.data_dir)
        cls.data_dir.mkdir()
        
        cls.servers = []
        cls.threads = []
        
        for node_id, host, port in cls.cluster_nodes:
            server = MasterlessServer(node_id, host, port, cls.cluster_nodes, data_dir=str(cls.data_dir))
            thread = threading.Thread(target=server.start, daemon=True)
            thread.start()
            cls.servers.append(server)
            cls.threads.append(thread)
        
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        """Stop cluster"""
        for server in cls.servers:
            server.stop()
        if cls.data_dir.exists():
            shutil.rmtree(cls.data_dir)

    def setUp(self):
        """Setup test"""
        self.client = MasterlessClient(host="localhost", port=7100)
        all_data = self.client.get_all()
        for key in all_data:
            self.client.delete(key)

    def tearDown(self):
        """Cleanup"""
        try:
            self.client.close()
        except:
            pass

    def test_set_and_get(self):
        """Test: Set and Get"""
        self.client.set("test_key", "test_value")
        result = self.client.get("test_key")
        self.assertEqual(result, "test_value")
        logger.info("✓ Set and Get passed")

    def test_eventual_consistency(self):
        """Test: Eventual consistency across nodes"""
        # Write to node 0
        self.client.set("consistency_key", "consistency_value")
        time.sleep(0.5)  # Wait for replication
        
        # Read from node 1
        client1 = MasterlessClient(host="localhost", port=7101)
        result = client1.get("consistency_key")
        client1.close()
        
        self.assertEqual(result, "consistency_value")
        logger.info("✓ Eventual consistency passed")

    def test_concurrent_writes_convergence(self):
        """Test: Concurrent writes converge"""
        # Write same key from different nodes (simulating concurrent writes)
        self.client.set("conflict_key", "value_from_node_0")
        
        client1 = MasterlessClient(host="localhost", port=7101)
        time.sleep(0.1)
        client1.set("conflict_key", "value_from_node_1")
        
        time.sleep(0.5)  # Wait for convergence
        
        # All nodes should have a consistent value
        result0 = self.client.get("conflict_key")
        result1 = client1.get("conflict_key")
        
        client2 = MasterlessClient(host="localhost", port=7102)
        result2 = client2.get("conflict_key")
        client2.close()
        
        self.assertEqual(result0, result1)
        self.assertEqual(result1, result2)
        logger.info(f"✓ Concurrent writes converged to: {result0}")
        
        client1.close()

    def test_delete_with_tombstone(self):
        """Test: Delete uses tombstone for consistency"""
        self.client.set("delete_key", "delete_value")
        self.client.delete("delete_key")
        
        time.sleep(0.5)  # Wait for replication
        
        # Verify deleted on all nodes
        result0 = self.client.get("delete_key")
        
        client1 = MasterlessClient(host="localhost", port=7101)
        result1 = client1.get("delete_key")
        client1.close()
        
        self.assertIsNone(result0)
        self.assertIsNone(result1)
        logger.info("✓ Delete with tombstone passed")

    def test_bulk_set_consistency(self):
        """Test: Bulk set is consistent across nodes"""
        items = [("bk_0", "bv_0"), ("bk_1", "bv_1"), ("bk_2", "bv_2")]
        self.client.bulk_set(items)
        
        time.sleep(0.5)
        
        client1 = MasterlessClient(host="localhost", port=7101)
        for key, value in items:
            result1 = client1.get(key)
            self.assertEqual(result1, value)
        client1.close()
        
        logger.info("✓ Bulk set consistency passed")

    def test_vector_clock_ordering(self):
        """Test: Vector clocks maintain causal ordering"""
        # Set a key
        self.client.set("vc_key", "initial_value")
        
        # Update it
        self.client.set("vc_key", "updated_value")
        time.sleep(0.3)
        
        # Should have latest value everywhere
        result = self.client.get("vc_key")
        self.assertEqual(result, "updated_value")
        
        client1 = MasterlessClient(host="localhost", port=7101)
        result1 = client1.get("vc_key")
        self.assertEqual(result1, "updated_value")
        client1.close()
        
        logger.info("✓ Vector clock ordering passed")


class TestSemanticsearch(unittest.TestCase):
    """Test semantic search with word embeddings"""

    @classmethod
    def setUpClass(cls):
        """Start cluster"""
        cls.cluster_nodes = [
            (0, "localhost", 7110),
            (1, "localhost", 7111),
            (2, "localhost", 7112),
        ]
        cls.data_dir = Path("test_semantic")
        if cls.data_dir.exists():
            shutil.rmtree(cls.data_dir)
        cls.data_dir.mkdir()
        
        cls.servers = []
        for node_id, host, port in cls.cluster_nodes:
            server = MasterlessServer(node_id, host, port, cls.cluster_nodes, data_dir=str(cls.data_dir))
            thread = threading.Thread(target=server.start, daemon=True)
            thread.start()
            cls.servers.append(server)
        
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        """Stop cluster"""
        for server in cls.servers:
            server.stop()
        if cls.data_dir.exists():
            shutil.rmtree(cls.data_dir)

    def setUp(self):
        """Setup"""
        self.client = MasterlessClient(host="localhost", port=7110)
        all_data = self.client.get_all()
        for key in all_data:
            self.client.delete(key)

    def tearDown(self):
        """Cleanup"""
        try:
            self.client.close()
        except:
            pass

    def test_semantic_search(self):
        """Test: Semantic search with embeddings"""
        # Add documents
        docs = {
            "doc_1": "machine learning algorithms",
            "doc_2": "deep neural networks",
            "doc_3": "artificial intelligence systems",
            "doc_4": "data science and analytics",
            "doc_5": "computer science programs"
        }
        
        for key, text in docs.items():
            self.client.set(key, text)
        
        time.sleep(0.3)
        
        # Search for related documents
        results = self.client.search_semantic("neural networks", top_k=3)
        
        # Should find related documents
        self.assertGreater(len(results), 0)
        
        # Results should be tuples of (key, similarity)
        for key, similarity in results:
            self.assertIn(key, docs)
            self.assertGreaterEqual(similarity, 0)
            self.assertLessEqual(similarity, 1)
        
        logger.info(f"✓ Semantic search passed: {results[:3]}")

    def test_value_index(self):
        """Test: Value index search"""
        self.client.set("user_1", "John Doe")
        self.client.set("user_2", "John Smith")
        self.client.set("user_3", "Jane Doe")
        
        results = self.client.search_by_value("John Doe")
        self.assertIn("user_1", results)
        logger.info("✓ Value index passed")

    def test_fulltext_search(self):
        """Test: Full-text search"""
        self.client.set("article_1", "python programming language")
        self.client.set("article_2", "javascript web development")
        self.client.set("article_3", "python machine learning")
        
        results = self.client.search_fulltext("python")
        self.assertIn("article_1", results)
        self.assertIn("article_3", results)
        self.assertNotIn("article_2", results)
        
        logger.info("✓ Full-text search passed")


class TestNodeMetadata(unittest.TestCase):
    """Test node metadata and monitoring"""

    @classmethod
    def setUpClass(cls):
        """Start cluster"""
        cls.cluster_nodes = [
            (0, "localhost", 7120),
            (1, "localhost", 7121),
            (2, "localhost", 7122),
        ]
        cls.data_dir = Path("test_metadata")
        if cls.data_dir.exists():
            shutil.rmtree(cls.data_dir)
        cls.data_dir.mkdir()
        
        cls.servers = []
        for node_id, host, port in cls.cluster_nodes:
            server = MasterlessServer(node_id, host, port, cls.cluster_nodes, data_dir=str(cls.data_dir))
            thread = threading.Thread(target=server.start, daemon=True)
            thread.start()
            cls.servers.append(server)
        
        time.sleep(1)

    @classmethod
    def tearDownClass(cls):
        """Stop cluster"""
        for server in cls.servers:
            server.stop()
        if cls.data_dir.exists():
            shutil.rmtree(cls.data_dir)

    def setUp(self):
        """Setup"""
        self.client = MasterlessClient(host="localhost", port=7120)

    def tearDown(self):
        """Cleanup"""
        try:
            self.client.close()
        except:
            pass

    def test_metadata_available(self):
        """Test: Node metadata is available"""
        metadata = self.client.get_metadata()
        
        self.assertIn('node_id', metadata)
        self.assertIn('vector_clock', metadata)
        self.assertIn('data_count', metadata)
        self.assertIn('tombstone_count', metadata)
        
        logger.info(f"✓ Metadata available: {metadata}")

    def test_metadata_consistency(self):
        """Test: Metadata is consistent across nodes"""
        # Add some data
        self.client.set("meta_key", "meta_value")
        time.sleep(0.3)
        
        # Check metadata on different nodes
        metadata0 = self.client.get_metadata()
        
        client1 = MasterlessClient(host="localhost", port=7121)
        metadata1 = client1.get_metadata()
        client1.close()
        
        # Both should have data count >= 1
        self.assertGreaterEqual(metadata0['data_count'], 1)
        self.assertGreaterEqual(metadata1['data_count'], 1)
        
        logger.info("✓ Metadata consistency passed")


def run_tests():
    """Run all masterless tests"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestMasterlessBasics))
    suite.addTests(loader.loadTestsFromTestCase(TestSemanticsearch))
    suite.addTests(loader.loadTestsFromTestCase(TestNodeMetadata))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
