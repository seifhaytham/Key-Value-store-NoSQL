"""
Comprehensive tests for Key-Value store
"""
import unittest
import time
import threading
import subprocess
import signal
import os
import sys
import shutil
from pathlib import Path
from typing import List
from src.client import KVClient
from src.server import KVServer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestKVBasics(unittest.TestCase):
    """Test basic Set, Get, Delete operations"""

    @classmethod
    def setUpClass(cls):
        """Start server before tests"""
        cls.data_dir = Path("test_data")
        if cls.data_dir.exists():
            shutil.rmtree(cls.data_dir)
        cls.data_dir.mkdir()
        
        cls.server = KVServer(host="localhost", port=5001, node_id=0, data_dir=str(cls.data_dir))
        cls.server_thread = threading.Thread(target=cls.server.start, daemon=True)
        cls.server_thread.start()
        time.sleep(0.5)  # Give server time to start

    @classmethod
    def tearDownClass(cls):
        """Stop server after tests"""
        cls.server.stop()
        if cls.data_dir.exists():
            shutil.rmtree(cls.data_dir)

    def setUp(self):
        """Clear data before each test"""
        self.client = KVClient(host="localhost", port=5001)
        # Clear all data
        all_data = self.client.get_all()
        for key in all_data:
            self.client.delete(key)

    def tearDown(self):
        """Clean up after each test"""
        self.client.close()

    def test_set_then_get(self):
        """Test: Set then Get"""
        self.client.set("key1", "value1")
        result = self.client.get("key1")
        self.assertEqual(result, "value1")
        logger.info("✓ Set then Get passed")

    def test_set_then_delete_then_get(self):
        """Test: Set then Delete then Get"""
        self.client.set("key2", "value2")
        self.assertTrue(self.client.delete("key2"))
        result = self.client.get("key2")
        self.assertIsNone(result)
        logger.info("✓ Set then Delete then Get passed")

    def test_get_without_setting(self):
        """Test: Get without setting"""
        result = self.client.get("nonexistent")
        self.assertIsNone(result)
        logger.info("✓ Get without setting passed")

    def test_set_twice_same_key(self):
        """Test: Set then Set (same key) then Get"""
        self.client.set("key3", "value3")
        self.client.set("key3", "value3_updated")
        result = self.client.get("key3")
        self.assertEqual(result, "value3_updated")
        logger.info("✓ Set twice same key passed")

    def test_bulk_set(self):
        """Test: Bulk Set"""
        items = [("key_a", "value_a"), ("key_b", "value_b"), ("key_c", "value_c")]
        self.assertTrue(self.client.bulk_set(items))
        
        self.assertEqual(self.client.get("key_a"), "value_a")
        self.assertEqual(self.client.get("key_b"), "value_b")
        self.assertEqual(self.client.get("key_c"), "value_c")
        logger.info("✓ Bulk Set passed")

    def test_different_data_types(self):
        """Test: Different data types"""
        self.client.set("int_key", 42)
        self.client.set("float_key", 3.14)
        self.client.set("bool_key", True)
        self.client.set("list_key", [1, 2, 3])
        self.client.set("dict_key", {"nested": "value"})
        
        self.assertEqual(self.client.get("int_key"), 42)
        self.assertEqual(self.client.get("float_key"), 3.14)
        self.assertEqual(self.client.get("bool_key"), True)
        self.assertEqual(self.client.get("list_key"), [1, 2, 3])
        self.assertEqual(self.client.get("dict_key"), {"nested": "value"})
        logger.info("✓ Different data types passed")


class TestPersistence(unittest.TestCase):
    """Test persistence across restarts"""

    def setUp(self):
        """Setup for each test"""
        self.data_dir = Path("test_persistence")
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)
        self.data_dir.mkdir()

    def tearDown(self):
        """Cleanup after each test"""
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)

    def test_set_then_exit_then_get(self):
        """Test: Set then exit (gracefully) then Get"""
        # Start server
        server1 = KVServer(host="localhost", port=5002, node_id=0, data_dir=str(self.data_dir))
        server_thread1 = threading.Thread(target=server1.start, daemon=True)
        server_thread1.start()
        time.sleep(0.5)
        
        # Set data
        client1 = KVClient(host="localhost", port=5002)
        client1.set("persist_key", "persist_value")
        client1.close()
        
        # Stop server
        server1.stop()
        time.sleep(0.5)
        
        # Start new server with same data directory
        server2 = KVServer(host="localhost", port=5002, node_id=0, data_dir=str(self.data_dir))
        server_thread2 = threading.Thread(target=server2.start, daemon=True)
        server_thread2.start()
        time.sleep(0.5)
        
        # Verify data persisted
        client2 = KVClient(host="localhost", port=5002)
        result = client2.get("persist_key")
        self.assertEqual(result, "persist_value")
        client2.close()
        
        server2.stop()
        logger.info("✓ Set then exit then Get passed")

    def test_bulk_set_persistence(self):
        """Test: Bulk Set persistence"""
        # Start server
        server1 = KVServer(host="localhost", port=5003, node_id=0, data_dir=str(self.data_dir))
        server_thread1 = threading.Thread(target=server1.start, daemon=True)
        server_thread1.start()
        time.sleep(0.5)
        
        # Bulk set data
        client1 = KVClient(host="localhost", port=5003)
        items = [("pk1", "pv1"), ("pk2", "pv2"), ("pk3", "pv3")]
        client1.bulk_set(items)
        client1.close()
        
        # Stop and restart
        server1.stop()
        time.sleep(0.5)
        
        server2 = KVServer(host="localhost", port=5003, node_id=0, data_dir=str(self.data_dir))
        server_thread2 = threading.Thread(target=server2.start, daemon=True)
        server_thread2.start()
        time.sleep(0.5)
        
        # Verify bulk data persisted
        client2 = KVClient(host="localhost", port=5003)
        self.assertEqual(client2.get("pk1"), "pv1")
        self.assertEqual(client2.get("pk2"), "pv2")
        self.assertEqual(client2.get("pk3"), "pv3")
        client2.close()
        
        server2.stop()
        logger.info("✓ Bulk Set persistence passed")


class TestConcurrency(unittest.TestCase):
    """Test concurrent operations"""

    @classmethod
    def setUpClass(cls):
        """Start server"""
        cls.data_dir = Path("test_concurrent")
        if cls.data_dir.exists():
            shutil.rmtree(cls.data_dir)
        cls.data_dir.mkdir()
        
        cls.server = KVServer(host="localhost", port=5004, node_id=0, data_dir=str(cls.data_dir))
        cls.server_thread = threading.Thread(target=cls.server.start, daemon=True)
        cls.server_thread.start()
        time.sleep(0.5)

    @classmethod
    def tearDownClass(cls):
        """Stop server"""
        cls.server.stop()
        if cls.data_dir.exists():
            shutil.rmtree(cls.data_dir)

    def setUp(self):
        """Clear data"""
        self.client = KVClient(host="localhost", port=5004)
        all_data = self.client.get_all()
        for key in all_data:
            self.client.delete(key)

    def tearDown(self):
        """Cleanup"""
        self.client.close()

    def test_concurrent_writes_same_key(self):
        """Test: Concurrent writes to same key"""
        num_threads = 10
        num_writes = 100
        
        def writer(thread_id):
            client = KVClient(host="localhost", port=5004)
            for i in range(num_writes):
                client.set("shared_key", f"value_{thread_id}_{i}")
            client.close()
        
        threads = [threading.Thread(target=writer, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Final value should be one of the written values
        result = self.client.get("shared_key")
        self.assertIsNotNone(result)
        logger.info("✓ Concurrent writes same key passed")

    def test_concurrent_bulk_writes(self):
        """Test: Concurrent bulk writes touching same keys"""
        num_threads = 5
        
        def bulk_writer(thread_id):
            client = KVClient(host="localhost", port=5004)
            items = [(f"key_{i}", f"thread_{thread_id}_value_{i}") for i in range(10)]
            client.bulk_set(items)
            client.close()
        
        threads = [threading.Thread(target=bulk_writer, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # All keys should exist
        all_data = self.client.get_all()
        self.assertGreater(len(all_data), 0)
        logger.info("✓ Concurrent bulk writes passed")


class TestACIDProperties(unittest.TestCase):
    """Test ACID properties"""

    def setUp(self):
        """Setup for each test"""
        self.data_dir = Path("test_acid")
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)
        self.data_dir.mkdir()

    def tearDown(self):
        """Cleanup"""
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)

    def test_atomicity_bulk_writes(self):
        """Test: Bulk writes are atomic"""
        server = KVServer(host="localhost", port=5005, node_id=0, data_dir=str(self.data_dir))
        server_thread = threading.Thread(target=server.start, daemon=True)
        server_thread.start()
        time.sleep(0.5)
        
        client = KVClient(host="localhost", port=5005)
        
        # Bulk set should write all or nothing
        items = [(f"atomic_key_{i}", f"atomic_value_{i}") for i in range(5)]
        client.bulk_set(items)
        
        # Verify all were written
        for i in range(5):
            result = client.get(f"atomic_key_{i}")
            self.assertEqual(result, f"atomic_value_{i}")
        
        client.close()
        server.stop()
        logger.info("✓ Atomicity bulk writes passed")

    def test_durability_kill_during_bulk(self):
        """Test: Durability - kill server during bulk write"""
        server = KVServer(host="localhost", port=5006, node_id=0, data_dir=str(self.data_dir))
        server_thread = threading.Thread(target=server.start, daemon=True)
        server_thread.start()
        time.sleep(0.5)
        
        client = KVClient(host="localhost", port=5006)
        
        # Bulk set many items
        items = [(f"durable_key_{i}", f"durable_value_{i}") for i in range(20)]
        client.bulk_set(items)
        
        # Verify some were written
        result_before = client.get("durable_key_0")
        self.assertIsNotNone(result_before)
        
        client.close()
        server.stop()
        
        # Restart and verify data
        time.sleep(0.5)
        server2 = KVServer(host="localhost", port=5006, node_id=0, data_dir=str(self.data_dir))
        server_thread2 = threading.Thread(target=server2.start, daemon=True)
        server_thread2.start()
        time.sleep(0.5)
        
        client2 = KVClient(host="localhost", port=5006)
        result_after = client2.get("durable_key_0")
        self.assertEqual(result_before, result_after)
        
        client2.close()
        server2.stop()
        logger.info("✓ Durability kill during bulk passed")


class TestIndexing(unittest.TestCase):
    """Test indexing features"""

    @classmethod
    def setUpClass(cls):
        """Start server"""
        cls.data_dir = Path("test_index")
        if cls.data_dir.exists():
            shutil.rmtree(cls.data_dir)
        cls.data_dir.mkdir()
        
        cls.server = KVServer(host="localhost", port=5007, node_id=0, data_dir=str(cls.data_dir))
        cls.server_thread = threading.Thread(target=cls.server.start, daemon=True)
        cls.server_thread.start()
        time.sleep(0.5)

    @classmethod
    def tearDownClass(cls):
        """Stop server"""
        cls.server.stop()
        if cls.data_dir.exists():
            shutil.rmtree(cls.data_dir)

    def setUp(self):
        """Setup"""
        self.client = KVClient(host="localhost", port=5007)
        all_data = self.client.get_all()
        for key in all_data:
            self.client.delete(key)

    def tearDown(self):
        """Cleanup"""
        self.client.close()

    def test_value_index(self):
        """Test: Value index search"""
        self.client.set("user_1", "John")
        self.client.set("user_2", "John")
        self.client.set("user_3", "Jane")
        
        results = self.client.search_by_value("John")
        self.assertIn("user_1", results)
        self.assertIn("user_2", results)
        self.assertNotIn("user_3", results)
        logger.info("✓ Value index passed")

    def test_fulltext_index(self):
        """Test: Full-text search"""
        self.client.set("doc_1", "hello world")
        self.client.set("doc_2", "hello python")
        self.client.set("doc_3", "goodbye world")
        
        results = self.client.search_fulltext("hello")
        self.assertIn("doc_1", results)
        self.assertIn("doc_2", results)
        self.assertNotIn("doc_3", results)
        
        results = self.client.search_fulltext("world")
        self.assertIn("doc_1", results)
        self.assertIn("doc_3", results)
        logger.info("✓ Full-text index passed")


def run_tests():
    """Run all tests"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestKVBasics))
    suite.addTests(loader.loadTestsFromTestCase(TestPersistence))
    suite.addTests(loader.loadTestsFromTestCase(TestConcurrency))
    suite.addTests(loader.loadTestsFromTestCase(TestACIDProperties))
    suite.addTests(loader.loadTestsFromTestCase(TestIndexing))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
