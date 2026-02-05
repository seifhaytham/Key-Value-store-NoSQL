"""
Benchmarks for Key-Value store
"""
import time
import threading
import subprocess
import os
import sys
import shutil
from pathlib import Path
from typing import List, Tuple
from src.client import KVClient
from src.server import KVServer
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class WritePerformanceBenchmark:
    """Benchmark write throughput with different data sizes"""

    def __init__(self, host: str = "localhost", port: int = 5010):
        self.host = host
        self.port = port
        self.data_dir = Path("bench_write")
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)
        self.data_dir.mkdir()

    def setup(self):
        """Start server"""
        self.server = KVServer(host=self.host, port=self.port, node_id=0, data_dir=str(self.data_dir))
        self.server_thread = threading.Thread(target=self.server.start, daemon=True)
        self.server_thread.start()
        time.sleep(0.5)

    def teardown(self):
        """Stop server"""
        self.server.stop()
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)

    def benchmark_write_throughput(self, num_writes: int = 1000):
        """Benchmark write throughput"""
        self.setup()
        
        client = KVClient(host=self.host, port=self.port)
        
        # Warm up
        client.set("warmup", "value")
        
        start_time = time.time()
        for i in range(num_writes):
            client.set(f"key_{i}", f"value_{i}")
        elapsed = time.time() - start_time
        
        throughput = num_writes / elapsed
        
        client.close()
        self.teardown()
        
        logger.info(f"Write Throughput ({num_writes} writes): {throughput:.2f} ops/sec")
        logger.info(f"  Time: {elapsed:.2f}s, Avg: {(elapsed*1000/num_writes):.2f}ms per op")
        
        return throughput

    def benchmark_write_with_preloaded_data(self):
        """Benchmark write throughput with pre-loaded data"""
        self.setup()
        
        client = KVClient(host=self.host, port=self.port)
        
        # Pre-populate with different amounts of data
        data_sizes = [100, 1000, 10000]
        results = {}
        
        for data_size in data_sizes:
            # Clear existing data
            all_data = client.get_all()
            for key in all_data:
                client.delete(key)
            
            # Pre-populate
            logger.info(f"Pre-populating with {data_size} items...")
            items = [(f"preload_{i}", f"value_{i}") for i in range(data_size)]
            
            # Bulk insert in batches
            batch_size = 100
            for batch_start in range(0, data_size, batch_size):
                batch_end = min(batch_start + batch_size, data_size)
                batch = items[batch_start:batch_end]
                client.bulk_set(batch)
            
            # Now benchmark writes
            time.sleep(0.1)  # Let server settle
            num_writes = 1000
            start_time = time.time()
            for i in range(num_writes):
                client.set(f"write_{i}", f"value_{i}")
            elapsed = time.time() - start_time
            
            throughput = num_writes / elapsed
            results[data_size] = throughput
            
            logger.info(f"  With {data_size} pre-loaded items: {throughput:.2f} ops/sec")
        
        client.close()
        self.teardown()
        
        return results

    def benchmark_bulk_set_throughput(self):
        """Benchmark bulk set throughput"""
        self.setup()
        
        client = KVClient(host=self.host, port=self.port)
        
        # Benchmark different batch sizes
        batch_sizes = [10, 50, 100, 500]
        total_items = 5000
        results = {}
        
        for batch_size in batch_sizes:
            # Clear data
            all_data = client.get_all()
            for key in all_data:
                client.delete(key)
            
            # Bulk set with batches
            num_batches = total_items // batch_size
            start_time = time.time()
            
            for batch_num in range(num_batches):
                items = [(f"bulk_{batch_num}_{i}", f"value_{i}") for i in range(batch_size)]
                client.bulk_set(items)
            
            elapsed = time.time() - start_time
            throughput = total_items / elapsed
            results[batch_size] = throughput
            
            logger.info(f"Bulk Set (batch_size={batch_size}): {throughput:.2f} ops/sec")
        
        client.close()
        self.teardown()
        
        return results


class DurabilityBenchmark:
    """Benchmark durability and data loss under failures"""

    def __init__(self, host: str = "localhost", port: int = 5011):
        self.host = host
        self.port = port
        self.data_dir = Path("bench_durability")
        self.acknowledged_keys = set()
        self.lost_keys = set()

    def setup(self):
        """Start server"""
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)
        self.data_dir.mkdir()
        
        self.server = KVServer(host=self.host, port=self.port, node_id=0, data_dir=str(self.data_dir))
        self.server_thread = threading.Thread(target=self.server.start, daemon=True)
        self.server_thread.start()
        time.sleep(0.5)

    def teardown(self):
        """Stop server"""
        try:
            self.server.stop()
        except:
            pass
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)

    def benchmark_durability_on_crash(self):
        """Benchmark durability: add data, crash server, check data loss"""
        self.setup()
        
        client = KVClient(host=self.host, port=self.port)
        
        num_writes = 1000
        self.acknowledged_keys = set()
        self.lost_keys = set()
        write_error_count = 0
        
        def write_thread():
            nonlocal write_error_count
            for i in range(num_writes):
                try:
                    client.set(f"durable_key_{i}", f"value_{i}")
                    self.acknowledged_keys.add(f"durable_key_{i}")
                except Exception as e:
                    logger.warning(f"Write error: {e}")
                    write_error_count += 1
        
        # Start writes
        writer = threading.Thread(target=write_thread)
        writer.start()
        
        # Let some writes happen
        time.sleep(0.2)
        
        # Kill server
        logger.info("Killing server...")
        self.server.stop()
        time.sleep(0.5)
        
        # Wait for writer to finish (will get errors)
        writer.join(timeout=5)
        
        # Restart server
        logger.info("Restarting server...")
        time.sleep(0.5)
        self.server = KVServer(host=self.host, port=self.port, node_id=0, data_dir=str(self.data_dir))
        self.server_thread = threading.Thread(target=self.server.start, daemon=True)
        self.server_thread.start()
        time.sleep(0.5)
        
        # Reconnect and check
        client2 = KVClient(host=self.host, port=self.port)
        
        for key in self.acknowledged_keys:
            value = client2.get(key)
            if value is None:
                self.lost_keys.add(key)
        
        data_loss = len(self.lost_keys)
        durability = (len(self.acknowledged_keys) - data_loss) / len(self.acknowledged_keys) * 100 if self.acknowledged_keys else 0
        
        logger.info(f"Durability Benchmark Results:")
        logger.info(f"  Writes attempted: {num_writes}")
        logger.info(f"  Acknowledged writes: {len(self.acknowledged_keys)}")
        logger.info(f"  Lost keys: {data_loss}")
        logger.info(f"  Durability: {durability:.2f}%")
        
        client2.close()
        self.teardown()
        
        return {
            'attempts': num_writes,
            'acknowledged': len(self.acknowledged_keys),
            'lost': data_loss,
            'durability': durability
        }

    def benchmark_concurrent_writes_with_crashes(self):
        """Benchmark concurrent writes with random server crashes"""
        self.setup()
        
        num_threads = 5
        writes_per_thread = 200
        crash_count = 3
        
        client = KVClient(host=self.host, port=self.port)
        self.acknowledged_keys = set()
        self.lost_keys = set()
        write_lock = threading.Lock()
        
        def write_thread(thread_id):
            local_client = KVClient(host=self.host, port=self.port)
            for i in range(writes_per_thread):
                try:
                    key = f"thread_{thread_id}_key_{i}"
                    local_client.set(key, f"value_{i}")
                    with write_lock:
                        self.acknowledged_keys.add(key)
                    time.sleep(0.001)  # Small delay
                except Exception as e:
                    pass
            local_client.close()
        
        # Start writers
        writers = [threading.Thread(target=write_thread, args=(i,)) for i in range(num_threads)]
        for w in writers:
            w.start()
        
        # Random crashes
        for _ in range(crash_count):
            time.sleep(0.3)
            logger.info(f"Crashing server (acknowledged: {len(self.acknowledged_keys)} keys)...")
            self.server.stop()
            time.sleep(0.2)
            
            self.server = KVServer(host=self.host, port=self.port, node_id=0, data_dir=str(self.data_dir))
            self.server_thread = threading.Thread(target=self.server.start, daemon=True)
            self.server_thread.start()
            time.sleep(0.3)
        
        # Wait for writers
        for w in writers:
            w.join()
        
        # Check data loss
        client2 = KVClient(host=self.host, port=self.port)
        for key in self.acknowledged_keys:
            if client2.get(key) is None:
                self.lost_keys.add(key)
        
        durability = (len(self.acknowledged_keys) - len(self.lost_keys)) / len(self.acknowledged_keys) * 100 if self.acknowledged_keys else 0
        
        logger.info(f"Concurrent Writes with Crashes:")
        logger.info(f"  Total threads: {num_threads}")
        logger.info(f"  Writes per thread: {writes_per_thread}")
        logger.info(f"  Crashes: {crash_count}")
        logger.info(f"  Acknowledged writes: {len(self.acknowledged_keys)}")
        logger.info(f"  Lost keys: {len(self.lost_keys)}")
        logger.info(f"  Durability: {durability:.2f}%")
        
        client2.close()
        self.teardown()
        
        return {
            'threads': num_threads,
            'writes_per_thread': writes_per_thread,
            'crashes': crash_count,
            'acknowledged': len(self.acknowledged_keys),
            'lost': len(self.lost_keys),
            'durability': durability
        }


def run_benchmarks():
    """Run all benchmarks"""
    logger.info("=" * 60)
    logger.info("WRITE PERFORMANCE BENCHMARKS")
    logger.info("=" * 60)
    
    write_bench = WritePerformanceBenchmark()
    
    logger.info("\n1. Basic Write Throughput (1000 writes):")
    write_bench.benchmark_write_throughput(1000)
    
    logger.info("\n2. Write Throughput with Pre-loaded Data:")
    write_bench.benchmark_write_with_preloaded_data()
    
    logger.info("\n3. Bulk Set Throughput:")
    write_bench.benchmark_bulk_set_throughput()
    
    logger.info("\n" + "=" * 60)
    logger.info("DURABILITY BENCHMARKS")
    logger.info("=" * 60)
    
    durability_bench = DurabilityBenchmark()
    
    logger.info("\n1. Durability on Server Crash:")
    durability_bench.benchmark_durability_on_crash()
    
    logger.info("\n2. Concurrent Writes with Random Crashes:")
    durability_bench.benchmark_concurrent_writes_with_crashes()
    
    logger.info("\n" + "=" * 60)
    logger.info("BENCHMARKS COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_benchmarks()
