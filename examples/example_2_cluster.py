#!/usr/bin/env python
"""
Example 2: Three-Node Cluster with Leader Election
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import threading
import time
from src.cluster_server import ClusterServer
from src.client import KVClient

def main():
    print("=" * 60)
    print("Example 2: Three-Node Cluster with Leader Election")
    print("=" * 60)
    
    # Define cluster nodes
    cluster_nodes = [
        (0, "localhost", 6000),
        (1, "localhost", 6001),
        (2, "localhost", 6002),
    ]
    
    # Start cluster
    print("\n1. Starting 3-node cluster...")
    servers = []
    for node_id, host, port in cluster_nodes:
        server = ClusterServer(node_id, host, port, cluster_nodes)
        thread = threading.Thread(target=server.start, daemon=True)
        thread.start()
        servers.append(server)
        print(f"   Started node {node_id} on {host}:{port}")
    
    # Wait for leader election
    print("\n2. Waiting for leader election (3 seconds)...")
    time.sleep(3)
    
    # Create client (connected to node 0)
    print("\n3. Creating client...")
    client = KVClient(host="localhost", port=6000)
    
    # Write data
    print("\n4. Writing data to cluster...")
    for i in range(5):
        client.set(f"key_{i}", f"value_{i}")
        print(f"   Set key_{i} = value_{i}")
    
    # Bulk set
    print("\n5. Bulk set operation...")
    items = [(f"bulk_{i}", f"bulk_value_{i}") for i in range(3)]
    client.bulk_set(items)
    print(f"   Bulk set {len(items)} items")
    
    # Read data from different nodes
    print("\n6. Reading data from all nodes...")
    time.sleep(0.5)  # Wait for replication
    
    for node_id, host, port in cluster_nodes:
        try:
            node_client = KVClient(host=host, port=port)
            value = node_client.get("key_0")
            print(f"   Node {node_id}: key_0 = {value}")
            node_client.close()
        except Exception as e:
            print(f"   Node {node_id}: Error - {e}")
    
    # Demonstrate failure scenario
    print("\n7. Demonstrating failover (stopping primary)...")
    primary_index = 0
    print(f"   Stopping primary (node {primary_index})...")
    servers[primary_index].stop()
    
    print("   Waiting for new election (3 seconds)...")
    time.sleep(3)
    
    # Try writing to another node
    print("\n8. Writing after primary failure...")
    try:
        client2 = KVClient(host="localhost", port=6001)
        client2.set("after_failure", "success")
        print(f"   Successfully wrote to failover primary")
        client2.close()
    except Exception as e:
        print(f"   Could not write: {e}")
    
    # Cleanup
    print("\n9. Cleanup:")
    client.close()
    for i, server in enumerate(servers):
        if i != primary_index:
            server.stop()
    print("   All servers stopped")
    
    print("\n" + "=" * 60)
    print("Example 2 Complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()
