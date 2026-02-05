#!/usr/bin/env python
"""
Example 3: Master-Less Peer-to-Peer Cluster with Semantic Search
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import threading
import time
from src.masterless_server import MasterlessServer
from src.masterless_client import MasterlessClient

def main():
    print("=" * 60)
    print("Example 3: Master-Less P2P Cluster with Semantic Search")
    print("=" * 60)
    
    # Define cluster
    cluster_nodes = [
        (0, "localhost", 7000),
        (1, "localhost", 7001),
        (2, "localhost", 7002),
    ]
    
    # Start cluster
    print("\n1. Starting master-less 3-node cluster...")
    servers = []
    for node_id, host, port in cluster_nodes:
        server = MasterlessServer(node_id, host, port, cluster_nodes)
        thread = threading.Thread(target=server.start, daemon=True)
        thread.start()
        servers.append(server)
        print(f"   Started node {node_id} on {host}:{port}")
    
    time.sleep(1)
    
    # Create clients to different nodes
    print("\n2. Creating clients to different nodes...")
    client0 = MasterlessClient(host="localhost", port=7000)
    client1 = MasterlessClient(host="localhost", port=7001)
    client2 = MasterlessClient(host="localhost", port=7002)
    
    # Add documents from different nodes (demonstrating any node can write)
    print("\n3. Adding documents from different nodes...")
    docs = {
        "doc_0": "machine learning algorithms",
        "doc_1": "deep neural networks",
        "doc_2": "artificial intelligence systems",
        "doc_3": "data science analytics",
        "doc_4": "python programming language"
    }
    
    # Write from different nodes
    client0.set("doc_0", docs["doc_0"])
    print(f"   Node 0: Set doc_0 = '{docs['doc_0']}'")
    
    client1.set("doc_1", docs["doc_1"])
    print(f"   Node 1: Set doc_1 = '{docs['doc_1']}'")
    
    client2.set("doc_2", docs["doc_2"])
    print(f"   Node 2: Set doc_2 = '{docs['doc_2']}'")
    
    # Bulk set
    print("\n4. Bulk set from node 0...")
    remaining = [("doc_3", docs["doc_3"]), ("doc_4", docs["doc_4"])]
    client0.bulk_set(remaining)
    print(f"   Bulk set {len(remaining)} documents")
    
    # Wait for replication
    print("\n5. Waiting for replication (0.5 seconds)...")
    time.sleep(0.5)
    
    # Read from different nodes (eventual consistency)
    print("\n6. Reading from different nodes (eventual consistency)...")
    for node_id, client in enumerate([client0, client1, client2]):
        doc = client.get("doc_0")
        print(f"   Node {node_id}: doc_0 = '{doc}'")
    
    # Full-text search
    print("\n7. Full-text search...")
    results = client0.search_fulltext("neural")
    print(f"   Documents with 'neural': {results}")
    
    # Semantic search (word embeddings)
    print("\n8. Semantic search using word embeddings...")
    query = "machine learning"
    results = client0.search_semantic(query, top_k=3)
    print(f"   Query: '{query}'")
    print(f"   Results (by similarity):")
    for doc_id, similarity in results:
        print(f"      {doc_id}: similarity = {similarity:.3f}")
    
    # Value search
    print("\n9. Value search...")
    client0.set("user:john", "engineer")
    client0.set("user:jane", "engineer")
    client0.set("user:bob", "manager")
    
    time.sleep(0.3)
    
    results = client0.search_by_value("engineer")
    print(f"   Keys with value 'engineer': {results}")
    
    # Node metadata
    print("\n10. Node metadata...")
    for node_id, client in enumerate([client0, client1, client2]):
        metadata = client.get_metadata()
        print(f"   Node {node_id}:")
        print(f"      Data count: {metadata['data_count']}")
        print(f"      Vector clock: {metadata['vector_clock']}")
    
    # Delete (with tombstones)
    print("\n11. Delete with tombstones...")
    client0.delete("doc_0")
    print(f"   Deleted doc_0 from node 0")
    
    time.sleep(0.3)
    
    result = client1.get("doc_0")
    print(f"   Node 1: doc_0 = {result} (deleted on node 0, replicated as None)")
    
    # Cleanup
    print("\n12. Cleanup:")
    client0.close()
    client1.close()
    client2.close()
    
    for server in servers:
        server.stop()
    
    print("   All clients and servers closed")
    
    print("\n" + "=" * 60)
    print("Example 3 Complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()
