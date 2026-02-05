#!/usr/bin/env python
"""
Example 1: Basic Key-Value Store (Standalone)
"""
import threading
import time
from src.server import KVServer
from src.client import KVClient

def main():
    print("=" * 60)
    print("Example 1: Basic Key-Value Store")
    print("=" * 60)
    
    # Start server
    print("\n1. Starting server...")
    server = KVServer(host="localhost", port=5000, node_id=0)
    server_thread = threading.Thread(target=server.start, daemon=True)
    server_thread.start()
    time.sleep(0.5)
    
    # Create client
    print("2. Connecting client...")
    client = KVClient(host="localhost", port=5000)
    
    # Set and Get
    print("\n3. Set and Get operations:")
    client.set("user:1", {"name": "John", "age": 30})
    print(f"   Set user:1 = {{'name': 'John', 'age': 30}}")
    
    user = client.get("user:1")
    print(f"   Get user:1 = {user}")
    
    # Bulk Set
    print("\n4. Bulk Set operation:")
    items = [
        ("user:2", {"name": "Jane", "age": 25}),
        ("user:3", {"name": "Bob", "age": 35}),
        ("user:4", {"name": "Alice", "age": 28})
    ]
    client.bulk_set(items)
    print(f"   Bulk set {len(items)} users")
    
    # Get all
    print("\n5. Get all data:")
    all_data = client.get_all()
    for key, value in all_data.items():
        print(f"   {key}: {value}")
    
    # Search by value
    print("\n6. Search by value:")
    results = client.search_by_value({"name": "John", "age": 30})
    print(f"   Keys with value {{'name': 'John', 'age': 30}}: {results}")
    
    # Delete
    print("\n7. Delete operation:")
    client.delete("user:4")
    print(f"   Deleted user:4")
    print(f"   user:4 value now: {client.get('user:4')}")
    
    # Full-text search
    print("\n8. Full-text search:")
    client.set("doc:1", "python programming language")
    client.set("doc:2", "javascript web development")
    client.set("doc:3", "python machine learning")
    
    results = client.search_fulltext("python")
    print(f"   Documents with 'python': {results}")
    
    # Cleanup
    print("\n9. Cleanup:")
    client.close()
    server.stop()
    print("   Server and client closed")
    
    print("\n" + "=" * 60)
    print("Example 1 Complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()
