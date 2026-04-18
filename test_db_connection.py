#!/usr/bin/env python3
"""
Database connectivity diagnostic script.
Run this to verify if MySQL and MongoDB are accessible before starting the main application.
"""

import sys
from utils.settings import HOST, PORT, USERNAME, PASSWORD, DB, CONNECTION

print("=" * 60)
print("DATABASE CONNECTIVITY TEST")
print("=" * 60)

# Test MySQL connectivity
print("\n[1/2] Testing MySQL Connection...")
print(f"  Settings: {USERNAME}@{HOST}:{PORT}/{DB}")

try:
    import mysql.connector
    
    connection = mysql.connector.connect(
        host=HOST,
        port=int(PORT),
        user=USERNAME,
        password=PASSWORD,
        database=DB
    )
    
    cursor = connection.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    
    print("  ✓ MySQL connection successful!")
    print(f"  Query result: {result}")
    
    cursor.close()
    connection.close()
    
except ImportError:
    print("  ✗ mysql-connector-python not installed")
    print("  Run: pip install mysql-connector-python")
    sys.exit(1)
    
except Exception as e:
    print(f"  ✗ MySQL connection failed: {type(e).__name__}: {e}")
    print("\n  Troubleshooting:")
    print("  - Is MySQL running? (check localhost:3306)")
    print(f"  - Are credentials correct? ({USERNAME}/{PASSWORD})")
    print(f"  - Does database '{DB}' exist?")
    print("  - Run: mysql -h localhost -u adapter -p and test manually")

# Test MongoDB connectivity
print("\n[2/2] Testing MongoDB Connection...")
print(f"  Settings: {CONNECTION}")

try:
    from pymongo import MongoClient
    
    client = MongoClient(CONNECTION, serverSelectionTimeoutMS=5000)
    
    # Verify connection by pinging
    admin_db = client["admin"]
    admin_db.command("ping")
    
    print("  ✓ MongoDB connection successful!")
    
    # Check if target database exists
    db = client[DB]
    collections = db.list_collection_names()
    print(f"  Database '{DB}' has {len(collections)} collection(s)")
    if collections:
        print(f"  Collections: {', '.join(collections[:5])}")
    
    client.close()
    
except ImportError:
    print("  ✗ pymongo not installed")
    print("  Run: pip install pymongo")
    sys.exit(1)
    
except Exception as e:
    print(f"  ✗ MongoDB connection failed: {type(e).__name__}: {e}")
    print("\n  Troubleshooting:")
    print("  - Is MongoDB running? (check localhost:27017)")
    print(f"  - Is connection string correct? ({CONNECTION})")
    print("  - Run: mongosh and test manually if available")

print("\n" + "=" * 60)
print("DATABASE TESTS COMPLETE")
print("=" * 60)
print("\nNext, try running the main application:")
print("  python ./main.py")
