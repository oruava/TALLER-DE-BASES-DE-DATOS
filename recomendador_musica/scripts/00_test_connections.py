#!/usr/bin/env python3
"""
Test database connections to MongoDB and Neo4j.

This script verifies connectivity to both databases using configuration
from the .env file via config.py module.
"""

import sys
import os

# Add parent directory to path to import config
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import NEO4J_CONFIG, MONGODB_CONFIG


def test_mongodb():
    """Test MongoDB connection."""
    print("\n" + "="*80)
    print("Testing MongoDB Connection")
    print("="*80)
    
    try:
        from pymongo import MongoClient
        
        uri = MONGODB_CONFIG['uri']
        db_name = MONGODB_CONFIG['database']
        
        print(f"Connecting to: {uri}")
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        
        # Verify connection
        client.admin.command('ping')
        print("SUCCESS: Connected to MongoDB")
        
        # Server information
        server_info = client.server_info()
        print(f"  Version: {server_info['version']}")
        
        # List databases
        dbs = client.list_database_names()
        print(f"  Databases: {', '.join(dbs)}")
        
        # Check if target database exists
        if db_name in dbs:
            db = client[db_name]
            collections = db.list_collection_names()
            print(f"  Collections in '{db_name}': {', '.join(collections) if collections else 'None'}")
        else:
            print(f"  Database '{db_name}' does not exist yet (will be created on first insert)")
        
        client.close()
        return True
        
    except ImportError:
        print("ERROR: pymongo module not installed")
        print("  Solution: pip install pymongo")
        return False
    except Exception as e:
        print(f"ERROR: {e}")
        print("\nTroubleshooting:")
        print("  1. Check container status: docker ps | grep mongodb")
        print("  2. Start containers: docker-compose up -d")
        print("  3. Check logs: docker logs music-mongodb")
        return False


def test_neo4j():
    """Test Neo4j connection."""
    print("\n" + "="*80)
    print("Testing Neo4j Connection")
    print("="*80)
    
    try:
        from neo4j import GraphDatabase
        
        uri = NEO4J_CONFIG['uri']
        user = NEO4J_CONFIG['user']
        password = NEO4J_CONFIG['password']
        
        print(f"Connecting to: {uri}")
        print(f"  User: {user}")
        
        driver = GraphDatabase.driver(uri, auth=(user, password))
        
        # Verify connection
        driver.verify_connectivity()
        print("SUCCESS: Connected to Neo4j")
        
        # Server information
        with driver.session() as session:
            # Get version
            result = session.run("CALL dbms.components() YIELD name, versions, edition")
            for record in result:
                print(f"  Version: {record['versions'][0]}")
                print(f"  Edition: {record['edition']}")
            
            # Count nodes
            result = session.run("MATCH (n) RETURN count(n) as count")
            node_count = result.single()['count']
            print(f"  Total nodes: {node_count:,}")
            
            # Count by label if nodes exist
            if node_count > 0:
                result = session.run("""
                    MATCH (n)
                    RETURN labels(n)[0] as label, count(n) as count
                    ORDER BY count DESC
                    LIMIT 5
                """)
                print("  Top node labels:")
                for record in result:
                    if record['label']:
                        print(f"    - {record['label']}: {record['count']:,}")
        
        driver.close()
        return True
        
    except ImportError:
        print("ERROR: neo4j module not installed")
        print("  Solution: pip install neo4j")
        return False
    except Exception as e:
        print(f"ERROR: {e}")
        print("\nTroubleshooting:")
        print("  1. Check container status: docker ps | grep neo4j")
        print("  2. Start containers: docker-compose up -d")
        print("  3. Wait 30 seconds for Neo4j to initialize")
        print("  4. Check logs: docker logs music-neo4j")
        return False


def print_access_urls():
    """Print web interface URLs."""
    print("\nWeb Interfaces:")
    print("  - Neo4j Browser: http://localhost:7474")
    print(f"    Credentials: {NEO4J_CONFIG['user']} / {NEO4J_CONFIG['password']}")
    print("  - Mongo Express: http://localhost:8081")
    print("    Credentials: admin / admin")


def main():
    """Main function to test all database connections."""
    print("="*80)
    print("DATABASE CONNECTION TEST")
    print("="*80)
    print("Configuration loaded from .env file")
    
    # Test databases
    mongodb_ok = test_mongodb()
    neo4j_ok = test_neo4j()
    
    # Summary
    print("\n" + "="*80)
    print("CONNECTION SUMMARY")
    print("="*80)
    
    status_mongodb = "CONNECTED" if mongodb_ok else "FAILED"
    status_neo4j = "CONNECTED" if neo4j_ok else "FAILED"
    
    print(f"MongoDB: {status_mongodb}")
    print(f"Neo4j:   {status_neo4j}")
    
    if mongodb_ok and neo4j_ok:
        print("\nStatus: All databases are ready")
        print_access_urls()
        print("\nNext step:")
        print("  python scripts/01_load_mongodb.py")
        return 0
    else:
        print("\nStatus: Some connections failed")
        print("Action: Review error messages above")
        return 1


if __name__ == "__main__":
    sys.exit(main())