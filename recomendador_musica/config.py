"""
Database connection configuration.
Loads credentials and settings from .env file in project root.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
project_root = Path(__file__).parent
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path)

# Neo4j Configuration
NEO4J_CONFIG = {
    'uri': os.getenv('NEO4J_URI', 'bolt://localhost:7687'),
    'user': os.getenv('NEO4J_USER', 'neo4j'),
    'password': os.getenv('NEO4J_PASSWORD', 'neo4j123'),
    'database': 'neo4j'
}

# MongoDB Configuration
MONGODB_CONFIG = {
    'uri': os.getenv('MONGODB_URI', 'mongodb://localhost:27017/'),
    'database': os.getenv('MONGODB_DB', 'music_db')
}

# Dataset Configuration
DATA_CONFIG = {
    'dataset_path': project_root / 'dataset' / 'dataset.csv',
    'batch_size': 1000
}

PATHS = {
    'root': project_root,
    'dataset': project_root / 'dataset',
    'scripts': project_root / 'scripts',
    'results': project_root / 'results',
    'docs': project_root / 'docs',
}
