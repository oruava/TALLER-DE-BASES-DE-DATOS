"""
Script to load Spotify tracks data into MongoDB.

This script:
1. Reads the CSV dataset
2. Transforms data into document format
3. Inserts documents into MongoDB in batches
4. Creates indexes for optimized queries
"""

import pandas as pd
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError, DuplicateKeyError
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path to import config
sys.path.append(str(Path(__file__).parent))
from config import MONGODB_CONFIG, DATA_CONFIG


def connect_mongodb():
    """Establish connection to MongoDB."""
    mongo_uri = MONGODB_CONFIG['uri']
    client = MongoClient(mongo_uri)
    db = client[MONGODB_CONFIG['database']]
    return client, db


def load_dataset():
    """Load and validate the dataset."""
    dataset_path = DATA_CONFIG['dataset_path']
    
    print(f"\nLoading dataset from: {dataset_path}")
    
    if not dataset_path.exists():
        print(f"ERROR: Dataset not found at {dataset_path}")
        print("\nPlease ensure the dataset.csv file is in the correct location.")
        sys.exit(1)
    
    df = pd.read_csv(dataset_path)
    print(f"Dataset loaded: {len(df):,} rows")
    
    return df


def transform_row_to_document(row):
    """Transform a DataFrame row into a MongoDB document."""
    
    # Parse artists (can be comma-separated)
    artists_str = str(row.get('artists', ''))
    artist_list = [a.strip() for a in artists_str.split(',') if a.strip()]
    
    # Build audio features subdocument
    audio_features = {}
    audio_feature_columns = [
        'danceability', 'energy', 'valence', 'tempo', 'loudness',
        'speechiness', 'acousticness', 'instrumentalness', 'liveness',
        'key', 'mode', 'time_signature'
    ]
    
    for col in audio_feature_columns:
        if col in row.index and pd.notna(row[col]):
            audio_features[col] = float(row[col]) if col != 'key' and col != 'mode' and col != 'time_signature' else int(row[col])
    
    # Build main document
    document = {
        'track_id': str(row.get('track_id', '')),
        'track_name': str(row.get('track_name', '')),
        'artists': artists_str,
        'artist_list': artist_list,
        'album_name': str(row.get('album_name', '')),
        'track_genre': str(row.get('track_genre', '')),
        'popularity': int(row.get('popularity', 0)),
        'duration_ms': int(row.get('duration_ms', 0)),
        'explicit': bool(row.get('explicit', False)),
        'audio_features': audio_features
    }
    
    return document


def insert_documents_batch(collection, documents, batch_num, total_batches):
    """Insert a batch of documents into MongoDB."""
    try:
        result = collection.insert_many(documents, ordered=False)
        inserted_count = len(result.inserted_ids)
        print(f"  Batch {batch_num}/{total_batches}: Inserted {inserted_count:,} documents")
        return inserted_count, 0
    except BulkWriteError as e:
        inserted_count = e.details['nInserted']
        duplicate_count = len(e.details['writeErrors'])
        print(f"  Batch {batch_num}/{total_batches}: Inserted {inserted_count:,} documents, {duplicate_count} duplicates skipped")
        return inserted_count, duplicate_count


def create_indexes(collection):
    """Create indexes for optimized queries."""
    print("\nCreating indexes...")
    
    indexes = [
        ('track_id_unique', [('track_id', ASCENDING)], {'unique': True}),
        ('track_genre', [('track_genre', ASCENDING)], {}),
        ('popularity', [('popularity', DESCENDING)], {}),
        ('artists', [('artists', ASCENDING)], {}),
        ('audio_energy', [('audio_features.energy', ASCENDING)], {}),
        ('audio_danceability', [('audio_features.danceability', ASCENDING)], {}),
        ('audio_valence', [('audio_features.valence', ASCENDING)], {}),
        ('audio_tempo', [('audio_features.tempo', ASCENDING)], {}),
        ('genre_popularity', [('track_genre', ASCENDING), ('popularity', DESCENDING)], {}),
    ]
    
    for index_name, keys, options in indexes:
        try:
            collection.create_index(keys, name=index_name, **options)
            print(f"  ✓ Created index: {index_name}")
        except Exception as e:
            if 'already exists' in str(e):
                print(f"  - Index already exists: {index_name}")
            else:
                print(f"  ✗ Failed to create index {index_name}: {e}")


def main():
    print(">>> Iniciando script 01_load_mongodb.py")
    """Main execution function."""
    start_time = datetime.now()
    
    print("=" * 80)
    print("MONGODB DATA LOAD")
    print("=" * 80)
    
    # Connect to MongoDB
    print("\nConnecting to MongoDB...")
    try:
        client, db = connect_mongodb()
        collection = db['tracks']
        print(f"Connected to database: {MONGODB_CONFIG['database']}")
        print(f"Collection: tracks")
    except Exception as e:
        print(f"ERROR: Failed to connect to MongoDB: {e}")
        sys.exit(1)
    
    # Load dataset
    df = load_dataset()
    
    # Check if collection already has data
    existing_count = collection.count_documents({})
    if existing_count > 0:
        print(f"\nWARNING: Collection already contains {existing_count:,} documents")
        response = input("Do you want to drop the collection and reload? (yes/no): ")
        if response.lower() == 'yes':
            collection.drop()
            print("Collection dropped")
        else:
            print("Skipping data load. Will only create/update indexes.")
            create_indexes(collection)
            client.close()
            return
    
    # Transform and insert data
    print("\nTransforming and inserting documents...")
    print(f"Total documents to insert: {len(df):,}")
    
    BATCH_SIZE = 1000
    total_batches = (len(df) + BATCH_SIZE - 1) // BATCH_SIZE
    
    total_inserted = 0
    total_duplicates = 0
    
    for i in range(0, len(df), BATCH_SIZE):
        batch_df = df.iloc[i:i + BATCH_SIZE]
        documents = [transform_row_to_document(row) for _, row in batch_df.iterrows()]
        
        batch_num = (i // BATCH_SIZE) + 1
        inserted, duplicates = insert_documents_batch(collection, documents, batch_num, total_batches)
        
        total_inserted += inserted
        total_duplicates += duplicates
    
    # Create indexes
    create_indexes(collection)
    
    # Final statistics
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "=" * 80)
    print("LOAD COMPLETE")
    print("=" * 80)
    print(f"Total documents inserted: {total_inserted:,}")
    print(f"Duplicates skipped: {total_duplicates:,}")
    print(f"Final collection size: {collection.count_documents({}):,}")
    print(f"Time elapsed: {duration:.2f} seconds")
    
    # Sample documents
    print("\nSample documents:")
    sample_docs = collection.find().limit(2)
    for i, doc in enumerate(sample_docs, 1):
        print(f"\nDocument {i}:")
        print(f"  Track: {doc['track_name']}")
        print(f"  Artist: {doc['artists']}")
        print(f"  Genre: {doc['track_genre']}")
        print(f"  Popularity: {doc['popularity']}")
        print(f"  Audio Features: {len(doc['audio_features'])} features")
    
    # Close connection
    client.close()
    print("\nMongoDB connection closed")
    
    print("\n" + "=" * 80)
    print("Next step: python scripts/02_load_neo4j.py")
    print("=" * 80)


if __name__ == "__main__":
    main()