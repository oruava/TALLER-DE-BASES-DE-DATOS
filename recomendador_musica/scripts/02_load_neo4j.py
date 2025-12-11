"""
Load songs, artists and genres from MongoDB into Neo4j.
GRAFO EXTENDIDO:
    (:Song)-[:BY_ARTIST]->(:Artist)
    (:Song)-[:IN_GENRE]->(:Genre)
    (:Artist)-[:PLAYS_GENRE]->(:Genre)
"""

from neo4j import GraphDatabase
from pymongo import MongoClient
from config import NEO4J_CONFIG, MONGODB_CONFIG
from math import isnan

BATCH_SIZE = 500


def clean_value(value):
    """Convierte NaN o None en None (evita errores con MERGE)."""
    if value is None:
        return None
    try:
        if isinstance(value, float) and isnan(value):
            return None
    except:
        pass
    value = str(value).strip()
    return value if value else None


def connect_mongo():
    client = MongoClient(MONGODB_CONFIG['uri'])
    db = client[MONGODB_CONFIG['database']]
    return db


def connect_neo4j():
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'],
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    return driver


def insert_batch(tx, batch):
    query = """
    UNWIND $rows AS row
    
    MERGE (song:Song {track_id: row.track_id})
        SET song.track_name = row.track_name

    MERGE (artist:Artist {name: row.artist})
    
    MERGE (genre:Genre {name: row.genre})

    MERGE (song)-[:BY_ARTIST]->(artist)
    MERGE (song)-[:IN_GENRE]->(genre)
    MERGE (artist)-[:PLAYS_GENRE]->(genre)
    """

    tx.run(query, rows=batch)


def main():
    print("=" * 80)
    print("NEO4J DATA LOAD (OPTION 2 - EXTENDED GRAPH)")
    print("=" * 80)

    # Connect to MongoDB
    db = connect_mongo()
    collection = db["tracks"]
    total_docs = collection.count_documents({})
    print(f"Total documents in MongoDB: {total_docs:,}")

    # Connect to Neo4j
    driver = connect_neo4j()
    print("Connected to Neo4j")

    cursor = collection.find({}, {
        "track_id": 1,
        "track_name": 1,
        "artist_list": 1,
        "track_genre": 1
    })

    batch = []
    batch_num = 1

    with driver.session() as session:
        for doc in cursor:
            track_id = clean_value(doc.get("track_id"))
            track_name = clean_value(doc.get("track_name"))
            genre = clean_value(doc.get("track_genre"))

            if not track_id or not genre:
                continue

            # Puede haber múltiples artistas por canción
            artist_list = doc.get("artist_list", [])
            if not artist_list:
                continue

            for artist in artist_list:
                artist = clean_value(artist)
                if not artist:
                    continue

                batch.append({
                    "track_id": track_id,
                    "track_name": track_name,
                    "artist": artist,
                    "genre": genre
                })

            if len(batch) >= BATCH_SIZE:
                print(f"✔ Inserting batch {batch_num} ({len(batch)} rows)")
                session.execute_write(insert_batch, batch)
                batch = []
                batch_num += 1

        # Final batch
        if batch:
            print(f"✔ Inserting FINAL batch {batch_num} ({len(batch)} rows)")
            session.execute_write(insert_batch, batch)

    print("\n🎉 EXTENDED GRAPH LOADED SUCCESSFULLY")
    print("=" * 80)

    driver.close()


if __name__ == "__main__":
    main()
