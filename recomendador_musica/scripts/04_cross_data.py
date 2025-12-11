"""
04_cross_data.py

Cruza información entre MongoDB (atributos musicales) y Neo4J (relaciones).
Genera una recomendación híbrida basada en:
- Géneros compartidos
- Popularidad de canciones
"""

from neo4j import GraphDatabase
from pymongo import MongoClient
from config import NEO4J_CONFIG, MONGODB_CONFIG


# ============================================================
# CONEXIONES A BASES DE DATOS
# ============================================================

def connect_neo4j():
    return GraphDatabase.driver(
        NEO4J_CONFIG["uri"],
        auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"])
    )

def connect_mongodb():
    client = MongoClient(MONGODB_CONFIG["uri"])
    db = client[MONGODB_CONFIG["database"]]
    return client, db


# ============================================================
# CONSULTAS A NEO4J
# ============================================================

def get_similar_artists(driver, artist_name):
    """
    Devuelve artistas que comparten géneros con el artista base.
    Añade: géneros compartidos y número de coincidencias.
    """

    query = """
    MATCH (a:Artist {name: $artist})-[:PLAYS_GENRE]->(g:Genre)
    MATCH (other:Artist)-[:PLAYS_GENRE]->(g)
    WHERE other <> a
    WITH other, COLLECT(DISTINCT g.name) AS shared_genres
    RETURN other.name AS artist,
           shared_genres AS genres,
           SIZE(shared_genres) AS matches
    ORDER BY matches DESC
    LIMIT 20;
    """

    with driver.session() as session:
        result = session.run(query, artist=artist_name)
        return [
            {
                "artist": record["artist"],
                "genres": record["genres"],
                "matches": record["matches"]
            }
            for record in result
        ]


# ============================================================
# CONSULTAS A MONGODB
# ============================================================

def get_top_songs_for_artists(db, artists_list):
    """
    Obtiene canciones populares desde MongoDB para los artistas recomendados.
    """

    pipeline = [
        {"$match": {"artists": {"$in": artists_list}}},
        {"$sort": {"popularity": -1}},
        {"$limit": 15},
        {
            "$project": {
                "track_name": 1,
                "artists": 1,
                "popularity": 1,
                "track_genre": 1,
                "_id": 0
            }
        }
    ]

    return list(db.tracks.aggregate(pipeline))


# ============================================================
# FLUJO PRINCIPAL
# ============================================================

def main():

    print("=" * 80)
    print("RECOMENDACIÓN HÍBRIDA (MongoDB + Neo4J)")
    print("=" * 80)

    # --- conexiones ---
    neo = connect_neo4j()
    mongo_client, mongo_db = connect_mongodb()

    # --- input del usuario ---
    artista_base = input("\nIngresa un artista para generar recomendaciones: ").strip()

    print("\nBuscando artistas similares en Neo4J...")

    # --- obtener artistas similares ---
    similar_artists = get_similar_artists(neo, artista_base)

    if not similar_artists:
        print("\n⚠ No se encontraron artistas similares.")
        return

    print("\n====================================================================")
    print(f"RECOMENDACIÓN HÍBRIDA BASADA EN: {artista_base.upper()}")
    print("====================================================================")

    print("\nArtistas similares encontrados (Neo4J):")
    print("--------------------------------------------------------------------")

    for item in similar_artists:
        artista = item["artist"]
        generos = ", ".join(item["genres"])
        count = item["matches"]

        print(f"{artista} — comparte {count} género(s): {generos}")

    # --- obtener canciones recomendadas desde MongoDB ---
    artists_only = [item["artist"] for item in similar_artists]
    songs = get_top_songs_for_artists(mongo_db, artists_only)

    print("\n\nCanciones recomendadas (MongoDB + Neo4J):")
    print("--------------------------------------------------------------------")

    if not songs:
        print("⚠ No se encontraron canciones en MongoDB para estos artistas.")
    else:
        for s in songs:
            print(f"{s['track_name']} — {s['artists']} (Popularidad: {s['popularity']})")

    # cerrar conexiones
    mongo_client.close()
    neo.close()


# ============================================================

if __name__ == "__main__":
    main()
