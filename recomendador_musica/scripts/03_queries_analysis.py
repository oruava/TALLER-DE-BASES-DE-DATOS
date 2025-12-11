"""
03_queries_analysis.py

Realiza consultas de extracción de información para AMBAS bases:
 - MongoDB: estadísticas y análisis del dataset original.
 - Neo4j: análisis del grafo musical.

Este archivo cumple con:
✔ Punto 3 de la tarea: “Realizar consultas a CADA base de datos”.
✔ Separación clara de extracción desde MongoDB y desde Neo4j.
"""

from neo4j import GraphDatabase
from pymongo import MongoClient
from config import NEO4J_CONFIG, MONGODB_CONFIG


# ============================================================
#  CONEXIÓN A LAS BASES DE DATOS
# ============================================================

def connect_mongodb():
    client = MongoClient(MONGODB_CONFIG["uri"])
    db = client[MONGODB_CONFIG["database"]]
    return client, db


def connect_neo4j():
    return GraphDatabase.driver(
        NEO4J_CONFIG["uri"],
        auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"])
    )


# ============================================================
#  UTILIDAD PARA MOSTRAR CONSULTAS
# ============================================================

def print_title(title):
    print("\n" + "="*90)
    print(title)
    print("="*90)


# ============================================================
#  SECCIÓN A: CONSULTAS MONGODB
# ============================================================

def run_mongodb_queries():
    client, db = connect_mongodb()
    tracks = db["tracks"]

    print_title("A) CONSULTAS A MONGODB")

    # 1) Canciones más populares
    print_title("A1) Top 10 canciones más populares")
    for x in tracks.find().sort("popularity", -1).limit(10):
        print(f"{x['track_name']} ({x['popularity']}) – {x['artists']}")

    # 2) Energía promedio por género
    print_title("A2) Energía promedio por género (top 10)")
    pipeline = [
        {"$group": {"_id": "$track_genre", "energia_promedio": {"$avg": "$audio_features.energy"}}},
        {"$sort": {"energia_promedio": -1}},
        {"$limit": 10}
    ]
    for x in tracks.aggregate(pipeline):
        print(x)

    # 3) Conteo de canciones por artista
    print_title("A3) Artistas con más canciones (MongoDB)")
    pipeline = [
        {"$group": {"_id": "$artists", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}},
        {"$limit": 10}
    ]
    for x in tracks.aggregate(pipeline):
        print(x)

    # 4) Géneros más frecuentes
    print_title("A4) Géneros con más canciones (MongoDB)")
    pipeline = [
        {"$group": {"_id": "$track_genre", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}},
        {"$limit": 10}
    ]
    for x in tracks.aggregate(pipeline):
        print(x)

    client.close()


# ============================================================
#  SECCIÓN B: CONSULTAS NEO4J
# ============================================================

def run_neo4j_queries():
    driver = connect_neo4j()

    print_title("B) CONSULTAS DE EXTRACCIÓN A NEO4J (GRAFO MUSICAL)")

    with driver.session() as session:

        # 1) Conteo de nodos por etiqueta
        print_title("B1) Conteo de nodos por tipo")
        result = session.run("""
            MATCH (n)
            RETURN labels(n)[0] AS label, COUNT(*) AS total
            ORDER BY total DESC
        """)
        for r in result:
            print(r)

        # 2) Artistas con más canciones (grafo)
        print_title("B2) Artistas con más canciones")
        result = session.run("""
            MATCH (a:Artist)<-[:BY_ARTIST]-(s:Song)
            RETURN a.name AS artista, COUNT(s) AS canciones
            ORDER BY canciones DESC
            LIMIT 20
        """)
        for r in result:
            print(r)

        # 3) Géneros más conectados
        print_title("B3) Géneros con más canciones")
        result = session.run("""
            MATCH (g:Genre)<-[:IN_GENRE]-(s:Song)
            RETURN g.name AS genero, COUNT(s) AS total_canciones
            ORDER BY total_canciones DESC
            LIMIT 20
        """)
        for r in result:
            print(r)

        # 4) Recomendación simple por artista
        print_title("B4) Recomendación por artista")
        result = session.run("""
            MATCH (a:Artist)<-[:BY_ARTIST]-(s:Song)
            WITH a, COUNT(s) AS num
            ORDER BY num DESC
            LIMIT 1
            MATCH (a)<-[:BY_ARTIST]-(songs:Song)
            RETURN a.name AS artista_base, COLLECT(songs.track_name)[0..10] AS recomendaciones
        """)
        for r in result:
            print(r)

        # 5) Recomendación simple por género
        print_title("B5) Recomendación por género")
        result = session.run("""
            MATCH (g:Genre)<-[:IN_GENRE]-(s:Song)
            WITH g, COUNT(s) AS num
            ORDER BY num DESC
            LIMIT 1
            MATCH (g)<-[:IN_GENRE]-(songs:Song)
            RETURN g.name AS genero_base, COLLECT(songs.track_name)[0..10] AS canciones_similares
        """)
        for r in result:
            print(r)

    driver.close()


# ============================================================
#  MAIN
# ============================================================

def main():
    print_title("03 — EXTRACCIÓN DE INFORMACIÓN DESDE AMBAS BASES")
    run_mongodb_queries()
    run_neo4j_queries()
    print_title("FIN DE LA EXTRACCIÓN DE INFORMACIÓN")


if __name__ == "__main__":
    main()
