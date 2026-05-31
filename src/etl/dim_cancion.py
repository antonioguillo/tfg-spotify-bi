"""
dim_cancion.py

ETL de la dimensión Canción. Fuentes:
  - canciones_features_kaggle.csv (HDFS Bronze): título, artista, álbum, duración.
  - Playlist1.json de cada usuario (HDFS Bronze): qué canciones están en playlists.

Pasos principales:
  1. Leer el CSV de features y seleccionar columnas de identidad.
  2. Calcular rangoDuracion como atributo derivado (bucketing de minutos).
  3. Cruzar con playlists de usuario para marcar el campo `playlist` (TINYINT 1/0).
  4. Generar ID autoincremental + fila centinela Desconocido (ID=-1).
  5. Persistir en Hive (formato Parquet).

Nota: el campo `playlist` es TINYINT (no BOOLEAN) para compatibilidad con
todas las versiones de Hive y con el motor OLAP Apache Kylin.
"""
from pyspark.sql.functions import (
    col, monotonically_increasing_id, lit, when, explode
)
from src.utils.spark_session import get_spark_session
from src.utils.paths import HDFS_FEATURES_CSV, HDFS_USUARIOS


def procesar_dim_cancion():
    spark = get_spark_session("ETL_Dimension_Cancion")

    # ==========================================================
    # 1. Leer canciones desde Bronze en HDFS
    # ==========================================================
    print("1. Leyendo dataset de canciones (HDFS Bronze)...")
    print(f"   Ruta: {HDFS_FEATURES_CSV}")
    df = spark.read.option("header", "true").option("inferSchema", "true") \
              .csv(HDFS_FEATURES_CSV)

    print("2. Seleccionando y limpiando columnas...")
    df_cancion = df.select(
        col("cancion").alias("titulo"),
        col("artista"),
        col("album"),
        col("duration_ms").cast("double")
    ).dropDuplicates(["titulo", "artista"])

    df_cancion = df_cancion.fillna("Desconocido", subset=["titulo", "artista", "album"])

    # ==========================================================
    # 3. Calcular rangoDuracion
    # ==========================================================
    print("3. Calculando rangoDuracion...")
    df_cancion = df_cancion.withColumn("duration_min", col("duration_ms") / 60000)
    df_cancion = df_cancion.withColumn("rangoDuracion",
        when(col("duration_min") <= 2,  "0-2 min")
        .when(col("duration_min") <= 4,  "2-4 min")
        .when(col("duration_min") <= 7,  "4-7 min")
        .when(col("duration_min") <= 11, "7-11 min")
        .otherwise("11+ min")
    ).drop("duration_min", "duration_ms")

    # ==========================================================
    # 4. Cruzar con playlists del usuario desde HDFS
    #
    # Los Playlist1.json están en HDFS Bronze:
    # /user/spotify_bi/bronze/usuarios/{USER}/Playlist1.json
    #
    # Estructura JSON:
    # { "playlists": [ { "items": [ { "track": {
    #     "trackName": "...", "artistName": "..." } } ] } ] }
    # ==========================================================
    print("4. Cruzando con playlists del usuario (HDFS Bronze)...")
    ruta_playlists = f"{HDFS_USUARIOS}/*/Playlist1.json"
    print(f"   Ruta: {ruta_playlists}")

    try:
        df_playlists_raw = spark.read.option("multiline", "true").json(ruta_playlists)

        # Explotar playlists -> items -> track
        df_tracks = df_playlists_raw \
            .select(explode(col("playlists")).alias("playlist")) \
            .select(explode(col("playlist.items")).alias("item")) \
            .select(
                col("item.track.trackName").alias("titulo_pl"),
                col("item.track.artistName").alias("artista_pl")
            ) \
            .filter(col("titulo_pl").isNotNull()) \
            .dropDuplicates(["titulo_pl", "artista_pl"]) \
            .withColumn("en_playlist", lit(1))

        n_playlist = df_tracks.count()
        print(f"   Canciones unicas en playlists: {n_playlist}")

        df_cancion = df_cancion.join(
            df_tracks,
            (df_cancion.titulo == df_tracks.titulo_pl) &
            (df_cancion.artista == df_tracks.artista_pl),
            "left"
        ).drop("titulo_pl", "artista_pl")

        # TINYINT: 1 = en playlist, 0 = no en playlist
        df_cancion = df_cancion.withColumn("playlist",
            when(col("en_playlist") == 1, lit(1)).otherwise(lit(0))
        ).drop("en_playlist")

        encontradas = df_cancion.filter(col("playlist") == 1).count()
        print(f"   Canciones del historial en playlist: {encontradas}")

    except Exception as e:
        print(f"   [SKIP] No se pudieron leer playlists de HDFS: {e}")
        df_cancion = df_cancion.withColumn("playlist", lit(0))

    # ==========================================================
    # 5. Generar IDs + fila Desconocido + guardar en Hive
    # ==========================================================
    print("5. Generando IDs...")
    df_cancion = df_cancion.withColumn("idCancion", monotonically_increasing_id())

    print("6. Anadiendo fila 'Desconocido' (ID -1)...")
    fila_desconocido = spark.createDataFrame([{
        "titulo":        "Desconocido",
        "artista":       "Desconocido",
        "album":         "Desconocido",
        "rangoDuracion": "Desconocido",
        "playlist":      0,
        "idCancion":     -1
    }], schema=df_cancion.schema)
    df_cancion = fila_desconocido.unionByName(df_cancion)

    df_cancion.show(5)

    print("7. Guardando en Hive (Parquet)...")
    df_cancion.write.mode("overwrite").format("parquet").saveAsTable("dim_cancion")
    print("Dimension Cancion completada!")


if __name__ == "__main__":
    procesar_dim_cancion()