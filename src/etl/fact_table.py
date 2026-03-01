from pyspark.sql.functions import (
    col, lit, when, to_timestamp, date_format,
    hour, input_file_name, regexp_extract, monotonically_increasing_id
)
from src.utils.spark_session import get_spark_session
from src.utils.paths import HDFS_STREAMING, HDFS_FEATURES_CSV

# Columnas de audio features que vienen del CSV de features
FEATURE_COLS = [
    "danceability", "energy", "key", "loudness", "mode",
    "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo"
]


def procesar_tabla_hechos():
    spark = get_spark_session("ETL_Tabla_Hechos")

    # ==========================================================
    # 1. Cargar dimensiones desde Hive (capa Silver)
    # ==========================================================
    print("1. Cargando dimensiones desde Hive...")
    df_dim_cancion  = spark.table("dim_cancion")
    df_dim_artista  = spark.table("dim_artista")
    df_dim_album    = spark.table("dim_album")
    df_dim_hora     = spark.table("dim_hora")
    df_dim_usuario  = spark.table("dim_usuario")

    # ==========================================================
    # 2. Leer historiales de streaming desde HDFS Bronze
    # ==========================================================
    print("2. Leyendo historiales de streaming (HDFS Bronze)...")
    print(f"   Ruta: {HDFS_STREAMING}")
    df_streaming = spark.read.json(HDFS_STREAMING)

    # ==========================================================
    # 3. Limpieza temporal
    # ==========================================================
    print("3. Limpieza temporal...")
    df_streaming = df_streaming \
        .withColumn("endTime", to_timestamp(col("endTime"), "yyyy-MM-dd HH:mm")) \
        .withColumn("idDate",  date_format(col("endTime"), "yyyyMMdd").cast("integer")) \
        .withColumn("hora_reproduccion", hour(col("endTime")))

    # ==========================================================
    # 4. Join con dim_hora (por rango de franja horaria)
    # ==========================================================
    print("4. Join con dim_hora...")
    df_hechos = df_streaming.join(
        df_dim_hora,
        (df_streaming.hora_reproduccion >= df_dim_hora.inicio) &
        (df_streaming.hora_reproduccion <= df_dim_hora.final),
        "left"
    ).drop("franjaHoraria", "inicio", "final", "hora_reproduccion")

    # ==========================================================
    # 5. Extraer usuario desde la ruta del archivo en HDFS
    #
    # En HDFS la ruta es:
    #   hdfs://.../bronze/usuarios/ALEX/StreamingHistory_music_0.json
    # Extraemos "ALEX" con regex
    # ==========================================================
    print("5. Extrayendo usuario desde ruta del archivo HDFS...")
    df_hechos = df_hechos \
        .withColumn("ruta_archivo",   input_file_name()) \
        .withColumn("nombre_carpeta", regexp_extract(col("ruta_archivo"), r"usuarios/([^/]+)/", 1))

    df_hechos = df_hechos.join(
        df_dim_usuario.select(col("idUsuario"), col("nombre").alias("nombre_carpeta")),
        on="nombre_carpeta",
        how="left"
    ).drop("nombre_carpeta", "ruta_archivo")

    df_hechos = df_hechos.fillna(-1, subset=["idUsuario"])

    # ==========================================================
    # 6. Enriquecer con audio features y nombre de album desde HDFS
    # ==========================================================
    print("6. Enriqueciendo con audio features (HDFS Bronze)...")
    print(f"   Ruta: {HDFS_FEATURES_CSV}")
    df_features = spark.read \
        .option("header", "true").option("inferSchema", "true") \
        .csv(HDFS_FEATURES_CSV)

    df_features_select = df_features.select(
        col("cancion").alias("trackName"),
        col("artista").alias("artistName"),
        col("album").alias("album_nombre"),
        col("duration_ms").cast("long").alias("msTotal"),
        *[col(f).cast("double") for f in FEATURE_COLS]
    ).dropDuplicates(["trackName", "artistName"])

    df_hechos = df_hechos.join(
        df_features_select,
        on=["trackName", "artistName"],
        how="left"
    )

    # ==========================================================
    # 7. Join con dim_cancion (por titulo + artista)
    # ==========================================================
    print("7. Join con dim_cancion...")
    df_hechos = df_hechos.join(
        df_dim_cancion.select(
            col("idCancion"),
            col("titulo").alias("trackName"),
            col("artista").alias("artistName")
        ),
        on=["trackName", "artistName"],
        how="left"
    )

    # ==========================================================
    # 8. Join con dim_artista (por nombre artista)
    # ==========================================================
    print("8. Join con dim_artista...")
    df_hechos = df_hechos.join(
        df_dim_artista.select(
            col("idArtista"),
            col("nombre").alias("artistName")
        ),
        on="artistName",
        how="left"
    )

    # ==========================================================
    # 9. Join con dim_album (por album + artista)
    #
    # FIX: El codigo original unia solo por artistName, lo que
    # generaba un producto cartesiano: un artista tiene MUCHOS
    # albumes. Ahora cruzamos por (album_nombre, artistName).
    # ==========================================================
    print("9. Join con dim_album (por album + artista)...")
    df_hechos = df_hechos.join(
        df_dim_album.select(
            col("idAlbum"),
            col("nombre").alias("album_nombre"),
            col("artista").alias("artistName")
        ),
        on=["album_nombre", "artistName"],
        how="left"
    )

    # ==========================================================
    # 10. Rellenar FKs nulas con -1 (registro "Desconocido")
    # ==========================================================
    print("10. Rellenando FKs nulas...")
    df_hechos = df_hechos.fillna(-1, subset=["idCancion", "idArtista", "idAlbum"])
    df_hechos = df_hechos.fillna(0,  subset=["msTotal"] + FEATURE_COLS)

    # ==========================================================
    # 11. Calcular metricas de escucha
    # ==========================================================
    print("11. Calculando metricas...")
    df_hechos = df_hechos \
        .withColumnRenamed("msPlayed", "msEscuchados") \
        .withColumn("msNoEscuchados",      col("msTotal") - col("msEscuchados")) \
        .withColumn("cancionesEscuchadas", lit(1))

    df_hechos = df_hechos.withColumn("msNoEscuchados",
        when(col("msNoEscuchados") < 0, lit(0)).otherwise(col("msNoEscuchados"))
    )

    # ==========================================================
    # 12. Seleccionar columnas finales (matching Hive DDL)
    # ==========================================================
    print("12. Seleccionando columnas finales...")
    df_final = df_hechos.withColumn("idhechos_historial", monotonically_increasing_id())

    df_final = df_final.select(
        col("idhechos_historial"),
        col("msEscuchados"),
        col("msNoEscuchados"),
        col("cancionesEscuchadas"),
        col("msTotal"),
        *[col(f) for f in FEATURE_COLS],
        col("idCancion").alias("IDCancion"),
        col("idArtista").alias("IDArtista"),
        col("idAlbum").alias("IDAlbum"),
        col("idHora").alias("IDHora"),
        col("idDate").alias("IDDate"),
        col("idUsuario").alias("IDUsuario")
    )

    print(f"   Filas en tabla de hechos: {df_final.count()}")
    df_final.show(10)

    # ==========================================================
    # 13. Guardar en Hive (Parquet)
    # ==========================================================
    print("13. Guardando en Hive (Parquet)...")
    df_final.write.mode("overwrite").format("parquet").saveAsTable("fact_historial")
    print("Tabla de Hechos completada!")


if __name__ == "__main__":
    procesar_tabla_hechos()