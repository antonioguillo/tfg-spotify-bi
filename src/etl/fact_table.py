from pyspark.sql.functions import (
    col, lit, when, to_timestamp, date_format,
    hour, input_file_name, regexp_extract, monotonically_increasing_id
)
from src.utils.spark_session import get_spark_session

# Columnas de audio features que vienen del CSV de features
FEATURE_COLS = [
    "danceability", "energy", "key", "loudness", "mode",
    "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo"
]


def procesar_tabla_hechos():
    spark = get_spark_session("ETL_Tabla_Hechos")

    print("1. Cargando dimensiones desde Hive...")
    df_dim_cancion  = spark.table("dim_cancion")
    df_dim_artista  = spark.table("dim_artista")
    df_dim_album    = spark.table("dim_album")
    df_dim_hora     = spark.table("dim_hora")
    df_dim_usuario  = spark.table("dim_usuario")

    print("2. Leyendo historiales de streaming...")
    ruta = "data/raw/raw_data_spotify/*/*/StreamingHistory_music_*.json"
    df_streaming = spark.read.json(ruta)

    print("3. Limpieza temporal...")
    df_streaming = df_streaming \
        .withColumn("endTime", to_timestamp(col("endTime"), "yyyy-MM-dd HH:mm")) \
        .withColumn("idDate",  date_format(col("endTime"), "yyyyMMdd").cast("integer")) \
        .withColumn("hora_reproduccion", hour(col("endTime")))

    print("4. Join con dim_hora...")
    df_hechos = df_streaming.join(
        df_dim_hora,
        (df_streaming.hora_reproduccion >= df_dim_hora.inicio) &
        (df_streaming.hora_reproduccion <= df_dim_hora.final),
        "left"
    ).drop("franjaHoraria", "inicio", "final", "hora_reproduccion")

    print("5. Extrayendo usuario desde ruta del archivo...")
    df_hechos = df_hechos \
        .withColumn("ruta_archivo",   input_file_name()) \
        .withColumn("nombre_carpeta", regexp_extract(col("ruta_archivo"), r"raw_data_spotify/([^/]+)/", 1))

    df_hechos = df_hechos.join(
        df_dim_usuario.select(col("idUsuario"), col("nombre").alias("nombre_carpeta")),
        on="nombre_carpeta",
        how="left"
    ).drop("nombre_carpeta", "ruta_archivo")

    df_hechos = df_hechos.fillna(-1, subset=["idUsuario"])

    print("6. Join con dimensiones musicales...")

    # Join cancion → obtenemos idCancion y audio features
    df_features = spark.read \
        .option("header", "true").option("inferSchema", "true") \
        .csv("data/raw/temp_api/canciones_features_kaggle.csv")

    # Añadimos features al streaming cruzando por cancion + artista
    df_hechos = df_hechos.join(
        df_features.select(
            col("cancion").alias("trackName"),
            col("artista").alias("artistName"),
            col("duration_ms").cast("long").alias("msTotal"),
            *[col(f).cast("double") for f in FEATURE_COLS]
        ).dropDuplicates(["trackName", "artistName"]),
        on=["trackName", "artistName"],
        how="left"
    )

    # Join cancion para idCancion
    df_hechos = df_hechos.join(
        df_dim_cancion.select(col("idCancion"), col("titulo").alias("trackName"), col("artista").alias("artistName")),
        on=["trackName", "artistName"],
        how="left"
    )

    # Join artista para idArtista
    df_hechos = df_hechos.join(
        df_dim_artista.select(col("idArtista"), col("nombre").alias("artistName")),
        on="artistName",
        how="left"
    )

    # Join album para idAlbum
    df_hechos = df_hechos.join(
        df_dim_album.select(col("idAlbum"), col("nombre").alias("album_nombre"), col("artista").alias("artistName")),
        on="artistName",
        how="left"
    )

    df_hechos = df_hechos.fillna(-1, subset=["idCancion", "idArtista", "idAlbum"])
    df_hechos = df_hechos.fillna(0,  subset=["msTotal"] + FEATURE_COLS)

    print("7. Calculando métricas...")
    df_hechos = df_hechos \
        .withColumnRenamed("msPlayed", "msEscuchados") \
        .withColumn("msNoEscuchados",      col("msTotal") - col("msEscuchados")) \
        .withColumn("cancionesEscuchadas", lit(1))

    # msNoEscuchados no puede ser negativo (canción skipeada después del total)
    df_hechos = df_hechos.withColumn("msNoEscuchados",
        when(col("msNoEscuchados") < 0, lit(0)).otherwise(col("msNoEscuchados"))
    )

    print("8. Seleccionando columnas finales...")
    df_final = df_hechos.select(
        col("msEscuchados"),
        col("msNoEscuchados"),
        col("cancionesEscuchadas"),
        col("msTotal"),
        *[col(f) for f in FEATURE_COLS],
        col("idCancion"),
        col("idArtista"),
        col("idAlbum").alias("IDAlbum"),
        col("idHora").alias("IDHora"),
        col("idDate").alias("IDDate"),
        col("idUsuario").alias("IDUsuario")
    ).withColumn("idhechos_historial", monotonically_increasing_id())

    df_final.show(10)

    print("9. Guardando en Hive (Parquet)...")
    df_final.write.mode("overwrite").format("parquet").saveAsTable("fact_historial")
    print("¡Tabla de Hechos completada!")


if __name__ == "__main__":
    procesar_tabla_hechos()