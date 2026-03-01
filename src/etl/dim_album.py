from pyspark.sql.functions import col, monotonically_increasing_id, lit, regexp_replace
from src.utils.spark_session import get_spark_session
from src.utils.paths import HDFS_FEATURES_CSV, HDFS_ALBUMS_INFO


def procesar_dim_album():
    spark = get_spark_session("ETL_Dimension_Album")

    # ==========================================================
    # 1. Leer álbumes desde Bronze en HDFS
    # ==========================================================
    print("1. Leyendo álbumes de nuestro dataset (HDFS Bronze)...")
    print(f"   Ruta: {HDFS_FEATURES_CSV}")
    df_base = spark.read.option("header", "true").csv(HDFS_FEATURES_CSV)

    df_album = df_base.select(
        col("album").alias("nombre"),
        col("artista")
    ).dropDuplicates(["nombre", "artista"])

    df_album = df_album.fillna("Desconocido", subset=["nombre", "artista"])

    print("2. Limpiando emojis y caracteres no ASCII...")
    df_album = df_album.withColumn("nombre", regexp_replace(col("nombre"), r"[^\x00-\x7F]+", ""))

    # ==========================================================
    # 3. Cruzar con productoras de MusicBrainz desde HDFS
    # ==========================================================
    print("3. Cruzando con productoras de MusicBrainz (HDFS Bronze)...")
    print(f"   Ruta: {HDFS_ALBUMS_INFO}")
    try:
        df_prod = spark.read.option("header", "true").csv(HDFS_ALBUMS_INFO)
        df_prod = df_prod.select(
            col("Album").alias("nombre"),
            col("Artista").alias("artista"),
            col("Productora").alias("productora")
        )
        df_album = df_album.join(df_prod, on=["nombre", "artista"], how="left")
        df_album = df_album.fillna("Desconocido", subset=["productora"])
        print("   Productoras cargadas correctamente desde HDFS.")
    except Exception as e:
        print(f"   [SKIP] No se pudo leer albums_info.csv de HDFS: {e}")
        df_album = df_album.withColumn("productora", lit("Desconocido"))

    # ==========================================================
    # 4. Generar IDs + fila Desconocido + guardar en Hive
    # ==========================================================
    print("4. Generando IDs autoincrementales...")
    df_album = df_album.withColumn("idAlbum", monotonically_increasing_id())

    print("5. Añadiendo la fila 'Desconocido' (ID -1)...")
    fila_desconocido = spark.createDataFrame([{
        "nombre":     "Desconocido",
        "artista":    "Desconocido",
        "productora": "Desconocido",
        "idAlbum":    -1
    }], schema=df_album.schema)
    df_album = fila_desconocido.unionByName(df_album)

    df_album.show(5)

    print("6. Guardando en Hive (formato Parquet)...")
    df_album.write.mode("overwrite").format("parquet").saveAsTable("dim_album")
    print("¡Dimensión Álbum completada!")


if __name__ == "__main__":
    procesar_dim_album()