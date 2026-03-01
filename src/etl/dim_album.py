from pyspark.sql.functions import col, monotonically_increasing_id, lit, regexp_replace
from src.utils.spark_session import get_spark_session


def procesar_dim_album():
    spark = get_spark_session("ETL_Dimension_Album")

    print("1. Leyendo álbumes de nuestro dataset...")
    df_base = spark.read.option("header", "true").csv("data/raw/temp_api/canciones_features_kaggle.csv")

    df_album = df_base.select(
        col("album").alias("nombre"),
        col("artista")
    ).dropDuplicates(["nombre", "artista"])

    df_album = df_album.fillna("Desconocido", subset=["nombre", "artista"])

    print("2. Limpiando emojis y caracteres no ASCII...")
    regex_emojis = r"[^\x00-\x7F]+"
    df_album = df_album.withColumn("nombre", regexp_replace(col("nombre"), regex_emojis, ""))

    print("3. Añadiendo la columna Productora...")
    df_album = df_album.withColumn("productora", lit("Desconocido"))

    print("4. Generando IDs autoincrementales...")
    df_album = df_album.withColumn("idAlbum", monotonically_increasing_id())

    print("5. Añadiendo la fila 'Desconocido' (ID -1) para integridad referencial...")
    esquema = df_album.schema
    fila_desconocido = spark.createDataFrame([{
        "nombre": "Desconocido",
        "artista": "Desconocido",
        "productora": "Desconocido",
        "idAlbum": -1
    }], schema=esquema)

    df_album = fila_desconocido.unionByName(df_album)

    df_album.show(5)

    print("6. Guardando en Hive (formato Parquet)...")
    df_album.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable("dim_album")

    print("¡Dimensión Álbum completada!")


if __name__ == "__main__":
    procesar_dim_album()