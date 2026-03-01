import os
from pyspark.sql.functions import col, monotonically_increasing_id, lit
from src.utils.spark_session import get_spark_session


def procesar_dim_artista():
    spark = get_spark_session("ETL_Dimension_Artista")

    print("1. Leyendo artistas únicos del dataset de features...")
    df_base = spark.read.option("header", "true").csv("data/raw/temp_api/canciones_features_kaggle.csv")
    df_artista = df_base.select(col("artista").alias("nombre")).dropDuplicates(["nombre"])
    df_artista = df_artista.fillna("Desconocido", subset=["nombre"])

    print("2. Cruzando con datos de MusicBrainz (tipo y país)...")
    ruta_mb = "data/raw/temp_api/artistas_info.csv"
    if os.path.exists(ruta_mb):
        df_mb = spark.read.option("header", "true").csv(ruta_mb)
        df_mb = df_mb.select(
            col("Artista").alias("nombre"),
            col("Tipo").alias("tipo"),
            col("País").alias("pais")
        )
        df_artista = df_artista.join(df_mb, on="nombre", how="left")
        df_artista = df_artista.fillna("Desconocido", subset=["tipo", "pais"])
        print("   MusicBrainz cargado correctamente.")
    else:
        print("   [SKIP] artistas_info.csv no encontrado — ejecuta get_info_artistas.py")
        df_artista = df_artista.withColumn("tipo", lit("Desconocido")) \
                               .withColumn("pais", lit("Desconocido"))

    print("3. Cruzando con géneros de Every Noise at Once...")
    ruta_generos = "data/raw/temp_api/artistas_generos.csv"
    if os.path.exists(ruta_generos):
        df_generos = spark.read.option("header", "true").csv(ruta_generos)
        df_generos = df_generos.select(
            col("Artista").alias("nombre"),
            col("Genero").alias("genero")
        )
        df_artista = df_artista.join(df_generos, on="nombre", how="left")
        df_artista = df_artista.fillna("Desconocido", subset=["genero"])
        print("   Every Noise cargado correctamente.")
    else:
        print("   [SKIP] artistas_generos.csv no encontrado — ejecuta get_generos_artistas.py")
        df_artista = df_artista.withColumn("genero", lit("Desconocido"))

    print("4. Generando IDs...")
    df_artista = df_artista.withColumn("idArtista", monotonically_increasing_id())

    print("5. Añadiendo fila 'Desconocido' (ID -1)...")
    fila_desconocido = spark.createDataFrame([{
        "nombre":    "Desconocido",
        "tipo":      "Desconocido",
        "pais":      "Desconocido",
        "genero":    "Desconocido",
        "idArtista": -1
    }], schema=df_artista.schema)
    df_artista = fila_desconocido.unionByName(df_artista)

    df_artista.show(5)

    print("6. Guardando en Hive (Parquet)...")
    df_artista.write.mode("overwrite").format("parquet").saveAsTable("dim_artista")
    print("¡Dimensión Artista completada!")


if __name__ == "__main__":
    procesar_dim_artista()
