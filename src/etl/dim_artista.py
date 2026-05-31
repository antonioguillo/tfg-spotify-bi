"""
dim_artista.py

ETL de la dimensión Artista. Fuentes:
  - canciones_features_kaggle.csv (HDFS Bronze): lista de artistas únicos.
  - artistas_info.csv (HDFS Bronze): tipo (Solista/Grupo) y país, obtenidos
    de la API REST de MusicBrainz.
  - artistas_generos.csv (HDFS Bronze): género musical principal, obtenido
    de Every Noise at Once.

Pasos principales:
  1. Extraer artistas únicos del CSV de features.
  2. Cruzar con MusicBrainz (LEFT JOIN por nombre) para tipo y país.
  3. Cruzar con Every Noise at Once (LEFT JOIN por nombre) para género.
  4. Generar ID autoincremental + fila centinela Desconocido (ID=-1).
  5. Persistir en Hive (formato Parquet).
"""
import os
from pyspark.sql.functions import col, monotonically_increasing_id, lit
from src.utils.spark_session import get_spark_session
from src.utils.paths import HDFS_FEATURES_CSV, HDFS_ARTISTAS_INFO, HDFS_ARTISTAS_GEN


def procesar_dim_artista():
    spark = get_spark_session("ETL_Dimension_Artista")

    # ==========================================================
    # 1. Leer artistas únicos desde Bronze en HDFS
    # ==========================================================
    print("1. Leyendo artistas únicos del dataset de features (HDFS Bronze)...")
    print(f"   Ruta: {HDFS_FEATURES_CSV}")
    df_base = spark.read.option("header", "true").csv(HDFS_FEATURES_CSV)
    df_artista = df_base.select(col("artista").alias("nombre")).dropDuplicates(["nombre"])
    df_artista = df_artista.fillna("Desconocido", subset=["nombre"])

    # ==========================================================
    # 2. Cruzar con MusicBrainz (tipo y país) desde HDFS
    # ==========================================================
    print("2. Cruzando con datos de MusicBrainz (tipo y país)...")
    print(f"   Ruta: {HDFS_ARTISTAS_INFO}")
    try:
        df_mb = spark.read.option("header", "true").csv(HDFS_ARTISTAS_INFO)
        df_mb = df_mb.select(
            col("Artista").alias("nombre"),
            col("Tipo").alias("tipo"),
            col("País").alias("pais")
        )
        df_artista = df_artista.join(df_mb, on="nombre", how="left")
        df_artista = df_artista.fillna("Desconocido", subset=["tipo", "pais"])
        print("   MusicBrainz cargado correctamente desde HDFS.")
    except Exception as e:
        print(f"   [SKIP] No se pudo leer artistas_info.csv de HDFS: {e}")
        df_artista = df_artista.withColumn("tipo", lit("Desconocido")) \
                               .withColumn("pais", lit("Desconocido"))

    # ==========================================================
    # 3. Cruzar con géneros de Every Noise desde HDFS
    # ==========================================================
    print("3. Cruzando con géneros de Every Noise at Once...")
    print(f"   Ruta: {HDFS_ARTISTAS_GEN}")
    try:
        df_generos = spark.read.option("header", "true").csv(HDFS_ARTISTAS_GEN)
        df_generos = df_generos.select(
            col("Artista").alias("nombre"),
            col("Genero").alias("genero")
        )
        df_artista = df_artista.join(df_generos, on="nombre", how="left")
        df_artista = df_artista.fillna("Desconocido", subset=["genero"])
        print("   Every Noise cargado correctamente desde HDFS.")
    except Exception as e:
        print(f"   [SKIP] No se pudo leer artistas_generos.csv de HDFS: {e}")
        df_artista = df_artista.withColumn("genero", lit("Desconocido"))

    # ==========================================================
    # 4. Generar IDs + fila Desconocido + guardar en Hive
    # ==========================================================
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