"""
dim_album.py

ETL de la dimensión Álbum. Fuentes:
  - canciones_features_kaggle.csv (HDFS Bronze): nombre del álbum y artista.
  - albums_info.csv (HDFS Bronze): productora/sello discográfico y año de
    publicación, obtenidos de la API REST de MusicBrainz.

Pasos principales:
  1. Leer álbumes únicos (nombre + artista) del CSV de features.
  2. Limpiar caracteres no ASCII del nombre del álbum (emojis, etc.).
  3. Cruzar con albums_info.csv para enriquecer con productora y anyo.
  4. Generar ID autoincremental + fila centinela Desconocido (ID=-1).
  5. Persistir en Hive (formato Parquet).

El campo `anyo` (INT) permite agrupar álbumes por décadas en el cubo OLAP
usando la expresión FLOOR(anyo / 10) * 10.
"""
from pyspark.sql.functions import col, lit, regexp_replace, row_number
from pyspark.sql.window import Window
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
    # 3. Cruzar con productoras y año de MusicBrainz desde HDFS
    #
    # albums_info.csv tiene columnas: Album, Artista, Productora, Anyo
    # La columna Anyo puede estar ausente en versiones antiguas del CSV;
    # en ese caso se imputa -1 para indicar año desconocido.
    # ==========================================================
    print("3. Cruzando con productoras y año de MusicBrainz (HDFS Bronze)...")
    print(f"   Ruta: {HDFS_ALBUMS_INFO}")
    try:
        df_prod = spark.read.option("header", "true").csv(HDFS_ALBUMS_INFO)
        cols_prod = [c.lower() for c in df_prod.columns]

        select_exprs = [
            col("Album").alias("nombre"),
            col("Artista").alias("artista"),
            col("Productora").alias("productora")
        ]
        # Año de publicación: permite agrupar por décadas en el cubo OLAP
        if "anyo" in cols_prod:
            select_exprs.append(col("Anyo").cast("int").alias("anyo"))
        else:
            select_exprs.append(lit(-1).alias("anyo"))

        df_prod = df_prod.select(*select_exprs)
        df_album = df_album.join(df_prod, on=["nombre", "artista"], how="left")
        df_album = df_album.fillna("Desconocido", subset=["productora"])
        df_album = df_album.fillna(-1, subset=["anyo"])
        print("   Productoras y años cargados correctamente desde HDFS.")
    except Exception as e:
        print(f"   [SKIP] No se pudo leer albums_info.csv de HDFS: {e}")
        df_album = df_album.withColumn("productora", lit("Desconocido")) \
                           .withColumn("anyo", lit(-1))

    # ==========================================================
    # 4. Generar IDs + fila Desconocido + guardar en Hive
    # ==========================================================
    print("4. Generando IDs secuenciales (row_number, empieza en 1)...")
    w = Window.orderBy("nombre", "artista")
    df_album = df_album.withColumn("idAlbum", row_number().over(w))

    print("5. Añadiendo la fila 'Desconocido' (ID -1)...")
    fila_desconocido = spark.createDataFrame([{
        "nombre":     "Desconocido",
        "artista":    "Desconocido",
        "productora": "Desconocido",
        "anyo":       -1,
        "idAlbum":    -1
    }], schema=df_album.schema)
    df_album = fila_desconocido.unionByName(df_album)

    df_album.show(5)

    print("6. Guardando en Hive (formato Parquet)...")
    df_album.write.mode("overwrite").format("parquet").saveAsTable("dim_album")
    print("¡Dimensión Álbum completada!")


if __name__ == "__main__":
    procesar_dim_album()