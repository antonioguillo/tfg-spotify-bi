from pyspark.sql.functions import col, monotonically_increasing_id, lit, when
from src.utils.spark_session import get_spark_session


def procesar_dim_cancion():
    spark = get_spark_session("ETL_Dimension_Cancion")

    print("1. Leyendo dataset de canciones...")
    df = spark.read.option("header", "true").option("inferSchema", "true") \
              .csv("data/raw/temp_api/canciones_features_kaggle.csv")

    print("2. Seleccionando y limpiando columnas...")
    df_cancion = df.select(
        col("cancion").alias("titulo"),
        col("artista"),
        col("album"),
        col("duration_ms").cast("double")
    ).dropDuplicates(["titulo", "artista"])

    df_cancion = df_cancion.fillna("Desconocido", subset=["titulo", "artista", "album"])

    print("3. Calculando rangoDuracion...")
    df_cancion = df_cancion.withColumn("duration_min", col("duration_ms") / 60000)
    df_cancion = df_cancion.withColumn("rangoDuracion",
        when(col("duration_min") <= 2,  "0-2 min")
        .when(col("duration_min") <= 4,  "2-4 min")
        .when(col("duration_min") <= 7,  "4-7 min")
        .when(col("duration_min") <= 11, "7-11 min")
        .otherwise("11+ min")
    ).drop("duration_min", "duration_ms")

    print("4. Añadiendo columna playlist (False por defecto)...")
    # En una mejora futura se cruzaría con los JSONs de playlist del usuario
    df_cancion = df_cancion.withColumn("playlist", lit(False))

    print("5. Generando IDs...")
    df_cancion = df_cancion.withColumn("idCancion", monotonically_increasing_id())

    print("6. Añadiendo fila 'Desconocido' (ID -1)...")
    fila_desconocido = spark.createDataFrame([{
        "titulo":        "Desconocido",
        "artista":       "Desconocido",
        "album":         "Desconocido",
        "rangoDuracion": "Desconocido",
        "playlist":      False,
        "idCancion":     -1
    }], schema=df_cancion.schema)
    df_cancion = fila_desconocido.unionByName(df_cancion)

    df_cancion.show(5)

    print("7. Guardando en Hive (Parquet)...")
    df_cancion.write.mode("overwrite").format("parquet").saveAsTable("dim_cancion")
    print("¡Dimensión Canción completada!")


if __name__ == "__main__":
    procesar_dim_cancion()