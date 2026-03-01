from pyspark.sql.functions import col, monotonically_increasing_id, lit
from src.utils.spark_session import get_spark_session


def procesar_dim_artista():
    spark = get_spark_session("ETL_Dimension_Artista")

    print("1. Leyendo artistas únicos del dataset...")
    df_base = spark.read.option("header", "true").csv("data/raw/temp_api/canciones_features_kaggle.csv")

    df_artista = df_base.select(col("artista").alias("nombre")).dropDuplicates(["nombre"])
    df_artista = df_artista.fillna("Desconocido", subset=["nombre"])

    print("2. Añadiendo columnas de enriquecimiento...")
    df_artista = df_artista.withColumn("tipo",   lit("Desconocido")) \
                           .withColumn("pais",   lit("Desconocido")) \
                           .withColumn("genero", lit("Desconocido"))

    print("3. Generando IDs...")
    df_artista = df_artista.withColumn("idArtista", monotonically_increasing_id())

    print("4. Añadiendo fila 'Desconocido' (ID -1)...")
    fila_desconocido = spark.createDataFrame([{
        "nombre":    "Desconocido",
        "tipo":      "Desconocido",
        "pais":      "Desconocido",
        "genero":    "Desconocido",
        "idArtista": -1
    }], schema=df_artista.schema)
    df_artista = fila_desconocido.unionByName(df_artista)

    df_artista.show(5)

    print("5. Guardando en Hive (Parquet)...")
    df_artista.write.mode("overwrite").format("parquet").saveAsTable("dim_artista")
    print("¡Dimensión Artista completada!")


if __name__ == "__main__":
    procesar_dim_artista()