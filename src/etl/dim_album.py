from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, lit, regexp_replace

def procesar_dim_album():
    spark = SparkSession.builder \
        .appName("ETL_Dimension_Album") \
        .getOrCreate()

    print("1. Leyendo álbumes de nuestro dataset...")
    df_base = spark.read.option("header", "true").csv("data/raw/temp_api/canciones_features_kaggle.csv")

    # Extraemos combinaciones únicas de álbum y artista (porque dos artistas pueden tener un álbum que se llame igual)
    df_album = df_base.select(
        col("album").alias("nombre"),
        col("artista")
    ).dropDuplicates(["nombre", "artista"])

    df_album = df_album.fillna("Desconocido", subset=["nombre", "artista"])

    print("2. Limpiando Emojis como en tu script original...")
    # Usamos expresiones regulares en PySpark para quitar caracteres raros/emojis
    regex_emojis = r"[^\x00-\x7F]+"
    df_album = df_album.withColumn("nombre", regexp_replace(col("nombre"), regex_emojis, ""))

    print("3. Añadiendo la columna Productora...")
    df_album = df_album.withColumn("productora", lit("Desconocido"))

    print("4. Generando IDs autoincrementales...")
    df_album = df_album.withColumn("idAlbum", monotonically_increasing_id())

    print("5. Añadiendo la fila 'Desconocido' (ID -1)...")
    esquema = df_album.schema
    fila_desconocido = spark.createDataFrame([{
        "nombre": "Desconocido",
        "artista": "Desconocido",
        "productora": "Desconocido",
        "idAlbum": -1
    }], schema=esquema)

    df_album = fila_desconocido.unionByName(df_album)

    df_album.show(5)

    ruta_salida = "data/processed_data/dim_album"
    print(f"Guardando datos en {ruta_salida}...")
    df_album.write.mode("overwrite").option("header", "true").csv(ruta_salida)
        
    print("¡Dimensión Álbum completada!")
    spark.stop()

if __name__ == "__main__":
    procesar_dim_album()