from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

def procesar_dim_cancion():
    spark = SparkSession.builder \
        .appName("ETL_Dimension_Cancion") \
        .getOrCreate()

    # 1. Leemos el CSV enriquecido que sacamos de Kaggle
    ruta_entrada = "data/raw/temp_api/canciones_features_kaggle.csv"
    print(f"Leyendo dataset de canciones: {ruta_entrada}...")
    
    df_cancion = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(ruta_entrada)

    # 2. Limpieza y renombramiento al estilo de tu canciones.py original
    print("Aplicando transformaciones...")
    df_cancion = df_cancion.select(
        col("cancion").alias("titulo"),
        col("artista"),
        col("album"),
        col("danceability").cast("double"),
        col("energy").cast("double"),
        col("tempo").cast("double"),
        col("valence").cast("double"),
        col("acousticness").cast("double"),
        col("speechiness").cast("double")
    )

    # Eliminamos duplicados por si acaso
    df_cancion = df_cancion.dropDuplicates(["titulo", "artista"])

    # Rellenamos los valores nulos (las canciones que Kaggle no encontró)
    df_cancion = df_cancion.fillna(0.0, subset=["danceability", "energy", "tempo", "valence", "acousticness", "speechiness"])
    df_cancion = df_cancion.fillna("Desconocido", subset=["titulo", "artista", "album"])

    # 3. Añadimos el ID autoincremental de PySpark
    df_cancion = df_cancion.withColumn("idCancion", monotonically_increasing_id())

    # 4. CREACIÓN DE LA FILA "DESCONOCIDO" (ID -1)
    # Tu idea original en Pandas, adaptada a PySpark
    print("Añadiendo fila 'Desconocido' (ID -1) para integridad referencial...")
    
    esquema = df_cancion.schema
    fila_desconocido = spark.createDataFrame([{
        "titulo": "Desconocido",
        "artista": "Desconocido",
        "album": "Desconocido",
        "danceability": 0.0,
        "energy": 0.0,
        "tempo": 0.0,
        "valence": 0.0,
        "acousticness": 0.0,
        "speechiness": 0.0,
        "idCancion": -1
    }], schema=esquema)

    # Unimos nuestro DataFrame con la fila de Desconocido
    df_cancion = fila_desconocido.unionByName(df_cancion)

    # Mostramos una muestra
    df_cancion.show(5)

    # 5. Guardado en formato Parquet (El estándar ultrarrápido de Big Data)
    ruta_salida = "data/processed_data/dim_cancion"
    print(f"Guardando datos en {ruta_salida}...")
    
    df_cancion.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(ruta_salida)
        
    print("¡Dimensión Canción completada con éxito!")
    spark.stop()

if __name__ == "__main__":
    procesar_dim_cancion()