from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, lit

def procesar_dim_artista():
    spark = SparkSession.builder \
        .appName("ETL_Dimension_Artista") \
        .getOrCreate()

    print("1. Leyendo artistas únicos de nuestro dataset...")
    # Leemos el dataset que cruzamos en pasos anteriores
    df_base = spark.read.option("header", "true").csv("data/raw/temp_api/canciones_features_kaggle.csv")

    # Extraemos solo los artistas y quitamos duplicados
    df_artista = df_base.select(col("artista").alias("nombre")).dropDuplicates(["nombre"])
    
    # Rellenamos nulos por si acaso
    df_artista = df_artista.fillna("Desconocido", subset=["nombre"])

    print("2. Añadiendo columnas de país, tipo y género (Placeholder)...")
    # Como en tu script original, preparamos las columnas. 
    # (Si luego cruzas con MusicBrainz, actualizarías estas columnas)
    df_artista = df_artista.withColumn("tipo", lit("Desconocido")) \
                           .withColumn("pais", lit("Desconocido")) \
                           .withColumn("genero", lit("Desconocido"))

    print("3. Generando IDs autoincrementales...")
    df_artista = df_artista.withColumn("idArtista", monotonically_increasing_id())

    print("4. Añadiendo la fila 'Desconocido' (ID -1)...")
    esquema = df_artista.schema
    fila_desconocido = spark.createDataFrame([{
        "nombre": "Desconocido",
        "tipo": "Desconocido",
        "pais": "Desconocido",
        "genero": "Desconocido",
        "idArtista": -1
    }], schema=esquema)

    df_artista = fila_desconocido.unionByName(df_artista)

    df_artista.show(5)

    ruta_salida = "data/processed_data/dim_artista"
    print(f"Guardando datos en {ruta_salida}...")
    df_artista.write.mode("overwrite").option("header", "true").csv(ruta_salida)
        
    print("¡Dimensión Artista completada!")
    spark.stop()

if __name__ == "__main__":
    procesar_dim_artista()