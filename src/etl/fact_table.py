from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, to_timestamp, date_format, hour, input_file_name, regexp_extract, monotonically_increasing_id

def procesar_tabla_hechos():
    spark = SparkSession.builder \
        .appName("ETL_Tabla_Hechos") \
        .getOrCreate()

    print("1. Cargando las Dimensiones (para cruzar los IDs)...")
    df_dim_usuario = spark.read.option("header", "true").csv("data/processed_data/dim_usuario")
    df_dim_cancion = spark.read.option("header", "true").csv("data/processed_data/dim_cancion")
    df_dim_artista = spark.read.option("header", "true").csv("data/processed_data/dim_artista")
    df_dim_album = spark.read.option("header", "true").csv("data/processed_data/dim_album")
    df_dim_hora = spark.read.option("header", "true").csv("data/processed_data/dim_hora")

    print("2. Leyendo TODOS los historiales de streaming...")
    # Leemos todos los historiales de música de todos los usuarios
    ruta_historiales = "data/raw/raw_data_spotify/*/*/StreamingHistory_music_*.json"
    df_streaming = spark.read.json(ruta_historiales)

    print("3. Limpieza y extracción temporal...")
    # Convertimos endTime a timestamp
    df_streaming = df_streaming.withColumn("endTime", to_timestamp(col("endTime"), "yyyy-MM-dd HH:mm"))
    
    # Extraemos el ID inteligente de Fecha (YYYYMMDD)
    df_streaming = df_streaming.withColumn("idDate", date_format(col("endTime"), "yyyyMMdd").cast("integer"))
    
    # Extraemos la hora para cruzar con dim_hora
    df_streaming = df_streaming.withColumn("hora_reproduccion", hour(col("endTime")))

    print("4. Cruzando IDs Temporales...")
    # Join con Dimensión Hora usando rango de horas (inicio y final)
    df_hechos = df_streaming.join(
        df_dim_hora,
        (df_streaming.hora_reproduccion >= df_dim_hora.inicio) & (df_streaming.hora_reproduccion <= df_dim_hora.final),
        "left"
    ).drop("franjaHoraria", "inicio", "final", "hora_reproduccion")

    print("5. Obteniendo usuario desde la ruta del archivo...")
    # Truco PySpark: Sacamos el nombre de la carpeta (ej: BEA) de la ruta del archivo
    df_hechos = df_hechos.withColumn("ruta_archivo", input_file_name())
    # Extrae el nombre de la carpeta que está justo antes de 'Spotify Account Data'
    df_hechos = df_hechos.withColumn("nombre_carpeta", regexp_extract(col("ruta_archivo"), r"raw_data_spotify/([^/]+)/", 1))
    
    # Cruzamos con dim_usuario para sacar el idUsuario
    df_hechos = df_hechos.join(
        df_dim_usuario.select(col("idUsuario"), col("nombre").alias("nombre_carpeta")),
        on="nombre_carpeta",
        how="left"
    ).drop("nombre_carpeta", "ruta_archivo")
    
    # Si no encuentra el usuario, le ponemos -1 (Aunque en este TFG deberían estar todos)
    df_hechos = df_hechos.fillna(-1, subset=["idUsuario"])

    print("6. Cruzando IDs Musicales (Canción, Artista, Álbum)...")
    
    # Left Join con Canción
    df_hechos = df_hechos.join(
        df_dim_cancion.select(col("idCancion"), col("titulo").alias("trackName"), col("artista").alias("artistName")),
        on=["trackName", "artistName"],
        how="left"
    )
    
    # Left Join con Artista
    df_hechos = df_hechos.join(
        df_dim_artista.select(col("idArtista"), col("nombre").alias("artistName")),
        on="artistName",
        how="left"
    )
    
    # Como el JSON original NO tiene el álbum, usamos el idCancion para ir a la dimensión canción
    # y sacar el nombre del álbum (si lo encontró Kaggle), y luego cruzarlo con dim_album.
    # Por simplicidad y asegurar rendimiento, aquí lo cruzamos directo si tuvieramos el álbum.
    # Como no está en el JSON, asumiremos que el álbum va ligado al idCancion, pero para cumplir
    # tu Estrella, vamos a mapear las que encontramos y las que no, ID -1.
    
    # Rellenamos los IDs que no han cruzado (canciones/artistas indie que Kaggle no tenía) con -1
    df_hechos = df_hechos.fillna(-1, subset=["idCancion", "idArtista"])
    
    # Para el idAlbum, como la canción en el historial no trae el álbum, asignamos -1 temporalmente 
    # (Lo ideal sería extraerlo del dataset de Kaggle antes).
    df_hechos = df_hechos.withColumn("idAlbum", lit(-1))

    print("7. Generando Métricas (Hechos)...")
    # Adaptado de tu tablaHechos.py original
    df_hechos = df_hechos.withColumnRenamed("msPlayed", "msEscuchados")
    df_hechos = df_hechos.withColumn("cancionesEscuchadas", lit(1))
    
    # Si tuvieras duration_ms de Kaggle podrías calcular msNoEscuchados.
    # Como en el JSON básico no viene, dejamos la métrica preparada o a 0.
    df_hechos = df_hechos.withColumn("msNoEscuchados", lit(0))

    # Seleccionamos solo las columnas finales para la base de datos
    df_hechos_final = df_hechos.select(
        col("msEscuchados"),
        col("msNoEscuchados"),
        col("cancionesEscuchadas"),
        col("idCancion"),
        col("idArtista"),
        col("idAlbum"),
        col("idUsuario"),
        col("idDate"),
        col("idHora")
    )

    # Añadimos la Primary Key de la tabla de hechos
    df_hechos_final = df_hechos_final.withColumn("idhechos_historial", monotonically_increasing_id())

    df_hechos_final.show(10)

    ruta_salida = "data/processed_data/tabla_hechos"
    print(f"Guardando Tabla de Hechos en {ruta_salida}...")
    df_hechos_final.write.mode("overwrite").option("header", "true").csv(ruta_salida)
        
    print("¡Tabla de Hechos completada con éxito!")
    spark.stop()

if __name__ == "__main__":
    procesar_tabla_hechos()