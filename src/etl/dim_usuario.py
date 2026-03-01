from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, when, monotonically_increasing_id

def procesar_dim_usuario():
    # 1. Inicializamos la sesión de Spark (el motor)
    spark = SparkSession.builder \
        .appName("ETL_Dimension_Usuario") \
        .getOrCreate()

    # 2. Leer TODOS los archivos Userdata.json de todos los usuarios a la vez
    # Usamos asteriscos (*) como comodines para entrar en todas las carpetas automáticamente
    ruta_entrada = "data/raw/raw_data_spotify/*/*/Userdata.json"
    
    print("Leyendo archivos de usuarios...")
    # Leemos el JSON. Dependiendo del formato exacto de Spotify, a veces requiere multiline
    df_usuario = spark.read.option("multiline", "true").json(ruta_entrada)

    # 3. Transformaciones (La magia de PySpark)
    print("Aplicando transformaciones...")
    
    # Extraemos el año de nacimiento
    df_usuario = df_usuario.withColumn("birth_year", year(to_date(col("birthdate"), "yyyy-MM-dd")))

    # Asignamos la generación usando "when" (mucho más eficiente que apply de Pandas)
    df_usuario = df_usuario.withColumn("generacion",
        when(col("birth_year") < 1946, "Generación Silenciosa")
        .when(col("birth_year") < 1965, "Baby Boomers")
        .when(col("birth_year") < 1981, "Generación X")
        .when(col("birth_year") < 1997, "Millennials")
        .otherwise("Generación Z")
    )

    # 4. Seleccionamos y renombramos columnas para dejarlas limpias
    df_usuario = df_usuario.select(
        col("username").alias("nombre"),
        col("email").alias("email"),
        col("generacion").alias("generacion"),
        col("country").alias("pais"),
        col("usernameType").alias("tipoUsuario")
    )
    
    # Eliminamos duplicados por si acaso el mismo usuario aparece dos veces
    df_usuario = df_usuario.dropDuplicates(["nombre", "email"])

    # 5. Añadimos un ID autoincremental de PySpark
    df_usuario = df_usuario.withColumn("idUsuario", monotonically_increasing_id())

    # Mostramos el resultado por consola
    df_usuario.show(truncate=False)

    # 6. Guardamos los datos procesados (Formato Parquet es el estándar en Big Data, 
    # pero podemos usar CSV si prefieres inyectarlo luego a MySQL)
    ruta_salida = "data/processed_data/dim_usuario"
    print(f"Guardando datos en {ruta_salida}...")
    
    df_usuario.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(ruta_salida)
        
    print("¡Dimensión Usuario completada con éxito!")
    spark.stop()

if __name__ == "__main__":
    procesar_dim_usuario()