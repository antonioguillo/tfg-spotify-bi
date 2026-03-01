from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def procesar_dim_hora():
    spark = SparkSession.builder \
        .appName("ETL_Dimension_Hora") \
        .getOrCreate()

    print("1. Creando la matriz de franjas horarias...")
    # Definimos los datos exactamente igual que en tu lógica original
    datos = [
        (0, "Madrugada", 0, 5),
        (1, "Mañana", 6, 11),
        (2, "Tarde", 12, 17),
        (3, "Noche", 18, 23)
    ]
    
    # Definimos la estructura exacta (el Schema)
    esquema = StructType([
        StructField("idHora", IntegerType(), True),
        StructField("franjaHoraria", StringType(), True),
        StructField("inicio", IntegerType(), True),
        StructField("final", IntegerType(), True)
    ])
    
    print("2. Generando el DataFrame de PySpark...")
    df_hora = spark.createDataFrame(datos, schema=esquema)
    
    # Mostramos el resultado
    df_hora.show()

    ruta_salida = "data/processed_data/dim_hora"
    print(f"Guardando datos en {ruta_salida}...")
    df_hora.write.mode("overwrite").option("header", "true").csv(ruta_salida)
        
    print("¡Dimensión Hora completada con éxito!")
    spark.stop()

if __name__ == "__main__":
    procesar_dim_hora()