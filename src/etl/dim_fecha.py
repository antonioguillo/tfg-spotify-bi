from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, date_format, when, udf
from pyspark.sql.types import BooleanType
import holidays

def procesar_dim_fecha():
    spark = SparkSession.builder \
        .appName("ETL_Dimension_Fecha") \
        .getOrCreate()

    print("1. Generando el calendario base (2015-2025)...")
    # Generamos todas las fechas posibles desde 2015 (creación de tu cuenta de Spotify aprox) hasta 2025
    df_date = spark.sql("""
        SELECT explode(sequence(to_date('2015-01-01'), to_date('2025-12-31'), interval 1 day)) as fechaString
    """)

    print("2. Extrayendo componentes (Día, Mes, Año)...")
    df_date = df_date.withColumn("dia", dayofmonth(col("fechaString"))) \
                     .withColumn("mes", month(col("fechaString"))) \
                     .withColumn("año", year(col("fechaString"))) \
                     .withColumn("mesString", date_format(col("fechaString"), "MMMM"))

    print("3. Calculando la estación del año...")
    # Sustituimos tu antiguo apply() por when() nativo de PySpark
    df_date = df_date.withColumn("estacion",
        when(col("mes").isin(1, 2, 3), "Invierno")
        .when(col("mes").isin(4, 5, 6), "Primavera")
        .when(col("mes").isin(7, 8, 9), "Verano")
        .otherwise("Otoño")
    )

    print("4. Integrando calendario de Festivos de España...")
    # Obtenemos los festivos de España para esos años
    dias_festivos = holidays.Spain(years=range(2015, 2026))
    
    # Creamos una UDF (User Defined Function) en PySpark para cruzar el festivo
    def es_festivo(fecha_val):
        return fecha_val in dias_festivos
    
    festivo_udf = udf(es_festivo, BooleanType())
    df_date = df_date.withColumn("festivo", festivo_udf(col("fechaString")))

    print("5. Generando Smart ID para BI...")
    # Creamos el ID inteligente YYYYMMDD (Ej: 2023-08-15 -> 20230815)
    df_date = df_date.withColumn("idDate", date_format(col("fechaString"), "yyyyMMdd").cast("integer"))

    # Mostramos el resultado por consola
    df_date.show(5)

    ruta_salida = "data/processed_data/dim_fecha"
    print(f"Guardando datos en {ruta_salida}...")
    df_date.write.mode("overwrite").option("header", "true").csv(ruta_salida)
        
    print("¡Dimensión Fecha completada con éxito!")
    spark.stop()

if __name__ == "__main__":
    procesar_dim_fecha()