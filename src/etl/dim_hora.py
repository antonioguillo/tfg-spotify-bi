from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.utils.spark_session import get_spark_session


def procesar_dim_hora():
    spark = get_spark_session("ETL_Dimension_Hora")

    print("1. Creando franjas horarias...")
    datos = [
        (0, "Madrugada", 0,  5),
        (1, "Mañana",    6,  11),
        (2, "Tarde",     12, 17),
        (3, "Noche",     18, 23)
    ]

    esquema = StructType([
        StructField("idHora",        IntegerType(), False),
        StructField("franjaHoraria", StringType(),  True),
        StructField("inicio",        IntegerType(), True),
        StructField("final",         IntegerType(), True)
    ])

    df_hora = spark.createDataFrame(datos, schema=esquema)
    df_hora.show()

    print("2. Guardando en Hive (Parquet)...")
    df_hora.write.mode("overwrite").format("parquet").saveAsTable("dim_hora")
    print("¡Dimensión Hora completada!")


if __name__ == "__main__":
    procesar_dim_hora()