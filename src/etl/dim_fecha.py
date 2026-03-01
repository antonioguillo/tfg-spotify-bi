import holidays
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, date_format,
    when, dayofweek, udf
)
from pyspark.sql.types import BooleanType
from src.utils.spark_session import get_spark_session


def procesar_dim_fecha():
    spark = get_spark_session("ETL_Dimension_Fecha")

    print("1. Generando calendario base 2015-2025...")
    df_date = spark.sql("""
        SELECT explode(sequence(
            to_date('2015-01-01'),
            to_date('2025-12-31'),
            interval 1 day
        )) AS fechaString
    """)

    print("2. Extrayendo componentes de fecha...")
    df_date = df_date \
        .withColumn("dia",       dayofmonth(col("fechaString"))) \
        .withColumn("mes",       month(col("fechaString"))) \
        .withColumn("año",       year(col("fechaString"))) \
        .withColumn("mesString", date_format(col("fechaString"), "MMMM")) \
        .withColumn("fechaString", date_format(col("fechaString"), "yyyy-MM-dd"))

    print("3. Calculando estación del año...")
    df_date = df_date.withColumn("estacion",
        when(col("mes").isin(1, 2, 3),  "Invierno")
        .when(col("mes").isin(4, 5, 6), "Primavera")
        .when(col("mes").isin(7, 8, 9), "Verano")
        .otherwise("Otoño")
    )

    print("4. Calculando fin de semana...")
    # dayofweek: 1=Domingo, 7=Sábado en Spark
    df_date = df_date.withColumn("finde",
        when(dayofweek(col("fechaString")).isin(1, 7), True).otherwise(False)
    )

    print("5. Calculando festivos de España...")
    dias_festivos = holidays.Spain(years=range(2015, 2026))

    def es_festivo(fecha_str):
        try:
            from datetime import date
            parts = fecha_str.split("-")
            d = date(int(parts[0]), int(parts[1]), int(parts[2]))
            return d in dias_festivos
        except Exception:
            return False

    festivo_udf = udf(es_festivo, BooleanType())
    df_date = df_date.withColumn("festivo", festivo_udf(col("fechaString")))

    print("6. Generando Smart Key YYYYMMDD...")
    df_date = df_date.withColumn("idDate",
        date_format(col("fechaString"), "yyyyMMdd").cast("integer")
    )

    # Reordenamos columnas igual que el schema de Hive
    df_date = df_date.select(
        "idDate", "dia", "mes", "año", "festivo",
        "finde", "fechaString", "mesString", "estacion"
    )

    df_date.show(5)

    print("7. Guardando en Hive (Parquet)...")
    df_date.write.mode("overwrite").format("parquet").saveAsTable("dim_fecha")
    print("¡Dimensión Fecha completada!")


if __name__ == "__main__":
    procesar_dim_fecha()