from pyspark.sql.functions import col, lit, to_date, year, when, row_number, input_file_name, regexp_extract
from pyspark.sql.window import Window
from src.utils.spark_session import get_spark_session
from src.utils.paths import HDFS_USERDATA


def procesar_dim_usuario():
    spark = get_spark_session("ETL_Dimension_Usuario")

    # ==========================================================
    # 1. Leer Userdata.json desde HDFS Bronze
    # input_file_name() debe llamarse antes de cualquier join
    # ==========================================================
    print("1. Leyendo archivos Userdata.json de todos los usuarios (HDFS Bronze)...")
    print(f"   Ruta: {HDFS_USERDATA}")
    df_usuario = spark.read.option("multiline", "true").json(HDFS_USERDATA) \
        .withColumn("nombre_carpeta", regexp_extract(input_file_name(), r"usuarios/([^/]+)/", 1))

    # ==========================================================
    # 2. Transformar datos
    # ==========================================================
    print("2. Transformando datos...")
    df_usuario = df_usuario.withColumn("birth_year", year(to_date(col("birthdate"), "yyyy-MM-dd")))

    df_usuario = df_usuario.withColumn("generacion",
        when(col("birth_year") < 1946, "Generacion Silenciosa")
        .when(col("birth_year") < 1965, "Baby Boomers")
        .when(col("birth_year") < 1981, "Generacion X")
        .when(col("birth_year") < 1997, "Millennials")
        .otherwise("Generacion Z")
    )

    # nombre = nombre de carpeta HDFS (ALEX, SERGI, YO...) para que el join
    # en fact_table.py funcione (extrae el mismo valor del path del archivo)
    df_usuario = df_usuario.select(
        col("nombre_carpeta").alias("nombre"),
        col("email").alias("email"),
        col("generacion"),
        col("country").alias("pais"),
        lit("desconocido").alias("tipoUsuario")
    ).dropDuplicates(["nombre"])

    # ==========================================================
    # 3. Generar IDs + guardar en Hive
    # ==========================================================
    print("3. Generando IDs secuenciales (row_number, empieza en 1)...")
    w = Window.orderBy("nombre")
    df_usuario = df_usuario.withColumn("idUsuario", row_number().over(w))

    df_usuario.show(truncate=False)

    print("4. Guardando en Hive (Parquet)...")
    df_usuario.write.mode("overwrite").format("parquet").saveAsTable("dim_usuario")
    print("Dimension Usuario completada!")


if __name__ == "__main__":
    procesar_dim_usuario()