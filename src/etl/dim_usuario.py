from pyspark.sql.functions import col, to_date, year, when, monotonically_increasing_id
from src.utils.spark_session import get_spark_session


def procesar_dim_usuario():
    spark = get_spark_session("ETL_Dimension_Usuario")

    print("1. Leyendo archivos Userdata.json de todos los usuarios...")
    ruta = "data/raw/raw_data_spotify/*/*/Userdata.json"
    df_usuario = spark.read.option("multiline", "true").json(ruta)

    print("2. Transformando datos...")
    df_usuario = df_usuario.withColumn("birth_year", year(to_date(col("birthdate"), "yyyy-MM-dd")))

    df_usuario = df_usuario.withColumn("generacion",
        when(col("birth_year") < 1946, "Generación Silenciosa")
        .when(col("birth_year") < 1965, "Baby Boomers")
        .when(col("birth_year") < 1981, "Generación X")
        .when(col("birth_year") < 1997, "Millennials")
        .otherwise("Generación Z")
    )

    df_usuario = df_usuario.select(
        col("username").alias("nombre"),
        col("email").alias("email"),
        col("generacion"),
        col("country").alias("pais"),
        col("usernameType").alias("tipoUsuario")
    ).dropDuplicates(["nombre", "email"])

    print("3. Generando IDs...")
    df_usuario = df_usuario.withColumn("idUsuario", monotonically_increasing_id())

    df_usuario.show(truncate=False)

    print("4. Guardando en Hive (Parquet)...")
    df_usuario.write.mode("overwrite").format("parquet").saveAsTable("dim_usuario")
    print("¡Dimensión Usuario completada!")


if __name__ == "__main__":
    procesar_dim_usuario()