import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

HIVE_HOST = os.getenv("HIVE_HOST", "localhost")
HIVE_PORT = os.getenv("HIVE_PORT", "10000")
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "spotify_dw")
HDFS_HOST = os.getenv("HDFS_HOST", "localhost")
HDFS_PORT = os.getenv("HDFS_PORT", "9000")


def get_spark_session(app_name: str) -> SparkSession:
    """
    Crea o reutiliza una SparkSession configurada para el ecosistema Hadoop/Hive.
    Todos los ETLs del proyecto deben usar esta función en lugar de construir
    su propia sesión, para garantizar una configuración uniforme.

    Args:
        app_name: Nombre de la aplicación Spark (aparece en la Spark UI).

    Returns:
        SparkSession lista para usar.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)

        # --- Integración con Hive ---
        # Activa el soporte para leer y escribir tablas Hive (HiveQL, metastore, etc.)
        .enableHiveSupport()
        .config("hive.metastore.uris", f"thrift://{HIVE_HOST}:{HIVE_PORT}")

        # --- Integración con HDFS ---
        # Le decimos a Spark dónde está el sistema de archivos distribuido
        .config("fs.defaultFS", f"hdfs://{HDFS_HOST}:{HDFS_PORT}")

        # --- Formato por defecto: Parquet ---
        # Parquet es columnar, comprimido y el estándar de Big Data.
        # Hive también lo usará como formato de almacenamiento en nuestras tablas.
        .config("spark.sql.sources.default", "parquet")

        # --- Optimizaciones generales ---
        # Reduce el número de particiones tras un shuffle (joins, groupBy).
        # 200 es el valor por defecto de Spark; con datasets medianos, 50 es más eficiente.
        .config("spark.sql.shuffle.partitions", "50")

        .getOrCreate()
    )

    # Seleccionamos la base de datos de Hive para no tener que prefijarlo en cada query
    spark.sql(f"USE {HIVE_DATABASE}")

    # Nivel de log a WARN para no ensuciar la consola con INFO de Spark
    spark.sparkContext.setLogLevel("WARN")

    return spark