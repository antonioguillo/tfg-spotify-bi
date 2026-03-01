"""
paths.py

Rutas centralizadas del proyecto. Todos los ETLs deben importar
las rutas de aquí en lugar de hardcodearlas.

Las rutas HDFS apuntan a la capa Bronze donde upload_raw.py
depositó los datos crudos. Spark las resuelve contra fs.defaultFS
(hdfs://localhost:9000) configurado en spark_session.py.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# HDFS — Capa Bronze (datos crudos inmutables)
# ============================================================
HDFS_BASE = os.getenv("HDFS_BASE_PATH", "/user/spotify_bi/bronze")

# Historiales de streaming y datos de usuario
HDFS_USUARIOS       = f"{HDFS_BASE}/usuarios"
# Patrón para leer TODOS los historiales de todos los usuarios
HDFS_STREAMING      = f"{HDFS_USUARIOS}/*/StreamingHistory_music_*.json"
# Patrón para leer TODOS los Userdata.json
HDFS_USERDATA       = f"{HDFS_USUARIOS}/*/Userdata.json"

# Features y datasets
HDFS_FEATURES       = f"{HDFS_BASE}/features"
HDFS_FEATURES_CSV   = f"{HDFS_FEATURES}/canciones_features_kaggle.csv"
HDFS_CANCIONES_JSON = f"{HDFS_FEATURES}/canciones_unicas.json"

# Enriquecimiento
HDFS_ENRICHMENT     = f"{HDFS_BASE}/enrichment"
HDFS_ARTISTAS_INFO  = f"{HDFS_ENRICHMENT}/artistas_info.csv"
HDFS_ARTISTAS_GEN   = f"{HDFS_ENRICHMENT}/artistas_generos.csv"
HDFS_ALBUMS_INFO    = f"{HDFS_ENRICHMENT}/albums_info.csv"

# ============================================================
# LOCAL — Solo para scripts de extracción (pre-ingesta)
# ============================================================
LOCAL_RAW            = "data/raw"
LOCAL_TEMP_API       = "data/raw/temp_api"
LOCAL_RAW_SPOTIFY    = "data/raw/raw_data_spotify"