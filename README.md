# Spotify BI — Data Warehouse con Apache Hive y Kylin

Pipeline ETL completo que transforma el historial personal de Spotify en un
Data Warehouse de esquema estrella (Kimball), listo para análisis OLAP con
Apache Kylin.

## Arquitectura

```
Spotify JSON / Kaggle CSV / MusicBrainz API
          │
          ▼
   [Extracción Python]
          │
          ▼
   HDFS Bronze (datos crudos inmutables)
          │
          ▼
   [ETL PySpark — dimensiones + tabla de hechos]
          │
          ▼
   Hive Data Warehouse (Parquet, esquema estrella)
          │
          ▼
   Apache Kylin (cubo OLAP)
```

**Tablas del DW:** `dim_artista`, `dim_album`, `dim_cancion`, `dim_fecha`,
`dim_hora`, `dim_usuario`, `fact_historial`.

## Prerrequisitos

| Componente | Versión mínima | Notas |
|---|---|---|
| Java (JDK) | 8 | Requerido por Hadoop y Hive |
| Hadoop HDFS | 3.x | `start-dfs.sh` debe estar en PATH |
| Apache Hive | 3.x | Metastore local o remoto |
| Apache Spark | 3.x | Con soporte Hive (`--packages`) |
| Python | 3.9+ | Entorno virtual recomendado |
| Apache Kylin | 4.x | Opcional, para cubo OLAP |

### Dependencias Python

```bash
pip install pyspark requests pandas python-dotenv
```

## Configuración

1. Copia el fichero de entorno y rellena las credenciales:

```bash
cp .env.example .env
```

Variables requeridas en `.env`:

```
HDFS_BASE_PATH=/user/spotify_bi/bronze   # ruta base en HDFS
SPOTIFY_CLIENT_ID=...                    # API de Spotify (opcional)
SPOTIFY_CLIENT_SECRET=...
```

2. Coloca los datos de exportación de Spotify en `data/raw/raw_data_spotify/`:

```
data/raw/raw_data_spotify/
├── ALEX/
│   ├── StreamingHistory_music_0.json
│   ├── StreamingHistory_music_1.json
│   └── Playlist1.json
└── MARIA/
    ├── StreamingHistory_music_0.json
    └── Playlist1.json
```

## Ejecución del pipeline

El orquestador completo es `run_pipeline.sh`. Ejecutar desde WSL2 o Linux:

```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
```

El script ejecuta en orden:

| Paso | Script | Descripción |
|------|--------|-------------|
| 1 | `src/extraction/extraction_set_up.py` | Genera lista de canciones únicas del historial |
| 2 | `src/extraction/merge_features_kaggle.py` | Merge de audio features (Kaggle + API Spotify) |
| 3 | `src/extraction/get_info_artistas.py` | Tipo y país de artistas (MusicBrainz) |
| 3 | `src/extraction/get_generos_artistas.py` | Géneros musicales (Every Noise at Once) |
| 3 | `src/extraction/get_albums_info.py` | Productoras de álbumes (MusicBrainz) |
| 4 | `src/ingestion/upload_raw.py` | Subida de datos crudos a HDFS (capa Bronze) |
| 5 | `ddl/hive_schema.sql` | Creación del esquema estrella en Hive |
| 6 | `src/etl/dim_*.py` | ETL de todas las dimensiones (spark-submit) |
| 7 | `src/etl/fact_table.py` | ETL de la tabla de hechos (spark-submit) |
| 8 | Verificación | Conteo de registros en cada tabla Hive |

Los logs se guardan en `logs/pipeline_YYYYMMDD_HHMMSS.log`.

### Ejecución individual de un ETL

```bash
spark-submit --master local[*] --driver-memory 4g src/etl/dim_artista.py
spark-submit --master local[*] --driver-memory 4g src/etl/fact_table.py
```

### Crear solo el esquema Hive

```bash
hive -f ddl/hive_schema.sql
```

## Estructura del repositorio

```
tfg-spotify-bi/
├── ddl/
│   └── hive_schema.sql          # DDL del esquema estrella en Hive
├── src/
│   ├── extraction/              # Scripts de extracción y enriquecimiento
│   ├── ingestion/
│   │   └── upload_raw.py        # Sube datos crudos a HDFS Bronze
│   ├── etl/
│   │   ├── dim_artista.py
│   │   ├── dim_album.py
│   │   ├── dim_cancion.py
│   │   ├── dim_fecha.py
│   │   ├── dim_hora.py
│   │   ├── dim_usuario.py
│   │   └── fact_table.py
│   └── utils/
│       ├── paths.py             # Rutas HDFS centralizadas
│       └── spark_session.py     # Configuración de SparkSession con Hive
├── data/
│   └── raw/                     # Datos locales (no versionados)
├── logs/                        # Logs del pipeline (no versionados)
├── run_pipeline.sh              # Orquestador completo
└── .env.example                 # Plantilla de variables de entorno
```

## Siguiente paso

Tras completar el pipeline, construir el cubo OLAP en Apache Kylin:

```
Kylin UI: http://localhost:7070
```
