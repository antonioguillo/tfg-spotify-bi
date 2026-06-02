#!/usr/bin/env python3
"""
setup_kylin.py
Crea proyecto, carga tablas, modelo y cubo en Kylin 4.x (Spark3 + Parquet) via REST API.

Requisitos previos en WSL2:
  - HDFS activo        (start-dfs.sh)
  - Hive Metastore     (hive --service metastore &)
  - Kylin arrancado    ($KYLIN_HOME/bin/kylin.sh start)
  - Tablas ETL cargadas en Hive (haber ejecutado el pipeline completo)

Uso:
  cd /mnt/c/Users/anton/Desktop/TFG/tfg-spotify-bi
  .venv/bin/python3 scripts/setup_kylin.py
"""

import json
import base64
import sys
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

# ── Configuración ─────────────────────────────────────────────────────────────
HOST    = "http://localhost:7070"
USER    = "ADMIN"
PASSWD  = "KYLIN"
PROJECT = "spotify_bi"
DB      = "SPOTIFY_DW"

TABLES = [
    "FACT_HISTORIAL",
    "DIM_CANCION", "DIM_ARTISTA", "DIM_ALBUM",
    "DIM_FECHA",   "DIM_HORA",    "DIM_USUARIO",
]

# ── HTTP helper ───────────────────────────────────────────────────────────────

def _headers(content_type="application/json"):
    creds = base64.b64encode(f"{USER}:{PASSWD}".encode()).decode()
    return {"Authorization": f"Basic {creds}", "Content-Type": content_type}

def api(method, path, body=None, params=None, fatal=True, form=False):
    """
    body  → se serializa como JSON (application/json)
    params → se añaden como query string a la URL
    form  → body se codifica como form-urlencoded en lugar de JSON
    """
    url = f"{HOST}/kylin/api{path}"
    if params:
        url += "?" + urlencode(params)

    if form and body:
        data = urlencode(body).encode()
        headers = _headers("application/x-www-form-urlencoded")
    elif body is not None:
        data = json.dumps(body).encode()
        headers = _headers("application/json")
    else:
        data = None
        headers = _headers("application/json")

    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=90) as r:
            raw = r.read().decode()
            return json.loads(raw) if raw.strip() else {}
    except HTTPError as e:
        msg = e.read().decode()[:800]
        print(f"  HTTP {e.code}: {msg}")
        if fatal:
            sys.exit(1)
        return None
    except URLError as e:
        print(f"  No se puede conectar a Kylin en {HOST}: {e.reason}")
        if fatal:
            sys.exit(1)
        return None

# ── Descriptores Kylin ────────────────────────────────────────────────────────

MODEL_DESC = {
    "name": "FACT_HISTORIAL",
    "description": "Modelo dimensional Spotify BI — esquema estrella sobre historial de reproduccion",
    "fact_table": f"{DB}.FACT_HISTORIAL",
    "lookups": [
        {
            "table": f"{DB}.DIM_CANCION", "kind": "LOOKUP", "alias": "DIM_CANCION",
            "join": {
                "type": "left",
                "foreign_key": ["FACT_HISTORIAL.IDCANCION"],
                "primary_key":  ["DIM_CANCION.IDCANCION"]
            }
        },
        {
            "table": f"{DB}.DIM_ARTISTA", "kind": "LOOKUP", "alias": "DIM_ARTISTA",
            "join": {
                "type": "left",
                "foreign_key": ["FACT_HISTORIAL.IDARTISTA"],
                "primary_key":  ["DIM_ARTISTA.IDARTISTA"]
            }
        },
        {
            "table": f"{DB}.DIM_ALBUM", "kind": "LOOKUP", "alias": "DIM_ALBUM",
            "join": {
                "type": "left",
                "foreign_key": ["FACT_HISTORIAL.IDALBUM"],
                "primary_key":  ["DIM_ALBUM.IDALBUM"]
            }
        },
        {
            "table": f"{DB}.DIM_FECHA", "kind": "LOOKUP", "alias": "DIM_FECHA",
            "join": {
                "type": "left",
                "foreign_key": ["FACT_HISTORIAL.IDDATE"],
                "primary_key":  ["DIM_FECHA.IDDATE"]
            }
        },
        {
            "table": f"{DB}.DIM_HORA", "kind": "LOOKUP", "alias": "DIM_HORA",
            "join": {
                "type": "left",
                "foreign_key": ["FACT_HISTORIAL.IDHORA"],
                "primary_key":  ["DIM_HORA.IDHORA"]
            }
        },
        {
            "table": f"{DB}.DIM_USUARIO", "kind": "LOOKUP", "alias": "DIM_USUARIO",
            "join": {
                "type": "left",
                "foreign_key": ["FACT_HISTORIAL.IDUSUARIO"],
                "primary_key":  ["DIM_USUARIO.IDUSUARIO"]
            }
        },
    ],
    # En Kylin 4.x el modelo agrupa las columnas por tabla (no un objeto por columna)
    "dimensions": [
        {"table": "DIM_FECHA",   "columns": ["IDDATE", "ANIO", "MES", "DIA", "MESSTRING", "ESTACION", "FINDE", "FESTIVO", "FECHASTRING"]},
        {"table": "DIM_HORA",    "columns": ["IDHORA", "FRANJAHORARIA", "INICIO", "FINAL"]},
        {"table": "DIM_ARTISTA", "columns": ["IDARTISTA", "NOMBRE", "TIPO", "PAIS", "GENERO"]},
        {"table": "DIM_ALBUM",   "columns": ["IDALBUM", "NOMBRE", "ARTISTA", "PRODUCTORA"]},
        {"table": "DIM_CANCION", "columns": ["IDCANCION", "TITULO", "PLAYLIST", "RANGODURACION"]},
        {"table": "DIM_USUARIO", "columns": ["IDUSUARIO", "NOMBRE", "GENERACION", "PAIS", "TIPOUSUARIO"]},
    ],
    # Métricas: formato TABLE.COLUMN (confirmado por export real de Kylin)
    "metrics": [
        "FACT_HISTORIAL.MSESCUCHADOS", "FACT_HISTORIAL.MSNOESCUCHADOS",
        "FACT_HISTORIAL.MSTOTAL", "FACT_HISTORIAL.CANCIONESESCUCHADAS",
        "FACT_HISTORIAL.DANCEABILITY", "FACT_HISTORIAL.ENERGY",
        "FACT_HISTORIAL.LOUDNESS", "FACT_HISTORIAL.SPEECHINESS",
        "FACT_HISTORIAL.ACOUSTICNESS", "FACT_HISTORIAL.INSTRUMENTALNESS",
        "FACT_HISTORIAL.LIVENESS", "FACT_HISTORIAL.VALENCE",
        "FACT_HISTORIAL.TEMPO", "FACT_HISTORIAL.IDCANCION",
    ],
    "filter_condition": "",
    "partition_desc": {
        "partition_date_column": None,
        "partition_date_format": "yyyy-MM-dd",
        "partition_type": "APPEND"
    },
    "capacity": "MEDIUM"
}

CUBE_DESC = {
    "name": "spotify_bi_cube",
    "model_name": "FACT_HISTORIAL",
    "description": "Cubo OLAP para analisis multidimensional del historial de Spotify",
    "null_string": None,
    "dimensions": [
        # DIM_FECHA
        {"name": "ANIO",         "table": "DIM_FECHA",   "column": "ANIO",         "derived": None},
        {"name": "MES",          "table": "DIM_FECHA",   "column": "MES",          "derived": None},
        {"name": "DIA",          "table": "DIM_FECHA",   "column": "DIA",          "derived": None},
        {"name": "MESSTRING",    "table": "DIM_FECHA",   "column": "MESSTRING",    "derived": None},
        {"name": "ESTACION",     "table": "DIM_FECHA",   "column": "ESTACION",     "derived": None},
        {"name": "FINDE",        "table": "DIM_FECHA",   "column": "FINDE",        "derived": None},
        {"name": "FESTIVO",      "table": "DIM_FECHA",   "column": "FESTIVO",      "derived": None},
        {"name": "FECHASTRING",  "table": "DIM_FECHA",   "column": "FECHASTRING",  "derived": None},
        # DIM_HORA
        {"name": "FRANJAHORARIA","table": "DIM_HORA",    "column": "FRANJAHORARIA","derived": None},
        # DIM_ARTISTA
        {"name": "ARTISTA_NOMBRE","table": "DIM_ARTISTA","column": "NOMBRE",       "derived": None},
        {"name": "ARTISTA_GENERO","table": "DIM_ARTISTA","column": "GENERO",       "derived": None},
        {"name": "ARTISTA_PAIS",  "table": "DIM_ARTISTA","column": "PAIS",         "derived": None},
        {"name": "ARTISTA_TIPO",  "table": "DIM_ARTISTA","column": "TIPO",         "derived": None},
        # DIM_ALBUM
        {"name": "ALBUM_NOMBRE", "table": "DIM_ALBUM",   "column": "NOMBRE",       "derived": None},
        {"name": "PRODUCTORA",   "table": "DIM_ALBUM",   "column": "PRODUCTORA",   "derived": None},
        # DIM_CANCION
        {"name": "TITULO",       "table": "DIM_CANCION", "column": "TITULO",       "derived": None},
        {"name": "RANGODURACION","table": "DIM_CANCION", "column": "RANGODURACION","derived": None},
        {"name": "PLAYLIST",     "table": "DIM_CANCION", "column": "PLAYLIST",     "derived": None},
        # DIM_USUARIO
        {"name": "USUARIO_NOMBRE","table": "DIM_USUARIO","column": "NOMBRE",       "derived": None},
        {"name": "GENERACION",   "table": "DIM_USUARIO", "column": "GENERACION",   "derived": None},
        {"name": "USUARIO_PAIS", "table": "DIM_USUARIO", "column": "PAIS",         "derived": None},
        {"name": "TIPOUSUARIO",  "table": "DIM_USUARIO", "column": "TIPOUSUARIO",  "derived": None},
    ],
    "measures": [
        {
            "name": "_COUNT_",
            "function": {
                "expression": "COUNT",
                "parameter": {"type": "constant", "value": "1"},
                "returntype": "bigint"
            }
        },
        {
            "name": "MSESCUCHADOS_SUM",
            "function": {
                "expression": "SUM",
                "parameter": {"type": "column", "value": "FACT_HISTORIAL.MSESCUCHADOS"},
                "returntype": "bigint"
            }
        },
        {
            "name": "MSNOESCUCHADOS_SUM",
            "function": {
                "expression": "SUM",
                "parameter": {"type": "column", "value": "FACT_HISTORIAL.MSNOESCUCHADOS"},
                "returntype": "bigint"
            }
        },
        {
            "name": "MSTOTAL_SUM",
            "function": {
                "expression": "SUM",
                "parameter": {"type": "column", "value": "FACT_HISTORIAL.MSTOTAL"},
                "returntype": "bigint"
            }
        },
        {
            "name": "CANCIONESESCUCHADAS_SUM",
            "function": {
                "expression": "SUM",
                "parameter": {"type": "column", "value": "FACT_HISTORIAL.CANCIONESESCUCHADAS"},
                "returntype": "bigint"
            }
        },
        {
            "name": "CANCIONES_DISTINTAS",
            "function": {
                "expression": "COUNT_DISTINCT",
                "parameter": {"type": "column", "value": "FACT_HISTORIAL.IDCANCION"},
                "returntype": "bitmap"
            }
        },
        # Kylin 4.x no soporta AVG como medida nativa. Se usan SUM; Kylin
        # calcula AVG en query time como SUM(col)/_COUNT_ automáticamente.
        {
            "name": "DANCEABILITY_SUM",
            "function": {
                "expression": "SUM",
                "parameter": {"type": "column", "value": "FACT_HISTORIAL.DANCEABILITY"},
                "returntype": "double"
            }
        },
        {
            "name": "ENERGY_SUM",
            "function": {
                "expression": "SUM",
                "parameter": {"type": "column", "value": "FACT_HISTORIAL.ENERGY"},
                "returntype": "double"
            }
        },
        {
            "name": "VALENCE_SUM",
            "function": {
                "expression": "SUM",
                "parameter": {"type": "column", "value": "FACT_HISTORIAL.VALENCE"},
                "returntype": "double"
            }
        },
        {
            "name": "ACOUSTICNESS_SUM",
            "function": {
                "expression": "SUM",
                "parameter": {"type": "column", "value": "FACT_HISTORIAL.ACOUSTICNESS"},
                "returntype": "double"
            }
        },
        {
            "name": "TEMPO_SUM",
            "function": {
                "expression": "SUM",
                "parameter": {"type": "column", "value": "FACT_HISTORIAL.TEMPO"},
                "returntype": "double"
            }
        },
        {
            "name": "SPEECHINESS_SUM",
            "function": {
                "expression": "SUM",
                "parameter": {"type": "column", "value": "FACT_HISTORIAL.SPEECHINESS"},
                "returntype": "double"
            }
        },
        {
            "name": "INSTRUMENTALNESS_SUM",
            "function": {
                "expression": "SUM",
                "parameter": {"type": "column", "value": "FACT_HISTORIAL.INSTRUMENTALNESS"},
                "returntype": "double"
            }
        },
        {
            "name": "LIVENESS_SUM",
            "function": {
                "expression": "SUM",
                "parameter": {"type": "column", "value": "FACT_HISTORIAL.LIVENESS"},
                "returntype": "double"
            }
        },
    ],
    "rowkey": {
        "rowkey_columns": [
            {"column": "DIM_FECHA.ANIO",          "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_FECHA.MES",            "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_FECHA.DIA",            "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_FECHA.ESTACION",       "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_FECHA.FINDE",          "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_FECHA.FESTIVO",        "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_FECHA.MESSTRING",      "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_FECHA.FECHASTRING",    "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_HORA.FRANJAHORARIA",   "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_ARTISTA.NOMBRE",       "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_ARTISTA.GENERO",       "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_ARTISTA.PAIS",         "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_ARTISTA.TIPO",         "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_ALBUM.NOMBRE",         "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_ALBUM.PRODUCTORA",     "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_CANCION.TITULO",       "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_CANCION.RANGODURACION","encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_CANCION.PLAYLIST",     "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_USUARIO.NOMBRE",       "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_USUARIO.GENERACION",   "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_USUARIO.PAIS",         "encoding": "dict", "encoding_version": 1, "isShardBy": False},
            {"column": "DIM_USUARIO.TIPOUSUARIO",  "encoding": "dict", "encoding_version": 1, "isShardBy": False},
        ]
    },
    "aggregation_groups": [
        {
            "includes": [
                "DIM_FECHA.ANIO", "DIM_FECHA.MES", "DIM_FECHA.MESSTRING",
                "DIM_FECHA.DIA", "DIM_FECHA.ESTACION", "DIM_FECHA.FINDE",
                "DIM_FECHA.FESTIVO", "DIM_HORA.FRANJAHORARIA"
            ],
            "select_rule": {
                "hierarchy_dims": [["DIM_FECHA.ANIO", "DIM_FECHA.MES", "DIM_FECHA.DIA"]],
                "mandatory_dims": [],
                "joint_dims": []
            }
        },
        {
            "includes": [
                "DIM_ARTISTA.NOMBRE", "DIM_ARTISTA.GENERO",
                "DIM_ARTISTA.PAIS",   "DIM_ARTISTA.TIPO",
                "DIM_ALBUM.NOMBRE",   "DIM_ALBUM.PRODUCTORA",
                "DIM_CANCION.TITULO", "DIM_CANCION.RANGODURACION",
                "DIM_CANCION.PLAYLIST"
            ],
            "select_rule": {
                "hierarchy_dims": [
                    ["DIM_ARTISTA.NOMBRE", "DIM_ALBUM.NOMBRE", "DIM_CANCION.TITULO"]
                ],
                "mandatory_dims": [],
                "joint_dims": []
            }
        },
        {
            "includes": [
                "DIM_USUARIO.NOMBRE", "DIM_USUARIO.GENERACION",
                "DIM_USUARIO.PAIS",   "DIM_USUARIO.TIPOUSUARIO"
            ],
            "select_rule": {
                "hierarchy_dims": [],
                "mandatory_dims": [],
                "joint_dims": []
            }
        },
        {
            "includes": [
                "DIM_FECHA.ANIO",   "DIM_FECHA.MES",  "DIM_FECHA.ESTACION",
                "DIM_FECHA.FINDE",  "DIM_HORA.FRANJAHORARIA",
                "DIM_ARTISTA.GENERO", "DIM_ARTISTA.NOMBRE",
                "DIM_USUARIO.NOMBRE"
            ],
            "select_rule": {
                "hierarchy_dims": [],
                "mandatory_dims": [],
                "joint_dims": []
            }
        }
    ],
    "hbase_mapping": {
        "column_family": [
            {
                "name": "F1",
                "columns": [
                    {
                        "qualifier": "M",
                        "measure_refs": [
                            "_COUNT_",
                            "MSESCUCHADOS_SUM", "MSNOESCUCHADOS_SUM",
                            "MSTOTAL_SUM", "CANCIONESESCUCHADAS_SUM",
                            "CANCIONES_DISTINTAS",
                            "DANCEABILITY_SUM", "ENERGY_SUM", "VALENCE_SUM",
                            "ACOUSTICNESS_SUM", "TEMPO_SUM", "SPEECHINESS_SUM",
                            "INSTRUMENTALNESS_SUM", "LIVENESS_SUM"
                        ]
                    }
                ]
            }
        ]
    },
    "signature": None,
    "notify_list": [],
    "status_need_notify": ["ERROR", "DISCARDED", "READY"],
    "partition_date_start": 0,
    "auto_merge_time_ranges": [604800000, 2419200000],
    "retention_range": 0,
    "engine_type": 6,
    "storage_type": 4,
    "override_kylin_properties": {}
}

# ── Pasos ─────────────────────────────────────────────────────────────────────

def step(n, msg):
    print(f"\n{'='*60}")
    print(f"PASO {n}: {msg}")
    print(f"{'='*60}")

def check_kylin():
    step(0, "Verificando conexion con Kylin")
    r = api("GET", "/user/authentication", fatal=False)
    if r is None:
        print("  Kylin no responde. Asegurate de haber ejecutado kylin.sh start")
        sys.exit(1)
    print(f"  Kylin OK — usuario: {r.get('userDetails', {}).get('username', '?')}")

def create_project():
    step(1, f"Crear proyecto '{PROJECT}'")
    # Kylin 4.x: ProjectController.saveProject espera {"projectDescData": "{json_string}"}
    payload = {"projectDescData": json.dumps({"name": PROJECT, "description": "Spotify BI TFG"})}
    r = api("POST", "/projects", payload, fatal=False)
    if r is None:
        print("  El proyecto puede que ya exista — continuando...")
    else:
        print(f"  Proyecto creado: {r.get('name', r)}")

def load_tables():
    step(2, "Cargar tablas de Hive en Kylin")
    tables_str = ",".join(f"{DB}.{t}" for t in TABLES)

    # Kylin 4.x usa GET para cargar tablas desde Hive (no POST).
    # El endpoint acepta las tablas como query params.
    candidates = [
        ("GET",  "/tables/load",  {"tables": tables_str, "project": PROJECT, "calculate": "false"}),
        ("GET",  "/tables",       {"tables": tables_str, "project": PROJECT, "calculate": "false"}),
        ("PUT",  "/tables",       {"tables": tables_str, "project": PROJECT, "calculate": "false"}),
    ]

    r = None
    for method, path, params in candidates:
        print(f"  Probando {method} {path} ...")
        r = api(method, path, params=params, fatal=False)
        if r is not None:
            break

    if r is None:
        print("\n  No se encontró el endpoint correcto de forma automática.")
        print("  Carga las tablas manualmente y luego relanza el script desde --step=3:")
        print("    Kylin UI → (proyecto spotify_bi) → Data Source → Load Table")
        print("    Selecciona spotify_dw → marca las 7 tablas → Sync")
        sys.exit(1)

    loaded = r.get("result", {}).get("result", r)
    print(f"  OK: {loaded}")

def _model_exists():
    """Devuelve True si el modelo ya existe en el proyecto."""
    r = api("GET", "/models", params={"projectName": PROJECT}, fatal=False)
    if r is None:
        return False
    models = r if isinstance(r, list) else r.get("code", [])
    if isinstance(models, list):
        return any(m.get("name") == MODEL_DESC["name"] for m in models)
    return False

def create_model():
    step(3, "Crear modelo dimensional")

    if _model_exists():
        print(f"  Modelo '{MODEL_DESC['name']}' ya existe — saltando creación.")
        return

    # Formato Kylin 4.x: modelDescData como JSON string + project + modelName
    payload = {
        "modelDescData": json.dumps(MODEL_DESC),
        "modelName": MODEL_DESC["name"],
        "project": PROJECT
    }
    r = api("POST", "/models", payload, fatal=False)
    if r is None:
        print("  Fallo en creación de modelo (ver error arriba).")
        sys.exit(1)

    # Verificar que realmente se creó
    if not _model_exists():
        print(f"  ADVERTENCIA: la API respondió OK pero el modelo no aparece en GET /models.")
        print(f"  Respuesta recibida: {json.dumps(r)[:300]}")
        print(f"  Crea el modelo manualmente en la UI y relanza con --from-step=4")
        sys.exit(1)

    print(f"  Modelo creado y verificado: {r.get('modelName', MODEL_DESC['name'])}")

def create_cube():
    step(4, "Crear cubo OLAP")

    # Verificar primero que el modelo existe
    if not _model_exists():
        print(f"  ERROR: El modelo '{MODEL_DESC['name']}' no existe en el proyecto '{PROJECT}'.")
        print(f"  Ejecuta primero el paso 3 o créalo manualmente en la UI.")
        sys.exit(1)

    payload = {
        "cubeDescData": json.dumps(CUBE_DESC),
        "cubeName": CUBE_DESC["name"],
        "project": PROJECT
    }
    r = api("POST", "/cubes", payload, fatal=False)
    if r is None:
        print("  Error al crear cubo (ver error arriba).")
        print(f"  Revisa también: $KYLIN_HOME/logs/kylin.log")
        sys.exit(1)
    print(f"  Cubo creado: {r.get('cubeName', CUBE_DESC['name'])}")

def build_cube():
    step(5, "Lanzar build completo del cubo")
    cube_name = CUBE_DESC["name"]
    r = api("PUT", f"/cubes/{cube_name}/build", {
        "startTime": 0,
        "endTime": 0,
        "buildType": "BUILD"
    })
    job_id = r.get("uuid", "?")
    print(f"  Job lanzado: {job_id}")
    print(f"  Sigue el progreso en: {HOST}/kylin -> Monitor")
    print(f"  Cuando el job llegue a READY, el cubo esta listo para consultas.")

if __name__ == "__main__":
    # --from-step=N  salta pasos ya completados manualmente
    # Ejemplo: python3 setup_kylin.py --from-step=3  (salta proyecto y tablas)
    from_step = 1
    for arg in sys.argv[1:]:
        if arg.startswith("--from-step="):
            from_step = int(arg.split("=")[1])

    check_kylin()
    if from_step <= 1:
        create_project()
    if from_step <= 2:
        load_tables()
    if from_step <= 3:
        create_model()
    if from_step <= 4:
        create_cube()
    build_cube()
    print(f"\n{'='*60}")
    print("Pipeline Kylin completado.")
    print(f"Accede a {HOST}/kylin -> Insight para lanzar consultas SQL.")
    print(f"{'='*60}\n")
