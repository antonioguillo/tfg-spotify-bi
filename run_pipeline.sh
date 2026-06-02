#!/bin/bash
# ============================================================
# run_pipeline.sh - Orquestador del pipeline Spotify BI
# Ejecutar desde la raíz del proyecto en WSL2:
#   chmod +x run_pipeline.sh
#   ./run_pipeline.sh
# ============================================================

set -e  # Para el script si cualquier comando falla

# ============================================================
# CONFIGURACIÓN
# ============================================================
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/pipeline_$(date +%Y%m%d_%H%M%S).log"
VENV="$PROJECT_DIR/.venv"
PYTHON="$VENV/bin/python3"
SPARK_SUBMIT="$VENV/bin/spark-submit"
HIVE="hive"

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ============================================================
# HELPERS
# ============================================================

mkdir -p "$LOG_DIR"

log() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

success() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')] ✓ $1${NC}" | tee -a "$LOG_FILE"
}

error() {
    echo -e "${RED}[$(date '+%H:%M:%S')] ✗ ERROR: $1${NC}" | tee -a "$LOG_FILE"
    echo -e "${RED}Revisa el log completo en: $LOG_FILE${NC}"
    exit 1
}

warn() {
    echo -e "${YELLOW}[$(date '+%H:%M:%S')] ⚠ $1${NC}" | tee -a "$LOG_FILE"
}

step() {
    echo "" | tee -a "$LOG_FILE"
    echo -e "${BLUE}============================================================${NC}" | tee -a "$LOG_FILE"
    echo -e "${BLUE} PASO $1: $2${NC}" | tee -a "$LOG_FILE"
    echo -e "${BLUE}============================================================${NC}" | tee -a "$LOG_FILE"
}

run_python() {
    local script=$1
    log "Ejecutando: $script"
    $PYTHON "$PROJECT_DIR/$script" >> "$LOG_FILE" 2>&1 || error "Falló $script"
    success "$script completado"
}

run_spark() {
    local script=$1
    log "Ejecutando con spark-submit: $script"
    # Usamos el spark-submit del venv (pyspark 3.5.0) para evitar conflictos
    # con Kylin Spark (cloudpickle antiguo incompatible con Python 3.12+).
    # SPARK_HOME apunta al Spark bundled dentro del pyspark del venv,
    # donde vive spark-class. Sin esto, spark-submit busca en /bin/spark-class.
    # PYSPARK_PYTHON asegura que los workers usen el mismo Python que el driver.
    SPARK_HOME="$VENV/lib/python3.12/site-packages/pyspark" \
    PYSPARK_PYTHON="$VENV/bin/python3" \
    PYSPARK_DRIVER_PYTHON="$VENV/bin/python3" \
    PYTHONPATH="$PROJECT_DIR" \
    "$SPARK_SUBMIT" \
        --master local[*] \
        --driver-memory 4g \
        --executor-memory 4g \
        "$PROJECT_DIR/$script" >> "$LOG_FILE" 2>&1 || error "Falló $script"
    success "$script completado"
}

# ============================================================
# VERIFICACIONES PREVIAS
# ============================================================

echo -e "${BLUE}"
echo "  ███████╗██████╗  ██████╗ ████████╗██╗███████╗██╗   ██╗    ██████╗ ██╗"
echo "  ██╔════╝██╔══██╗██╔═══██╗╚══██╔══╝██║██╔════╝╚██╗ ██╔╝    ██╔══██╗██║"
echo "  ███████╗██████╔╝██║   ██║   ██║   ██║█████╗   ╚████╔╝     ██████╔╝██║"
echo "  ╚════██║██╔═══╝ ██║   ██║   ██║   ██║██╔══╝    ╚██╔╝      ██╔══██╗██║"
echo "  ███████║██║     ╚██████╔╝   ██║   ██║██║        ██║       ██████╔╝██║"
echo "  ╚══════╝╚═╝      ╚═════╝    ╚═╝   ╚═╝╚═╝        ╚═╝       ╚═════╝ ╚═╝"
echo -e "${NC}"
log "Iniciando pipeline Spotify BI — Log: $LOG_FILE"

# Verificar que estamos en el directorio correcto
cd "$PROJECT_DIR" || error "No se puede acceder a $PROJECT_DIR"

# Activar entorno virtual Python
if [ ! -f "$VENV/bin/python3" ]; then
    log "Creando entorno virtual en $VENV ..."
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install -r "$PROJECT_DIR/requirements.txt" --quiet \
        || error "Falló la instalación de dependencias Python"
    success "Entorno virtual creado e instalado"
fi

# Verificar que el .env existe
if [ ! -f "$PROJECT_DIR/.env" ]; then
    error "No se encuentra .env — copia .env.example y rellena las credenciales"
fi

# Verificar servicios Hadoop/Hive activos
log "Verificando servicios del stack..."

if ! hdfs dfs -ls / > /dev/null 2>&1; then
    error "HDFS no está activo. Ejecuta: start-dfs.sh"
fi
success "HDFS activo"

if ! $HIVE -e "SHOW DATABASES;" > /dev/null 2>&1; then
    error "Hive CLI no responde. Ejecuta: hiveserver2 &"
fi
success "Hive activo"

# Verificar que el Hive Metastore Thrift está activo en puerto 9083
# (necesario para que Spark pueda conectar con enableHiveSupport)
if ! nc -z localhost 9083 > /dev/null 2>&1; then
    warn "Hive Metastore no detectado en :9083 — arrancando en segundo plano..."
    nohup hive --service metastore >> "$LOG_DIR/metastore.log" 2>&1 &
    METASTORE_PID=$!
    log "Esperando que el metastore arranque (PID $METASTORE_PID)..."
    sleep 20
    if ! nc -z localhost 9083 > /dev/null 2>&1; then
        error "El Hive Metastore no arrancó. Revisa $LOG_DIR/metastore.log"
    fi
fi
success "Hive Metastore activo en :9083"

# ============================================================
# PASO 1: EXTRACCIÓN — Generar canciones únicas del historial
# ============================================================
step "1" "Extracción — Generando canciones únicas del historial"
run_python "src/extraction/extraction_set_up.py"

# ============================================================
# PASO 2: EXTRACCIÓN — Merge de features (Histórico + Kaggle + Síntesis)
# ============================================================
step "2" "Extracción — Merge de audio features"
FEATURES_OUT="$PROJECT_DIR/data/raw/temp_api/canciones_features_kaggle.csv"
FEATURES_CACHED="$PROJECT_DIR/data/raw/features/canciones_features_kaggle.csv"

if [ -f "$FEATURES_OUT" ]; then
    warn "canciones_features_kaggle.csv ya existe en temp_api — saltando merge (usa --force para regenerar)"
elif [ -f "$FEATURES_CACHED" ]; then
    warn "Usando canciones_features_kaggle.csv cacheado de data/raw/features/"
    cp "$FEATURES_CACHED" "$FEATURES_OUT"
    success "canciones_features_kaggle.csv copiado desde caché"
else
    run_python "src/extraction/merge_features_kaggle.py"
    if [ ! -f "$FEATURES_OUT" ]; then
        error "No se generó canciones_features_kaggle.csv"
    fi
    success "canciones_features_kaggle.csv generado"
fi

# ============================================================
# PASO 3: ENRIQUECIMIENTO — MusicBrainz + Every Noise at Once
# ============================================================
step "3" "Enriquecimiento — MusicBrainz + Every Noise at Once"

run_if_missing() {
    local output="$1"
    local script="$2"
    local label="$3"
    if [ -f "$PROJECT_DIR/$output" ]; then
        warn "$label ya existe — saltando (borra $output para regenerar)"
    else
        run_python "$script"
    fi
}

log "Obteniendo tipo y país de artistas (MusicBrainz)..."
run_if_missing "data/raw/temp_api/artistas_info.csv" \
    "src/extraction/get_info_artistas.py" "artistas_info.csv"

log "Obteniendo géneros de artistas (Every Noise at Once)..."
run_if_missing "data/raw/temp_api/artistas_generos.csv" \
    "src/extraction/get_generos_artistas.py" "artistas_generos.csv"

log "Obteniendo productoras de álbumes (MusicBrainz)..."
run_if_missing "data/raw/temp_api/albums_info.csv" \
    "src/extraction/get_albums_info.py" "albums_info.csv"

success "Enriquecimiento completado"

# ============================================================
# PASO 4: INGESTA — Subida de datos crudos a HDFS (capa Bronze)
# ============================================================
step "4" "Ingesta — Subiendo datos crudos a HDFS (capa Bronze)"
run_python "src/ingestion/upload_raw.py"
success "Datos crudos en HDFS Bronze"

# ============================================================
# PASO 5: DDL — Crear esquema en Hive
# ============================================================
step "5" "Data Warehouse — Creando esquema estrella en Hive"
log "Ejecutando hive_schema.sql..."
$HIVE -f "$PROJECT_DIR/ddl/hive_schema.sql" >> "$LOG_FILE" 2>&1 || error "Falló hive_schema.sql"
success "Esquema Hive creado"

# ============================================================
# PASO 6: ETL DIMENSIONES
# ============================================================
step "6" "ETL — Procesando dimensiones"

log "Dimensión Fecha..."
run_spark "src/etl/dim_fecha.py"

log "Dimensión Hora..."
run_spark "src/etl/dim_hora.py"

log "Dimensión Usuario..."
run_spark "src/etl/dim_usuario.py"

log "Dimensión Artista..."
run_spark "src/etl/dim_artista.py"

log "Dimensión Álbum..."
run_spark "src/etl/dim_album.py"

log "Dimensión Canción..."
run_spark "src/etl/dim_cancion.py"

# ============================================================
# PASO 7: ETL TABLA DE HECHOS
# ============================================================
step "7" "ETL — Procesando tabla de hechos"
run_spark "src/etl/fact_table.py"

# ============================================================
# PASO 8: VERIFICACIÓN FINAL
# ============================================================
step "8" "Verificación — Comprobando tablas en Hive"

log "Contando registros en cada tabla..."
$HIVE -e "
USE spotify_dw;
SELECT 'dim_artista'    AS tabla, COUNT(*) AS registros FROM dim_artista    UNION ALL
SELECT 'dim_album'      AS tabla, COUNT(*) AS registros FROM dim_album      UNION ALL
SELECT 'dim_cancion'    AS tabla, COUNT(*) AS registros FROM dim_cancion    UNION ALL
SELECT 'dim_fecha'      AS tabla, COUNT(*) AS registros FROM dim_fecha      UNION ALL
SELECT 'dim_hora'       AS tabla, COUNT(*) AS registros FROM dim_hora       UNION ALL
SELECT 'dim_usuario'    AS tabla, COUNT(*) AS registros FROM dim_usuario    UNION ALL
SELECT 'fact_historial' AS tabla, COUNT(*) AS registros FROM fact_historial;
" 2>&1 | tee -a "$LOG_FILE"

# ============================================================
# FIN
# ============================================================
echo "" | tee -a "$LOG_FILE"
echo -e "${GREEN}============================================================${NC}" | tee -a "$LOG_FILE"
echo -e "${GREEN} ✓ PIPELINE COMPLETADO EXITOSAMENTE${NC}" | tee -a "$LOG_FILE"
echo -e "${GREEN} Log completo: $LOG_FILE${NC}" | tee -a "$LOG_FILE"
echo -e "${GREEN}============================================================${NC}" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo -e "${YELLOW} Próximo paso: Construir el cubo OLAP en Apache Kylin${NC}" | tee -a "$LOG_FILE"
echo -e "${YELLOW} Kylin UI: http://localhost:7070${NC}" | tee -a "$LOG_FILE"
