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
PYTHON="python3"
SPARK_SUBMIT="spark-submit"
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
    $SPARK_SUBMIT \
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
    error "Hive no está activo. Ejecuta: hiveserver2 &"
fi
success "Hive activo"

# ============================================================
# PASO 1: EXTRACCIÓN — Generar canciones únicas del historial
# ============================================================
step "1" "Extracción — Generando canciones únicas del historial"
run_python "src/extraction/extraction_set_up.py"

# ============================================================
# PASO 2: EXTRACCIÓN — Merge de features (Histórico + Kaggle + Síntesis)
# ============================================================
step "2" "Extracción — Merge de audio features"
run_python "src/extraction/merge_features_kaggle.py"

# Verificar que el output existe
if [ ! -f "$PROJECT_DIR/data/raw/temp_api/canciones_features_kaggle.csv" ]; then
    error "No se generó canciones_features_kaggle.csv"
fi
success "canciones_features_kaggle.csv generado"

# ============================================================
# PASO 3: ENRIQUECIMIENTO — MusicBrainz + Every Noise at Once
# ============================================================
step "3" "Enriquecimiento — MusicBrainz + Every Noise at Once"

log "Obteniendo tipo y país de artistas (MusicBrainz)..."
run_python "src/extraction/get_info_artistas.py"

log "Obteniendo géneros de artistas (Every Noise at Once)..."
run_python "src/extraction/get_generos_artistas.py"

log "Obteniendo productoras de álbumes (MusicBrainz)..."
run_python "src/extraction/get_albums_info.py"

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
