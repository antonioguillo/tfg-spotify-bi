-- ============================================================
-- HIVE SCHEMA - Spotify BI Data Warehouse
-- Arquitectura: Esquema Estrella (Kimball)
-- Formato: Parquet (columnar, optimizado para OLAP y Kylin)
-- Base de datos: spotify_dw
--
-- NOTA: Hive no soporta constraints (FK, PK, UNIQUE) ni índices
-- tradicionales. La integridad referencial se garantiza en el
-- proceso ETL (PySpark). Los comentarios documentan las relaciones.
-- ============================================================

CREATE DATABASE IF NOT EXISTS spotify_dw
COMMENT 'Data Warehouse del historial de reproduccion de Spotify';

USE spotify_dw;


-- ============================================================
-- DIMENSIÓN ARTISTA
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_artista (
    idArtista       INT         COMMENT 'Clave primaria. -1 para registros desconocidos.',
    nombre          STRING      COMMENT 'Nombre del artista.',
    pais            STRING      COMMENT 'Pais de origen del artista.',
    tipo            STRING      COMMENT 'Tipo: Solista, Grupo, Orquesta, etc.',
    genero          STRING      COMMENT 'Genero musical principal.'
)
COMMENT 'Dimension que describe los artistas del historial de reproduccion.'
STORED AS PARQUET;


-- ============================================================
-- DIMENSIÓN ÁLBUM
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_album (
    idAlbum         INT         COMMENT 'Clave primaria. -1 para registros desconocidos.',
    nombre          STRING      COMMENT 'Nombre del album.',
    artista         STRING      COMMENT 'Artista del album.',
    productora      STRING      COMMENT 'Productora o sello discografico.'
)
COMMENT 'Dimension que describe los albumes del historial de reproduccion.'
STORED AS PARQUET;


-- ============================================================
-- DIMENSIÓN CANCIÓN
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_cancion (
    idCancion       INT         COMMENT 'Clave primaria. -1 para registros desconocidos.',
    titulo          STRING      COMMENT 'Titulo de la cancion.',
    album           STRING      COMMENT 'Album al que pertenece.',
    artista         STRING      COMMENT 'Artista de la cancion.',
    playlist        BOOLEAN     COMMENT 'Indica si la cancion esta en alguna playlist del usuario.',
    rangoDuracion   STRING      COMMENT 'Rango de duracion: 0-1, 2-3, 4-6, 6-10 min, 10<.'
)
COMMENT 'Dimension que describe las canciones escuchadas.'
STORED AS PARQUET;


-- ============================================================
-- DIMENSIÓN FECHA
-- Smart Key en formato YYYYMMDD para eficiencia en Kylin.
-- Incluye finde (fin de semana) del modelo original MySQL.
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_fecha (
    idDate          INT         COMMENT 'Smart Key formato YYYYMMDD (ej: 20230815).',
    dia             INT         COMMENT 'Dia del mes (1-31).',
    mes             INT         COMMENT 'Mes del ano (1-12).',
    año             INT         COMMENT 'Ano (ej: 2023).',
    festivo         BOOLEAN     COMMENT 'Indica si el dia es festivo en Espana.',
    finde           BOOLEAN     COMMENT 'Indica si el dia es sabado o domingo.',
    fechaString     STRING      COMMENT 'Fecha en formato texto YYYY-MM-DD.',
    mesString       STRING      COMMENT 'Nombre del mes en texto (ej: January).',
    estacion        STRING      COMMENT 'Estacion del ano: Invierno, Primavera, Verano, Otono.'
)
COMMENT 'Dimension de tiempo con calendario completo 2015-2025.'
STORED AS PARQUET;


-- ============================================================
-- DIMENSIÓN HORA
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_hora (
    idHora          INT         COMMENT 'Clave primaria (0-3).',
    franjaHoraria   STRING      COMMENT 'Franja: Madrugada, Manana, Tarde, Noche.',
    inicio          INT         COMMENT 'Hora de inicio de la franja (0-23).',
    final           INT         COMMENT 'Hora de fin de la franja (0-23).'
)
COMMENT 'Dimension de franjas horarias del dia.'
STORED AS PARQUET;


-- ============================================================
-- DIMENSIÓN USUARIO
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_usuario (
    idUsuario       INT         COMMENT 'Clave primaria.',
    generacion      STRING      COMMENT 'Generacion: Millennials, Generacion Z, etc.',
    pais            STRING      COMMENT 'Pais de la cuenta de Spotify.',
    tipoUsuario     STRING      COMMENT 'Tipo de cuenta: premium, free, etc.',
    nombre          STRING      COMMENT 'Nombre de usuario de Spotify.',
    email           STRING      COMMENT 'Email del usuario.'
)
COMMENT 'Dimension que describe los usuarios analizados.'
STORED AS PARQUET;


-- ============================================================
-- TABLA DE HECHOS - HISTORIAL DE REPRODUCCIÓN
-- Grano: una única reproducción (stream).
--
-- Las audio features se mantienen aquí (igual que en el modelo
-- MySQL original) porque son métricas del evento de escucha.
-- Esto permite agregarlas en Kylin: energía media por hora,
-- danceability media por género, etc.
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_historial (
    idhechos_historial  INT     COMMENT 'Clave primaria de la tabla de hechos.',

    -- Métricas de escucha
    msEscuchados        INT     COMMENT 'Milisegundos reproducidos de la cancion.',
    msNoEscuchados      INT     COMMENT 'Milisegundos no reproducidos (msTotal - msEscuchados).',
    cancionesEscuchadas INT     COMMENT 'Contador de reproducciones. Siempre 1 por fila.',
    msTotal             INT     COMMENT 'Duracion total de la cancion en milisegundos.',

    -- Audio features (agregables en el cubo OLAP)
    danceability        DOUBLE  COMMENT 'Bailabilidad (0.0 - 1.0).',
    energy              DOUBLE  COMMENT 'Energia (0.0 - 1.0).',
    key                 INT     COMMENT 'Tonalidad musical (0-11).',
    loudness            DOUBLE  COMMENT 'Volumen en decibelios.',
    mode                INT     COMMENT 'Modalidad: 1 mayor, 0 menor.',
    speechiness         DOUBLE  COMMENT 'Presencia de voz hablada (0.0 - 1.0).',
    acousticness        DOUBLE  COMMENT 'Nivel de acustica (0.0 - 1.0).',
    instrumentalness    DOUBLE  COMMENT 'Nivel instrumental (0.0 - 1.0).',
    liveness            DOUBLE  COMMENT 'Probabilidad de ser directo (0.0 - 1.0).',
    valence             DOUBLE  COMMENT 'Positividad musical (0.0 - 1.0).',
    tempo               DOUBLE  COMMENT 'Tempo en BPM.',

    -- Claves foráneas
    IDCancion           INT     COMMENT 'FK -> dim_cancion.idCancion.',
    IDArtista           INT     COMMENT 'FK -> dim_artista.idArtista.',
    IDAlbum             INT     COMMENT 'FK -> dim_album.idAlbum.',
    IDHora              INT     COMMENT 'FK -> dim_hora.idHora.',
    IDDate              INT     COMMENT 'FK -> dim_fecha.idDate.',
    IDUsuario           INT     COMMENT 'FK -> dim_usuario.idUsuario.'
)
COMMENT 'Tabla de hechos central. Grano: una reproduccion de Spotify.'
STORED AS PARQUET
TBLPROPERTIES ('transactional'='false');