#!/usr/bin/env python3
"""
upload_to_bronze.py

Sube todos los datos crudos a HDFS replicando la estructura local.

Estructura en HDFS:
    /user/spotify_bi/bronze/
    ├── usuarios/
    │   ├── ALEX/
    │   │   ├── StreamingHistory_music_0.json
    │   │   ├── StreamingHistory_music_1.json
    │   │   └── Userdata.json
    │   ├── BEA/
    │   │   └── ...
    │   └── ...
    ├── features/
    │   ├── features_historico.csv
    │   ├── features_kaggle.csv
    │   ├── features_kaggle_2.csv
    │   └── canciones_features_kaggle.csv
    └── enrichment/
        ├── artistas_info.csv
        ├── artistas_generos.csv
        └── albums_info.csv

Uso:
    python src/ingestion/upload_to_bronze.py
    python src/ingestion/upload_to_bronze.py --force   # sobreescribe todo
"""
import os
import sys
import argparse
import subprocess
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIGURACIÓN
# ============================================================
HDFS_BASE  = os.getenv("HDFS_BASE_PATH", "/user/spotify_bi/bronze")
LOCAL_BASE = Path("data/raw")

# Archivos de usuario que nos interesan (el resto los ignoramos)
ARCHIVOS_USUARIO = {
    "StreamingHistory_music_*.json",
    "Userdata.json"
}

# ============================================================
# HELPERS
# ============================================================

def hdfs(*args) -> tuple[int, str, str]:
    """Ejecuta un comando hdfs dfs."""
    cmd = ["hdfs", "dfs"] + list(args)
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def hdfs_mkdir(ruta: str):
    hdfs("-mkdir", "-p", ruta)


def hdfs_exists(ruta: str) -> bool:
    code, _, _ = hdfs("-test", "-e", ruta)
    return code == 0


def subir_archivo(local: Path, hdfs_dest: str, force: bool) -> bool:
    """Sube un archivo a HDFS. Devuelve True si OK."""
    hdfs_ruta = f"{hdfs_dest}/{local.name}"

    if not force and hdfs_exists(hdfs_ruta):
        print(f"    [SKIP] {local.name}")
        return True

    args = ["-put", "-f", str(local), hdfs_dest] if force else ["-put", str(local), hdfs_dest]
    code, _, err = hdfs("-put", *([ "-f"] if force else []), str(local), hdfs_dest)
    if code != 0:
        print(f"    [ERROR] {local.name}: {err}")
        return False

    print(f"    [OK] {local.name}")
    return True


# ============================================================
# UPLOAD POR BLOQUES
# ============================================================

def subir_usuarios(force: bool) -> tuple[int, int]:
    """Sube los JSONs de cada usuario en su carpeta correspondiente."""
    ok, err = 0, 0
    carpeta_usuarios = LOCAL_BASE / "raw_data_spotify"

    if not carpeta_usuarios.exists():
        print(f"  [ERROR] No existe: {carpeta_usuarios}")
        return 0, 1

    # Cada subcarpeta es un usuario
    usuarios = [d for d in carpeta_usuarios.iterdir() if d.is_dir()]
    print(f"  Usuarios encontrados: {[u.name for u in usuarios]}")

    for usuario_dir in sorted(usuarios):
        nombre_usuario = usuario_dir.name
        # Buscar la carpeta "Spotify Account Data" dentro si existe
        spotify_data = usuario_dir / "Spotify Account Data"
        fuente = spotify_data if spotify_data.exists() else usuario_dir

        hdfs_usuario = f"{HDFS_BASE}/usuarios/{nombre_usuario}"
        hdfs_mkdir(hdfs_usuario)
        print(f"\n  Usuario: {nombre_usuario}")

        # Subir solo los archivos relevantes
        archivos_subidos = 0
        for patron in ARCHIVOS_USUARIO:
            for archivo in sorted(fuente.glob(patron)):
                resultado = subir_archivo(archivo, hdfs_usuario, force)
                if resultado:
                    ok += 1
                    archivos_subidos += 1
                else:
                    err += 1

        if archivos_subidos == 0:
            print(f"    [WARN] No se encontraron archivos relevantes en {fuente}")

    return ok, err


def subir_features(force: bool) -> tuple[int, int]:
    """Sube los CSVs de features."""
    ok, err = 0, 0
    hdfs_features = f"{HDFS_BASE}/features"
    hdfs_mkdir(hdfs_features)

    archivos = [
        LOCAL_BASE / "features_historico.csv",
        *sorted(LOCAL_BASE.glob("features_kaggle*.csv")),
        LOCAL_BASE / "temp_api" / "canciones_features_kaggle.csv",
        LOCAL_BASE / "temp_api" / "canciones_unicas.json",
    ]

    for archivo in archivos:
        if not archivo.exists():
            print(f"    [SKIP] No existe localmente: {archivo.name}")
            continue
        resultado = subir_archivo(archivo, hdfs_features, force)
        if resultado:
            ok += 1
        else:
            err += 1

    return ok, err


def subir_enrichment(force: bool) -> tuple[int, int]:
    """Sube los CSVs de enriquecimiento (MusicBrainz, Every Noise)."""
    ok, err = 0, 0
    hdfs_enrichment = f"{HDFS_BASE}/enrichment"
    hdfs_mkdir(hdfs_enrichment)

    archivos = [
        LOCAL_BASE / "temp_api" / "artistas_info.csv",
        LOCAL_BASE / "temp_api" / "artistas_generos.csv",
        LOCAL_BASE / "temp_api" / "albums_info.csv",
    ]

    for archivo in archivos:
        if not archivo.exists():
            print(f"    [SKIP] No existe localmente: {archivo.name}")
            continue
        resultado = subir_archivo(archivo, hdfs_enrichment, force)
        if resultado:
            ok += 1
        else:
            err += 1

    return ok, err


# ============================================================
# MAIN
# ============================================================

def upload_to_bronze(force: bool = False):
    print("=" * 60)
    print("  SUBIDA A HDFS — CAPA BRONZE")
    print("=" * 60)

    # Verificar HDFS activo
    print("\nVerificando HDFS...")
    code, _, err = hdfs("-ls", "/")
    if code != 0:
        print(f"[ERROR] HDFS no responde: {err}")
        print("Ejecuta: start-dfs.sh")
        sys.exit(1)
    print("  HDFS activo ✓")

    total_ok, total_err = 0, 0

    # 1. Usuarios
    print(f"\n{'─'*60}")
    print("  1. USUARIOS (StreamingHistory + Userdata)")
    print(f"{'─'*60}")
    ok, err = subir_usuarios(force)
    total_ok += ok
    total_err += err

    # 2. Features
    print(f"\n{'─'*60}")
    print("  2. FEATURES (histórico + Kaggle + merged)")
    print(f"{'─'*60}")
    ok, err = subir_features(force)
    total_ok += ok
    total_err += err

    # 3. Enrichment
    print(f"\n{'─'*60}")
    print("  3. ENRICHMENT (MusicBrainz + Every Noise)")
    print(f"{'─'*60}")
    ok, err = subir_enrichment(force)
    total_ok += ok
    total_err += err

    # Resumen
    print(f"\n{'='*60}")
    print(f"  RESUMEN")
    print(f"  Archivos subidos : {total_ok}")
    print(f"  Errores          : {total_err}")
    print(f"  Base HDFS        : {HDFS_BASE}")
    print(f"{'='*60}")

    # Mostrar estructura final
    print("\nEstructura en HDFS:")
    _, out, _ = hdfs("-ls", "-R", HDFS_BASE)
    print(out)

    if total_err > 0:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sube datos crudos a HDFS Bronze")
    parser.add_argument("--force", "-f", action="store_true",
                        help="Sobreescribe archivos existentes en HDFS")
    args = parser.parse_args()
    upload_to_bronze(force=args.force)