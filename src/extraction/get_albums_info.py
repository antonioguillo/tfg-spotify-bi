"""
get_albums_info.py

Enriquece los álbumes con su productora/sello discográfico usando la API
REST de MusicBrainz. Busca cada combinación (álbum, artista) como un
release-group para obtener el label asociado.

Tiene checkpoint — si se interrumpe, continúa desde donde lo dejó.

Uso:
    python src/extraction/get_albums_info.py
"""
import json
import csv
import os
import time
import requests
import pandas as pd

# ============================================================
# CONFIGURACIÓN
# ============================================================
INPUT_FILE  = "data/raw/temp_api/canciones_features_kaggle.csv"
OUTPUT_FILE = "data/raw/temp_api/albums_info.csv"

MB_BASE_URL = "https://musicbrainz.org/ws/2"
HEADERS = {
    "User-Agent": "TFG-SpotifyBI/1.0 (antonio99guillo@gmail.com)",
    "Accept": "application/json"
}
DELAY    = 1.1   # Rate limit MusicBrainz: 1 req/s
TIMEOUT  = 15    # Timeout por petición
INTENTOS = 3     # Reintentos por álbum

# ============================================================
# HELPERS
# ============================================================

def cargar_checkpoint() -> set:
    """Devuelve el conjunto de álbumes ya procesados como tuplas (Album, Artista)."""
    procesados = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                procesados.add((row["Album"], row["Artista"]))
    return procesados


def buscar_productora(album: str, artista: str) -> dict:
    """
    Busca la productora/sello discográfico de un álbum en MusicBrainz.

    Estrategia:
    1. Busca el release-group por nombre de álbum + artista.
    2. Si encuentra un resultado, obtiene los releases asociados.
    3. Del primer release con label, extrae la productora.

    Returns:
        dict con claves Album, Artista, Productora
    """
    vacio = {"Album": album, "Artista": artista, "Productora": "Desconocido"}

    for intento in range(INTENTOS):
        try:
            # Paso 1: Buscar el release (no release-group) directamente
            # ya que los releases tienen la info de label
            query = f'release:"{album}" AND artist:"{artista}"'
            resp = requests.get(
                f"{MB_BASE_URL}/release",
                params={"query": query, "limit": 3, "fmt": "json"},
                headers=HEADERS,
                timeout=TIMEOUT
            )

            if resp.status_code == 429:
                print(f"  [RATE LIMIT] Esperando 30s...")
                time.sleep(30)
                continue

            if resp.status_code != 200:
                print(f"  [HTTP {resp.status_code}] {album} — {artista}")
                break

            data = resp.json()
            releases = data.get("releases", [])

            # Buscar el primer release que tenga un label
            for release in releases:
                label_info = release.get("label-info", [])
                for li in label_info:
                    label = li.get("label", {})
                    label_name = label.get("name", "")
                    if label_name and label_name != "[no label]":
                        return {
                            "Album": album,
                            "Artista": artista,
                            "Productora": label_name
                        }

            # Si ningún release tiene label, intentar búsqueda más amplia
            # solo por nombre de álbum
            if not releases:
                resp2 = requests.get(
                    f"{MB_BASE_URL}/release",
                    params={"query": f'release:"{album}"', "limit": 1, "fmt": "json"},
                    headers=HEADERS,
                    timeout=TIMEOUT
                )
                time.sleep(DELAY)

                if resp2.status_code == 200:
                    data2 = resp2.json()
                    for release in data2.get("releases", []):
                        for li in release.get("label-info", []):
                            label = li.get("label", {})
                            label_name = label.get("name", "")
                            if label_name and label_name != "[no label]":
                                return {
                                    "Album": album,
                                    "Artista": artista,
                                    "Productora": label_name
                                }

            return vacio

        except requests.exceptions.Timeout:
            espera = (intento + 1) * 5
            print(f"  [TIMEOUT] {album} — reintentando en {espera}s")
            time.sleep(espera)

        except requests.exceptions.ConnectionError:
            espera = (intento + 1) * 5
            print(f"  [CONEXIÓN] {album} (intento {intento+1}/{INTENTOS}) — reintentando en {espera}s")
            time.sleep(espera)

        except Exception as e:
            print(f"  [ERROR] {album}: {e}")
            break

    return vacio


# ============================================================
# MAIN
# ============================================================

def get_albums_info():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: No se encuentra {INPUT_FILE}")
        print("Ejecuta primero merge_features_kaggle.py")
        return

    # Leer álbumes únicos desde el CSV de features
    print("Cargando álbumes únicos desde canciones_features_kaggle.csv...")
    df = pd.read_csv(INPUT_FILE)
    albums_unicos = df[["album", "artista"]].drop_duplicates()

    # Filtrar los que ya son "Desconocido" o vacíos
    albums_unicos = albums_unicos[
        (albums_unicos["album"].notna()) &
        (albums_unicos["album"] != "Desconocido") &
        (albums_unicos["album"].str.strip() != "")
    ]

    albums_list = list(albums_unicos.itertuples(index=False, name=None))

    procesados = cargar_checkpoint()
    pendientes = [(a, art) for a, art in albums_list if (a, art) not in procesados]

    print(f"Total álbumes únicos : {len(albums_list)}")
    print(f"Ya procesados        : {len(procesados)}")
    print(f"Pendientes           : {len(pendientes)}")

    if not pendientes:
        print("¡Todos los álbumes ya están procesados!")
        return

    archivo_existe = os.path.exists(OUTPUT_FILE)
    cabeceras = ["Album", "Artista", "Productora"]

    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cabeceras)
        if not archivo_existe:
            writer.writeheader()

        for i, (album, artista) in enumerate(pendientes):
            if i % 50 == 0:
                print(f"  Procesando {i}/{len(pendientes)}...")

            resultado = buscar_productora(album, artista)
            writer.writerow(resultado)
            f.flush()
            time.sleep(DELAY)

    # Resumen final
    total = len(procesados) + len(pendientes)
    df_resultado = pd.read_csv(OUTPUT_FILE)
    encontrados = len(df_resultado[df_resultado["Productora"] != "Desconocido"])
    print(f"\n{'='*55}")
    print(f"¡Completado! -> {OUTPUT_FILE}")
    print(f"  Total álbumes     : {total}")
    print(f"  Con productora    : {encontrados} ({encontrados/total*100:.1f}%)")
    print(f"  Sin productora    : {total - encontrados} ({(total-encontrados)/total*100:.1f}%)")
    print(f"{'='*55}")


if __name__ == "__main__":
    get_albums_info()