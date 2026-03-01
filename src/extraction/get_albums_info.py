"""
get_albums_info.py

Enriquece los artistas con tipo y país usando la API REST de MusicBrainz.
Usa requests directamente (más estable en Windows que musicbrainzngs).

Uso:
    python src/extraction/get_artists_info.py
"""
import json
import csv
import os
import time
import requests

# ============================================================
# CONFIGURACIÓN
# ============================================================
INPUT_FILE  = "data/raw/temp_api/artistas_unicos.json"
OUTPUT_FILE = "data/raw/temp_api/artistas_info.csv"

MB_BASE_URL = "https://musicbrainz.org/ws/2"
HEADERS = {
    "User-Agent": "TFG-SpotifyBI/1.0 (antonio99guillo@gmail.com)",
    "Accept": "application/json"
}
DELAY    = 1.1   # Rate limit MusicBrainz: 1 req/s
TIMEOUT  = 15    # Timeout por petición
INTENTOS = 3     # Reintentos por artista

# ============================================================
# HELPERS
# ============================================================

def cargar_checkpoint() -> set:
    procesados = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                procesados.add(row["Artista"])
    return procesados


def buscar_artista(nombre: str) -> dict:
    vacio = {"Artista": nombre, "Tipo": "Desconocido", "País": "Desconocido"}

    for intento in range(INTENTOS):
        try:
            resp = requests.get(
                f"{MB_BASE_URL}/artist",
                params={"query": nombre, "limit": 1, "fmt": "json"},
                headers=HEADERS,
                timeout=TIMEOUT
            )

            if resp.status_code == 429:
                print(f"  [RATE LIMIT] Esperando 30s...")
                time.sleep(30)
                continue

            if resp.status_code != 200:
                print(f"  [HTTP {resp.status_code}] {nombre}")
                break

            data = resp.json()
            artistas = data.get("artists", [])
            if artistas:
                a = artistas[0]
                return {
                    "Artista": nombre,
                    "Tipo":    a.get("type", "Desconocido"),
                    "País":    a.get("country", "Desconocido")
                }
            return vacio

        except requests.exceptions.Timeout:
            espera = (intento + 1) * 5
            print(f"  [TIMEOUT] {nombre} — reintentando en {espera}s")
            time.sleep(espera)

        except requests.exceptions.ConnectionError as e:
            espera = (intento + 1) * 5
            print(f"  [CONEXIÓN] {nombre} (intento {intento+1}/{INTENTOS}) — reintentando en {espera}s")
            time.sleep(espera)

        except Exception as e:
            print(f"  [ERROR] {nombre}: {e}")
            break

    return vacio


# ============================================================
# MAIN
# ============================================================

def get_artists_info():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: No se encuentra {INPUT_FILE}")
        return

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        artistas = [item["artista"] for item in json.load(f)]

    procesados = cargar_checkpoint()
    pendientes = [a for a in artistas if a not in procesados]

    print(f"Total artistas : {len(artistas)}")
    print(f"Ya procesados  : {len(procesados)}")
    print(f"Pendientes     : {len(pendientes)}")

    if not pendientes:
        print("¡Todos los artistas ya están procesados!")
        return

    archivo_existe = os.path.exists(OUTPUT_FILE)
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Artista", "Tipo", "País"])
        if not archivo_existe:
            writer.writeheader()

        for i, nombre in enumerate(pendientes):
            if i % 100 == 0:
                print(f"  Procesando {i}/{len(pendientes)}...")

            writer.writerow(buscar_artista(nombre))
            f.flush()
            time.sleep(DELAY)

    print(f"\n¡Completado! -> {OUTPUT_FILE}")


if __name__ == "__main__":
    get_artists_info()