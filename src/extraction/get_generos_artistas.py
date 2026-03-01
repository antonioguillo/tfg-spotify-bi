"""
get_artists_genres.py

Obtiene el género musical de cada artista haciendo scraping de Every Noise at Once.
Tiene checkpoint — si se interrumpe, continúa desde donde lo dejó.

Uso:
    python src/extraction/get_artists_genres.py
"""
import json
import csv
import os
import time
import requests
from bs4 import BeautifulSoup

# ============================================================
# CONFIGURACIÓN
# ============================================================
INPUT_FILE  = "data/raw/temp_api/artistas_unicos.json"
OUTPUT_FILE = "data/raw/temp_api/artistas_generos.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# Segundos entre peticiones para no saturar el servidor
DELAY = 0.5

# ============================================================
# HELPERS
# ============================================================

def cargar_checkpoint() -> set:
    """Devuelve el conjunto de artistas ya procesados."""
    procesados = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                procesados.add(row["Artista"])
    return procesados


def obtener_genero(nombre: str) -> str:
    """
    Scraping de Every Noise at Once para obtener el género principal del artista.
    Devuelve 'Desconocido' si no encuentra nada o hay error.
    """
    try:
        query = "+".join(nombre.split())
        url = f"https://everynoise.com/lookup.cgi?who={query}"

        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            print(f"  [HTTP {response.status_code}] {nombre}")
            return "Desconocido"

        soup = BeautifulSoup(response.text, "html.parser")

        # Every Noise devuelve los géneros en enlaces dentro de la respuesta
        # El primero es el género principal
        genero_element = soup.find("a", href=True)
        if genero_element:
            return genero_element.text.strip()

        return "Desconocido"

    except requests.exceptions.Timeout:
        print(f"  [TIMEOUT] {nombre}")
        return "Desconocido"
    except Exception as e:
        print(f"  [ERROR] {nombre}: {e}")
        return "Desconocido"


# ============================================================
# MAIN
# ============================================================

def get_artists_genres():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: No se encuentra {INPUT_FILE}")
        print("Ejecuta primero extraction_set_up.py")
        return

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        artistas = [item["artista"] for item in json.load(f)]

    procesados = cargar_checkpoint()
    pendientes = [a for a in artistas if a not in procesados]

    print(f"Total artistas  : {len(artistas)}")
    print(f"Ya procesados   : {len(procesados)}")
    print(f"Pendientes      : {len(pendientes)}")

    if not pendientes:
        print("¡Todos los artistas ya tienen género!")
        return

    archivo_existe = os.path.exists(OUTPUT_FILE)
    cabeceras = ["Artista", "Genero"]

    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cabeceras)
        if not archivo_existe:
            writer.writeheader()

        for i, nombre in enumerate(pendientes):
            if i % 100 == 0:
                print(f"  Procesando {i}/{len(pendientes)}...")

            genero = obtener_genero(nombre)
            writer.writerow({"Artista": nombre, "Genero": genero})
            f.flush()

            time.sleep(DELAY)

    print(f"\n¡Completado! Datos guardados en {OUTPUT_FILE}")


if __name__ == "__main__":
    get_artists_genres()