import json
import csv
import os
import time
import requests
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv

load_dotenv()

os.environ['NO_PROXY'] = '*'

INPUT_FILE = 'data/raw/temp_api/canciones_unicas.json'
OUTPUT_FILE = 'data/raw/temp_api/canciones_features.csv'

CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')

if not CLIENT_ID or not CLIENT_SECRET:
    raise EnvironmentError("Faltan SPOTIFY_CLIENT_ID o SPOTIFY_CLIENT_SECRET en el archivo .env")

def inicializar_spotify():
    session = requests.Session()
    session.trust_env = False

    client_credentials_manager = SpotifyClientCredentials(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        requests_session=session
    )

    return spotipy.Spotify(
        client_credentials_manager=client_credentials_manager,
        requests_session=session,
        retries=3,
        status_retries=3
    )

def extraer_features():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: No se encuentra {INPUT_FILE}.")
        return

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        canciones = json.load(f)

    canciones_procesadas = set()
    archivo_existe = os.path.exists(OUTPUT_FILE)

    if archivo_existe:
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                canciones_procesadas.add((row['cancion'], row['artista']))

    canciones_restantes = [c for c in canciones if (c['cancion'], c['artista']) not in canciones_procesadas]

    print(f"Total de canciones en JSON: {len(canciones)}")
    print(f"Canciones ya procesadas: {len(canciones_procesadas)}")
    print(f"Iniciando extracción para {len(canciones_restantes)} canciones restantes...")

    if not canciones_restantes:
        print("¡Todas las canciones ya han sido procesadas!")
        return

    sp = inicializar_spotify()
    cabeceras = ['cancion', 'artista', 'album', 'id_spotify', 'danceability', 'energy', 'key',
                 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness',
                 'liveness', 'valence', 'tempo']

    with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=cabeceras)
        if not archivo_existe:
            writer.writeheader()

        for i, item in enumerate(canciones_restantes):
            cancion = item['cancion']
            artista = item['artista']

            if i % 50 == 0 and i > 0:
                print(f"Procesando {i}/{len(canciones_restantes)}...")

            try:
                query = f"track:{cancion} artist:{artista}"
                resultados = sp.search(q=query, type='track', limit=1)

                if not resultados['tracks']['items']:
                    fila_vacia = {col: '' for col in cabeceras}
                    fila_vacia['cancion'] = cancion
                    fila_vacia['artista'] = artista
                    fila_vacia['album'] = 'No Encontrado'
                    writer.writerow(fila_vacia)
                    csvfile.flush()
                    continue

                track_info = resultados['tracks']['items'][0]
                track_id = track_info['id']
                nombre_album = track_info['album']['name']

                features_list = sp.audio_features(track_id)
                if not features_list or features_list[0] is None:
                    continue

                features = features_list[0]

                fila = {
                    'cancion': cancion,
                    'artista': artista,
                    'album': nombre_album,
                    'id_spotify': track_id,
                    'danceability': features.get('danceability'),
                    'energy': features.get('energy'),
                    'key': features.get('key'),
                    'loudness': features.get('loudness'),
                    'mode': features.get('mode'),
                    'speechiness': features.get('speechiness'),
                    'acousticness': features.get('acousticness'),
                    'instrumentalness': features.get('instrumentalness'),
                    'liveness': features.get('liveness'),
                    'valence': features.get('valence'),
                    'tempo': features.get('tempo')
                }
                writer.writerow(fila)
                csvfile.flush()

            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 429:
                    retry_after = int(e.headers.get('Retry-After', 60)) if e.headers else 60
                    print(f"\n[!] Límite de peticiones alcanzado. Esperando {retry_after} segundos...")
                    time.sleep(retry_after + 1)
                    sp = inicializar_spotify()
                else:
                    print(f"Error de Spotify al buscar '{cancion}': {e}")

            except Exception as e:
                print(f"Error procesando '{cancion}': {e}")

            finally:
                time.sleep(0.3)

    print(f"\n¡Extracción finalizada! Datos guardados en {OUTPUT_FILE}")

if __name__ == "__main__":
    extraer_features()