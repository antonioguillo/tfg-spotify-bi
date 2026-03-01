import os
import json

# Rutas de entrada y salida corregidas con tu estructura real
INPUT_DIR = 'data/raw/raw_data_spotify'
OUTPUT_DIR = 'data/raw/temp_api'

# Aseguramos que la carpeta de destino existe
os.makedirs(OUTPUT_DIR, exist_ok=True)

def extraer_unicos():
    
    ruta_absoluta = os.path.abspath(INPUT_DIR)
    print(f"\n[DEBUG] Python está buscando EXACTAMENTE en esta ruta:")
    print(f"-> {ruta_absoluta}")
    
    if not os.path.exists(INPUT_DIR):
        print(f"¡ALERTA! Python dice que esa carpeta NO EXISTE. Revisa los nombres.\n")
        return [], []
    
    print(f"Buscando historiales en: {INPUT_DIR}...")
    
    artistas_unicos = set()
    canciones_unicas = set() 
    total_reproducciones = 0

    # 1. Recorrer todas las carpetas (entrará solo a BEA, luego a Spotify Account Data, etc.)
    for root, dirs, files in os.walk(INPUT_DIR):
        for file in files:
            # Buscamos exactamente tus archivos de música
            if file.startswith('StreamingHistory_music_') and file.endswith('.json'):
                filepath = os.path.join(root, file)
                print(f"Leyendo: {filepath}")
                
                with open(filepath, 'r', encoding='utf-8') as f:
                    try:
                        data = json.load(f)
                        for row in data:
                            total_reproducciones += 1
                            
                            # Claves exactas de tu archivo StreamingHistory_music_0.json
                            artista = row.get('artistName')
                            cancion = row.get('trackName')
                            
                            if artista:
                                artistas_unicos.add(artista)
                            if artista and cancion:
                                # Guardamos la tupla (canción, artista)
                                canciones_unicas.add((cancion, artista))
                    except Exception as e:
                        print(f"Error leyendo {file}: {e}")

    print(f"\nResumen del escaneo:")
    print(f"- Total reproducciones analizadas: {total_reproducciones}")
    print(f"- Artistas únicos encontrados a nivel global: {len(artistas_unicos)}")
    print(f"- Canciones únicas encontradas a nivel global: {len(canciones_unicas)}")
    
    return list(artistas_unicos), list(canciones_unicas)

def guardar_listas_completas(artistas, canciones):
    """Guarda todos los datos únicos en dos únicos archivos JSON"""
    print(f"\nGuardando listas consolidadas en {OUTPUT_DIR}...")
    
    ruta_artistas = os.path.join(OUTPUT_DIR, 'artistas_unicos.json')
    ruta_canciones = os.path.join(OUTPUT_DIR, 'canciones_unicas.json')
    
    # Damos formato de diccionario y guardamos artistas
    datos_artistas = [{"artista": a} for a in sorted(artistas)]
    with open(ruta_artistas, 'w', encoding='utf-8') as f:
        json.dump(datos_artistas, f, ensure_ascii=False, indent=4)
        
    # Damos formato de diccionario y guardamos canciones
    datos_canciones = [{"cancion": c[0], "artista": c[1]} for c in sorted(canciones)]
    with open(ruta_canciones, 'w', encoding='utf-8') as f:
        json.dump(datos_canciones, f, ensure_ascii=False, indent=4)
        
    print(f"¡Éxito! Archivos creados:")
    print(f" -> {ruta_artistas}")
    print(f" -> {ruta_canciones}")

# Ejecución principal
if __name__ == "__main__":
    artistas, canciones = extraer_unicos()
    if artistas or canciones:
        guardar_listas_completas(artistas, canciones)
    else:
        print("No se encontraron datos. Asegúrate de que los archivos estén en data/raw/raw_data_spotify")