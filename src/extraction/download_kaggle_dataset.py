"""
download_kaggle_datasets.py

Descarga los datasets de Kaggle necesarios para el merge de features.
Requiere tener configurado kagglehub con tu API key de Kaggle.

Setup previo (solo una vez):
  pip install kagglehub
  Y tener el archivo ~/.kaggle/kaggle.json con tu API key,
  o la variable de entorno KAGGLE_KEY configurada en el .env
"""
import os
import shutil
import kagglehub

OUTPUT_DIR = "data/raw"

DATASETS = [
    {
        "handle": "maharshipandya/-spotify-tracks-dataset",
        "archivo_origen": "dataset.csv",
        "archivo_destino": "features_kaggle_2.csv"
    },
    {
        "handle": "rodolfofigueroa/spotify-12m-songs",
        "archivo_origen": "tracks_features.csv",
        "archivo_destino": "features_kaggle_3.csv"
    }
]

def descargar_datasets():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for dataset in DATASETS:
        print(f"\nDescargando: {dataset['handle']}...")
        try:
            path = kagglehub.dataset_download(dataset["handle"])
            print(f"  Descargado en: {path}")

            origen = os.path.join(path, dataset["archivo_origen"])
            destino = os.path.join(OUTPUT_DIR, dataset["archivo_destino"])

            if os.path.exists(origen):
                shutil.copy(origen, destino)
                print(f"  Copiado a: {destino}")
            else:
                # Si no está en la raíz, buscar recursivamente
                for root, _, files in os.walk(path):
                    for f in files:
                        if f == dataset["archivo_origen"]:
                            shutil.copy(os.path.join(root, f), destino)
                            print(f"  Copiado a: {destino}")
                            break

        except Exception as e:
            print(f"  Error descargando {dataset['handle']}: {e}")
            print(f"  Descárgalo manualmente desde kaggle.com y guárdalo como {destino}")

    print("\n¡Listo! Ahora puedes ejecutar merge_features_kaggle.py")

if __name__ == "__main__":
    descargar_datasets()