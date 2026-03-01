import pandas as pd
import glob
import os

def unificar_csvs():
    # 1. Rutas corregidas con tu usuario "anton" y "Desktop"
    ruta_carpeta = r"C:\Users\anton\Desktop\TFG\SPOTIFYFINAL (1)\SPOTIFYFINAL\features"
    ruta_salida_dir = r"C:\Users\anton\Desktop\TFG\tfg-spotify-bi\data\raw"
    
    print(f"Buscando archivos CSV en: {ruta_carpeta}...")
    
    # 2. Buscamos todos los archivos que terminen en .csv en esa carpeta
    archivos_csv = glob.glob(os.path.join(ruta_carpeta, "*.csv"))
    
    if not archivos_csv:
        print("❌ No se encontraron archivos CSV. Comprueba la ruta.")
        return

    print(f"✅ Se han encontrado {len(archivos_csv)} archivos. Unificando...")
    
    # 3. Leemos y guardamos cada DataFrame en una lista
    lista_df = []
    for archivo in archivos_csv:
        try:
            df = pd.read_csv(archivo)
            lista_df.append(df)
        except Exception as e:
            print(f"Error leyendo {archivo}: {e}")
            
    # 4. Concatenamos todos los DataFrames en uno solo
    df_features = pd.concat(lista_df, ignore_index=True)
    
    # 5. Limpieza: Eliminamos duplicados
    filas_antes = len(df_features)
    
    if 'trackName' in df_features.columns and 'artistName' in df_features.columns:
        df_features = df_features.drop_duplicates(subset=['trackName', 'artistName'])
        filas_despues = len(df_features)
        print(f"🧹 Se han eliminado {filas_antes - filas_despues} filas duplicadas.")
    else:
        df_features = df_features.drop_duplicates()
        print("🧹 Se han eliminado filas duplicadas exactas.")

    # 6. Creamos la carpeta de salida si no existe y guardamos
    os.makedirs(ruta_salida_dir, exist_ok=True)
    ruta_salida = os.path.join(ruta_salida_dir, "canciones_features_KAGGLE_UNIFICADO.csv")
    
    df_features.to_csv(ruta_salida, index=False, encoding='utf-8')
    
    print(f"🎉 ¡Éxito! Archivo final guardado en: {ruta_salida}")
    print(f"📊 Total de canciones únicas listas para el TFG: {len(df_features)}")

if __name__ == "__main__":
    unificar_csvs()