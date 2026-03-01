import pandas as pd

# Ruta al CSV que acabamos de generar
CSV_FILE = 'data/raw/temp_api/canciones_features_kaggle.csv'

def revisar_datos():
    print("Cargando el dataset cruzado...\n")
    df = pd.read_csv(CSV_FILE)
    
    total_canciones = len(df)
    
    # Contamos cuántos nulos hay en una columna clave (ej. danceability)
    nulos = df['danceability'].isna().sum()
    porcentaje_nulos = (nulos / total_canciones) * 100
    
    print(f"Total de canciones en tu historial: {total_canciones}")
    print(f"Canciones SIN datos (Nulos): {nulos} ({porcentaje_nulos:.2f}%)")
    print("-" * 40)
    
    if porcentaje_nulos == 0:
        print("¡Increíble! Tienes el 100% de los datos. Listo para Power BI/Tableau.")
    elif porcentaje_nulos < 15:
        print("El porcentaje de pérdida es bajo. Puedes eliminar estas filas tranquilamente.")
    else:
        print("Tienes bastantes nulos. Habrá que decidir si rellenarlos con la media del artista o buscar otra estrategia.")
        
    # Mostrar una muestra de las canciones que NO se encontraron
    print("\nEjemplo de canciones que Kaggle NO encontró:")
    print(df[df['danceability'].isna()][['cancion', 'artista']].head(10))

if __name__ == "__main__":
    revisar_datos()