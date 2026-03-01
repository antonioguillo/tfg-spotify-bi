import os
import re
import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process

# ============================================================
# RUTAS
# ============================================================
CANCIONES_UNICAS   = "data/raw/temp_api/canciones_unicas.json"
FEATURES_HISTORICO = "data/raw/features_historico.csv"
OUTPUT_FILE        = "data/raw/temp_api/canciones_features_kaggle.csv"

KAGGLE_DATASETS = [
    {
        "ruta": "data/raw/features_kaggle.csv",
        "col_cancion": "name",
        "col_artista": "artists",
        "col_album":   "album",
        "es_lista":    True
    },
    {
        "ruta": "data/raw/features_kaggle_2.csv",
        "col_cancion": "track_name",
        "col_artista": "artists",
        "col_album":   "album_name",
        "es_lista":    True
    },
    {
        "ruta": "data/raw/features_kaggle_3.csv",
        "col_cancion": "name",
        "col_artista": "artists",
        "col_album":   "album",
        "es_lista":    True
    }
]

FEATURE_COLS = [
    "danceability", "energy", "key", "loudness", "mode",
    "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo", "duration_ms"
]

# Umbral de similitud para rapidfuzz (0-100). 
# 85 = muy estricto, 75 = más matches pero algún falso positivo
FUZZY_THRESHOLD = 80

# ============================================================
# HELPERS
# ============================================================

def normalizar_texto(texto: str) -> str:
    if pd.isna(texto):
        return ""
    # Lowercase, strip, y eliminamos contenido entre paréntesis
    # (ej: "Blinding Lights (Radio Edit)" -> "blinding lights")
    texto = str(texto).strip().lower()
    texto = re.sub(r"\(.*?\)", "", texto).strip()
    texto = re.sub(r"\[.*?\]", "", texto).strip()
    texto = re.sub(r"\s+", " ", texto)
    return texto


def limpiar_artists_kaggle(valor: str) -> str:
    if pd.isna(valor):
        return ""
    limpio = re.sub(r"[\[\]'\"]", "", str(valor))
    return limpio.split(",")[0].strip()


def cruce_exacto(df_pendientes, df_fuente, on, cols_salida, origen_label):
    """Cruce exacto por las columnas indicadas."""
    df = df_pendientes.merge(
        df_fuente[on + cols_salida],
        on=on,
        how="left"
    )
    encontrados = df[df["origen"] == origen_label].copy()
    pendientes  = df[df["origen"].isna()][["cancion", "artista", "cancion_norm", "artista_norm"]].copy()
    return encontrados, pendientes


def cruce_fuzzy(df_pendientes: pd.DataFrame, df_kaggle: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cruce fuzzy usando rapidfuzz.
    Busca para cada canción pendiente la más similar en Kaggle
    usando token_sort_ratio sobre 'cancion_norm + artista_norm'.
    """
    print(f"  Preparando índice fuzzy sobre {len(df_kaggle)} canciones de Kaggle...")

    # Creamos una clave combinada para el matching
    df_kaggle = df_kaggle.copy()
    df_kaggle["clave_fuzzy"] = df_kaggle["cancion_norm"] + " " + df_kaggle["artista_norm"]
    claves_kaggle = df_kaggle["clave_fuzzy"].tolist()

    encontrados_fuzzy = []
    pendientes_final  = []

    total = len(df_pendientes)
    for i, (_, row) in enumerate(df_pendientes.iterrows()):
        if i % 1000 == 0:
            print(f"  Fuzzy: {i}/{total}...")

        clave_busqueda = row["cancion_norm"] + " " + row["artista_norm"]

        # rapidfuzz encuentra el mejor match y su score
        resultado = process.extractOne(
            clave_busqueda,
            claves_kaggle,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=FUZZY_THRESHOLD
        )

        if resultado:
            match_str, score, idx = resultado
            fila_kaggle = df_kaggle.iloc[idx]
            fila = {col: fila_kaggle[col] for col in FEATURE_COLS if col in fila_kaggle}
            fila["cancion"] = row["cancion"]
            fila["artista"] = row["artista"]
            fila["album"]   = fila_kaggle.get("album", "Desconocido")
            fila["origen"]  = "kaggle_fuzzy"
            fila["cancion_norm"] = row["cancion_norm"]
            fila["artista_norm"] = row["artista_norm"]
            encontrados_fuzzy.append(fila)
        else:
            pendientes_final.append(row.to_dict())

    df_encontrados = pd.DataFrame(encontrados_fuzzy) if encontrados_fuzzy else pd.DataFrame()
    df_pendientes  = pd.DataFrame(pendientes_final)  if pendientes_final  else pd.DataFrame()
    return df_encontrados, df_pendientes


def generar_features_sinteticas(df_sin: pd.DataFrame, df_reales: pd.DataFrame) -> pd.DataFrame:
    """Genera features sintéticas con media por artista o global + ruido gaussiano."""
    print(f"  Generando features sintéticas para {len(df_sin)} canciones...")

    medias_artista  = df_reales.groupby("artista_norm")[FEATURE_COLS].mean()
    medias_globales = df_reales[FEATURE_COLS].mean()

    filas = []
    for _, row in df_sin.iterrows():
        artista_norm = normalizar_texto(row.get("artista", ""))

        if artista_norm in medias_artista.index:
            features = medias_artista.loc[artista_norm].to_dict()
            origen   = "sintetico_artista"
        else:
            features = medias_globales.to_dict()
            origen   = "sintetico_global"

        for col in ["danceability", "energy", "valence", "acousticness",
                    "speechiness", "liveness", "instrumentalness"]:
            features[col] = float(np.clip(features[col] + np.random.normal(0, 0.03), 0.0, 1.0))

        features["cancion"] = row["cancion"]
        features["artista"] = row["artista"]
        features["album"]   = "Desconocido"
        features["origen"]  = origen
        filas.append(features)

    return pd.DataFrame(filas)


def cargar_kaggle_dataset(config: dict) -> pd.DataFrame | None:
    if not os.path.exists(config["ruta"]):
        print(f"   [SKIP] No encontrado: {config['ruta']}")
        return None

    df = pd.read_csv(config["ruta"])
    cols_disponibles = [c for c in FEATURE_COLS if c in df.columns]
    df = df[cols_disponibles + [config["col_cancion"], config["col_artista"], config["col_album"]]].copy()

    if config["es_lista"]:
        df["artista_limpio"] = df[config["col_artista"]].apply(limpiar_artists_kaggle)
    else:
        df["artista_limpio"] = df[config["col_artista"]]

    df = df.drop_duplicates(subset=[config["col_cancion"], "artista_limpio"])
    df["cancion_norm"] = df[config["col_cancion"]].apply(normalizar_texto)
    df["artista_norm"] = df["artista_limpio"].apply(normalizar_texto)
    df["album"]        = df[config["col_album"]]
    df["origen"]       = "kaggle"
    return df


# ============================================================
# MAIN
# ============================================================

def merge_features():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    # ----------------------------------------------------------
    # 1. Canciones únicas del historial
    # ----------------------------------------------------------
    print("1. Cargando canciones únicas del historial...")
    df_canciones = pd.read_json(CANCIONES_UNICAS)
    df_canciones.columns = ["cancion", "artista"]
    df_canciones["cancion_norm"] = df_canciones["cancion"].apply(normalizar_texto)
    df_canciones["artista_norm"] = df_canciones["artista"].apply(normalizar_texto)
    print(f"   Total: {len(df_canciones)}")

    # ----------------------------------------------------------
    # 2. Dataset histórico personal
    # ----------------------------------------------------------
    print("\n2. Cargando histórico personal...")
    df_hist = pd.read_csv(FEATURES_HISTORICO)
    df_hist = df_hist[FEATURE_COLS + ["trackName", "artistName"]].copy()
    df_hist = df_hist.drop_duplicates(subset=["trackName", "artistName"])
    df_hist["cancion_norm"] = df_hist["trackName"].apply(normalizar_texto)
    df_hist["artista_norm"] = df_hist["artistName"].apply(normalizar_texto)
    df_hist["album"]  = "Desconocido"
    df_hist["origen"] = "historico"
    print(f"   Canciones: {len(df_hist)}")

    # ----------------------------------------------------------
    # 3. Cargar y combinar todos los datasets de Kaggle
    # ----------------------------------------------------------
    print("\n3. Cargando datasets de Kaggle...")
    kaggle_dfs = []
    for config in KAGGLE_DATASETS:
        df_k = cargar_kaggle_dataset(config)
        if df_k is not None:
            kaggle_dfs.append(df_k)
            print(f"   {config['ruta']}: {len(df_k)} canciones")

    if kaggle_dfs:
        df_kaggle_all = pd.concat(kaggle_dfs, ignore_index=True)
        df_kaggle_all = df_kaggle_all.drop_duplicates(subset=["cancion_norm", "artista_norm"])
        print(f"   Total combinado (sin duplicados): {len(df_kaggle_all)}")
    else:
        df_kaggle_all = pd.DataFrame()

    cols_salida = FEATURE_COLS + ["album", "origen"]
    pendientes  = df_canciones[["cancion", "artista", "cancion_norm", "artista_norm"]].copy()

    # ----------------------------------------------------------
    # 4. Cruce exacto: histórico
    # ----------------------------------------------------------
    print("\n4. Cruce exacto con histórico...")
    encontrados_hist, pendientes = cruce_exacto(
        pendientes, df_hist,
        on=["cancion_norm", "artista_norm"],
        cols_salida=cols_salida,
        origen_label="historico"
    )
    print(f"   Encontradas: {len(encontrados_hist)} | Pendientes: {len(pendientes)}")

    # ----------------------------------------------------------
    # 5. Cruce exacto: Kaggle (cancion + artista)
    # ----------------------------------------------------------
    print("\n5. Cruce exacto con Kaggle (cancion + artista)...")
    encontrados_kaggle_exacto = pd.DataFrame()
    if not df_kaggle_all.empty:
        encontrados_kaggle_exacto, pendientes = cruce_exacto(
            pendientes, df_kaggle_all,
            on=["cancion_norm", "artista_norm"],
            cols_salida=cols_salida,
            origen_label="kaggle"
        )
        print(f"   Encontradas: {len(encontrados_kaggle_exacto)} | Pendientes: {len(pendientes)}")

    # ----------------------------------------------------------
    # 6. Cruce exacto: Kaggle solo por cancion (sin artista)
    # ----------------------------------------------------------
    print("\n6. Cruce exacto con Kaggle (solo cancion)...")
    encontrados_kaggle_cancion = pd.DataFrame()
    if not df_kaggle_all.empty and not pendientes.empty:
        df_kaggle_por_cancion = df_kaggle_all.drop_duplicates(subset=["cancion_norm"])
        df_cruce = pendientes.merge(
            df_kaggle_por_cancion[["cancion_norm"] + cols_salida],
            on="cancion_norm",
            how="left"
        )
        encontrados_kaggle_cancion = df_cruce[df_cruce["origen"] == "kaggle"].copy()
        pendientes = df_cruce[df_cruce["origen"].isna()][["cancion", "artista", "cancion_norm", "artista_norm"]].copy()
        print(f"   Encontradas: {len(encontrados_kaggle_cancion)} | Pendientes: {len(pendientes)}")

    # ----------------------------------------------------------
    # 7. Cruce fuzzy: rapidfuzz (cancion + artista)
    # ----------------------------------------------------------
    print("\n7. Cruce fuzzy con rapidfuzz...")
    encontrados_fuzzy = pd.DataFrame()
    if not df_kaggle_all.empty and not pendientes.empty:
        encontrados_fuzzy, pendientes = cruce_fuzzy(pendientes, df_kaggle_all)
        print(f"   Encontradas: {len(encontrados_fuzzy)} | Pendientes: {len(pendientes)}")

    # ----------------------------------------------------------
    # 8. Síntesis para las que no cruzaron
    # ----------------------------------------------------------
    print("\n8. Generando features sintéticas...")
    df_reales = pd.concat([
        df_hist.assign(artista=df_hist["artistName"]),
        df_kaggle_all.assign(artista=df_kaggle_all["artista_norm"]) if not df_kaggle_all.empty else pd.DataFrame()
    ], ignore_index=True)
    df_reales["artista_norm"] = df_reales["artista"].apply(normalizar_texto)
    df_sintetico = generar_features_sinteticas(pendientes, df_reales) if not pendientes.empty else pd.DataFrame()

    # ----------------------------------------------------------
    # 9. Consolidar
    # ----------------------------------------------------------
    print("\n9. Consolidando resultado final...")
    cols_finales = ["cancion", "artista", "album", "origen"] + FEATURE_COLS

    bloques = [encontrados_hist[cols_finales]]
    for df_bloque in [encontrados_kaggle_exacto, encontrados_kaggle_cancion, encontrados_fuzzy, df_sintetico]:
        if not df_bloque.empty:
            bloques.append(df_bloque[cols_finales])

    df_final = pd.concat(bloques, ignore_index=True)
    df_final.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    total = len(df_final)
    def pct(label):
        n = len(df_final[df_final["origen"] == label])
        return f"{n} ({n/total*100:.1f}%)"

    print(f"\n{'='*55}")
    print(f"¡Merge completado! -> {OUTPUT_FILE}")
    print(f"  Total canciones          : {total}")
    print(f"  Histórico personal       : {pct('historico')}")
    print(f"  Kaggle exacto            : {pct('kaggle')}")
    print(f"  Kaggle solo cancion      : {pct('kaggle')}")  
    print(f"  Kaggle fuzzy             : {pct('kaggle_fuzzy')}")
    print(f"  Sintético (artista)      : {pct('sintetico_artista')}")
    print(f"  Sintético (global)       : {pct('sintetico_global')}")
    datos_reales = len(df_final[df_final["origen"].isin(["historico", "kaggle", "kaggle_fuzzy"])])
    print(f"\n  Datos reales             : {datos_reales} ({datos_reales/total*100:.1f}%)")
    print(f"  Datos sintéticos         : {total - datos_reales} ({(total-datos_reales)/total*100:.1f}%)")
    print(f"{'='*55}")


if __name__ == "__main__":
    merge_features()