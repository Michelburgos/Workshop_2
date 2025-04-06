import pandas as pd
from transform_spotify import transform_spotify_data
from transform_grammys import transform_grammy_data
from transform_api import transform_api_data
import logging

def merge_datasets():
    """
    Une los datasets transformados de Spotify, Grammy y Artistas API.

    Returns:
        pd.DataFrame: DataFrame combinado y listo para análisis.
    """
    try:
        # Extraer y transformar cada dataset
        df_spotify = transform_spotify_data()   # Devuelve un DataFrame limpio de Spotify
        df_grammy = transform_grammy_data()     # Devuelve un DataFrame limpio de Grammy
        df_artists = transform_api_data()   # Devuelve un DataFrame limpio de la API

        # === MERGE entre Grammy y Artistas API por el campo 'artist' ===
        df_merged = pd.merge(df_grammy, df_artists, how='left', on='artist')

        # === MERGE con Spotify ===
        # Asumimos que hay campos comunes como 'track_name' y 'artists'

        df_final = pd.merge(df_merged, df_spotify, how='left', left_on='artists', right_on='artist')
        # Opcional: eliminar columnas redundantes
        df_final = df_final.drop(columns=['track_name', 'artists'], errors='ignore')

        logging.info(f"✅ Merge completado. Shape final: {df_final.shape}")
        return df_final

    except Exception as e:
        logging.error(f"❌ Error al hacer el merge de datasets: {e}")
        raise

