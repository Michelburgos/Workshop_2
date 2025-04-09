# merge_datasets.py

import pandas as pd
import logging
import re

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def expand_artists_column(df: pd.DataFrame, column: str = "artist") -> pd.DataFrame:
    """
    Expande filas con múltiples artistas en la columna especificada, separando por símbolos comunes.

    Args:
        df (pd.DataFrame): DataFrame con posibles colaboraciones.
        column (str): Columna con nombres de artistas a expandir.

    Returns:
        pd.DataFrame: DataFrame expandido, con un artista por fila.
    """
    logging.info(f"🔄 Expandiendo artistas en columna '{column}'...")

    # Separadores típicos de colaboraciones
    separators = r';|,|&| Featuring | feat\.| Feat\.| ft\.|/| x '
    df = df.copy()
    df[column] = df[column].astype(str)

    df_expanded = df.assign(**{
        column: df[column].str.split(separators)
    }).explode(column)

    df_expanded[column] = df_expanded[column].str.strip().str.lower()
    return df_expanded


def merge_datasets(df_spotify: pd.DataFrame, df_grammy: pd.DataFrame, df_wikidata: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza el merge de los datasets de Spotify, Grammy y Wikidata considerando colaboraciones.

    Args:
        df_spotify (pd.DataFrame): Dataset transformado de Spotify.
        df_grammy (pd.DataFrame): Dataset transformado de premios Grammy.
        df_wikidata (pd.DataFrame): Dataset transformado de Wikidata.

    Returns:
        pd.DataFrame: Dataset unificado.
    """

    logging.info("🚀 Iniciando merge entre Spotify, Grammy y Wikidata...")

    # 🔹 Expandir artistas
    df_spotify_exp = expand_artists_column(df_spotify, "artists").rename(columns={"artists": "artist"})
    df_grammy_exp = expand_artists_column(df_grammy, "artist")

    # 🔹 Normalizar nombres para merge
    df_wikidata['artist'] = df_wikidata['artist'].str.strip().str.lower()

    # 🔹 Merge Spotify + Grammy
    logging.info("🔗 Merge Spotify + Grammy...")
    merged_spotify_grammy = pd.merge(df_spotify_exp, df_grammy_exp, on='artist', how='left', suffixes=('', '_grammy'))

    # 🔹 Merge con Wikidata
    logging.info("🔗 Merge con Wikidata...")
    final_merged = pd.merge(merged_spotify_grammy, df_wikidata, on='artist', how='left', suffixes=('', '_wikidata'))

    # 🔹 Final
    final_merged = final_merged.drop_duplicates(subset=['track_id', 'artist'], keep='first').reset_index(drop=True)
    logging.info(f"✅ Merge completo: {len(final_merged)} filas")

    return final_merged
