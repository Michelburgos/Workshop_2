import pandas as pd
import logging
import re
from rapidfuzz import process, fuzz

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def expand_artists_column(df: pd.DataFrame, column: str = "artist") -> pd.DataFrame:
    """Expande filas con múltiples artistas en la columna especificada, separando por símbolos comunes.

    Args:
        df (pd.DataFrame): DataFrame de entrada con una columna que contiene nombres de artistas.
        column (str, optional): Nombre de la columna a expandir. Por defecto, "artist".

    Returns:
        pd.DataFrame: DataFrame con la columna expandida, donde cada artista ocupa una fila y está en minúsculas.
    """
    logging.info(f"Expandiendo artistas en columna '{column}'...")

    separators = r';|,|&| Featuring | feat\.| Feat\.| ft\.|/| x '
    df = df.copy()
    df[column] = df[column].astype(str)

    df_expanded = df.assign(**{
        column: df[column].str.split(separators)
    }).explode(column)

    df_expanded[column] = df_expanded[column].str.strip().str.lower()
    return df_expanded

def merge_datasets(df_spotify: pd.DataFrame, df_grammy: pd.DataFrame, df_wikidata: pd.DataFrame) -> pd.DataFrame:
    """Realiza el merge de los datasets de Spotify, Grammy y Wikidata considerando colaboraciones.

    Args:
        df_spotify (pd.DataFrame): DataFrame con datos de Spotify.
        df_grammy (pd.DataFrame): DataFrame con datos de Grammy.
        df_wikidata (pd.DataFrame): DataFrame con datos de Wikidata.

    Returns:
        pd.DataFrame: DataFrame combinado con información de los tres datasets, sin duplicados por track_id y artista.
    """
    logging.info("Iniciando merge entre Spotify, Grammy y Wikidata...")

    df_spotify_exp = expand_artists_column(df_spotify, "artists").rename(columns={"artists": "artist"})
    df_grammy_exp = expand_artists_column(df_grammy, "artist")

    df_wikidata['artist'] = df_wikidata['artist'].str.strip().str.lower()

    logging.info("Merge Spotify + Grammy...")
    # Usar rapidfuzz para hacer fuzzy matching y conservar solo matches válidos
    merged_spotify_grammy = df_spotify_exp.copy()
    merged_spotify_grammy['matched_artist'] = merged_spotify_grammy['artist'].apply(
        lambda x: process.extractOne(x, df_grammy_exp['artist'], scorer=fuzz.WRatio, score_cutoff=85)
    )
    # Filtrar filas sin match
    merged_spotify_grammy = merged_spotify_grammy[merged_spotify_grammy['matched_artist'].notnull()]
    merged_spotify_grammy['matched_artist_name'] = merged_spotify_grammy['matched_artist'].apply(
        lambda x: x[0]
    )
    merged_spotify_grammy = pd.merge(
        merged_spotify_grammy,
        df_grammy_exp,
        left_on='matched_artist_name',
        right_on='artist',
        how='inner',  # Usar inner para conservar solo matches
        suffixes=('', '_grammy')
    )
    # Mantener solo la columna 'artist' original de Spotify
    merged_spotify_grammy = merged_spotify_grammy.drop(columns=['matched_artist', 'matched_artist_name', 'artist_grammy'])

    logging.info("Merge con Wikidata...")
    # Usar rapidfuzz para el merge con Wikidata y conservar solo matches válidos
    final_merged = merged_spotify_grammy.copy()
    final_merged['matched_artist'] = final_merged['artist'].apply(
        lambda x: process.extractOne(x, df_wikidata['artist'], scorer=fuzz.WRatio, score_cutoff=85)
    )
    # Filtrar filas sin match
    final_merged = final_merged[final_merged['matched_artist'].notnull()]
    final_merged['matched_artist_name'] = final_merged['matched_artist'].apply(
        lambda x: x[0]
    )
    final_merged = pd.merge(
        final_merged,
        df_wikidata,
        left_on='matched_artist_name',
        right_on='artist',
        how='inner',  # Usar inner para conservar solo matches
        suffixes=('', '_wikidata')
    )
    # Mantener solo la columna 'artist' original
    final_merged = final_merged.drop(columns=['matched_artist', 'matched_artist_name', 'artist_wikidata'])

    if "won_grammy" in final_merged.columns:
        final_merged["won_grammy"] = final_merged["won_grammy"].fillna("No")

    final_merged = final_merged.drop_duplicates(subset=['track_id', 'artist'], keep='first').reset_index(drop=True)

    logging.info(f"Merge completo: {len(final_merged)} filas")
    return final_merged



