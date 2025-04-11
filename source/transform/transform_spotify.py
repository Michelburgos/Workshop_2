"""
🎧 Módulo de transformación de datos de Spotify.
"""

import pandas as pd
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def eliminar_columnas_innecesarias(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina columnas irrelevantes como 'Unnamed: 0' si existe."""
    logging.info("Eliminando columnas innecesarias...")
    return df.drop(columns=["Unnamed: 0"], errors='ignore')


def eliminar_nulos(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina filas con valores nulos."""
    logging.info("Eliminando filas con valores nulos...")
    return df.dropna().reset_index(drop=True)


def eliminar_duplicados_exactos(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina duplicados exactos en todo el DataFrame."""
    logging.info("Eliminando duplicados exactos...")
    return df.drop_duplicates()


def eliminar_duplicados_por_contenido(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina duplicados ignorando 'track_id' y 'album_name'."""
    subset_cols = [col for col in df.columns if col not in ["track_id", "album_name"]]
    logging.info("Eliminando duplicados por contenido (ignorando track_id y album_name)...")
    return df.drop_duplicates(subset=subset_cols, keep="first")


def conservar_mas_popular_por_nombre_artista(df: pd.DataFrame) -> pd.DataFrame:
    """Conserva la fila más popular para cada combinación única de track_name y artista."""
    logging.info("Conservando canción más popular por artista y track_name...")
    idx = df.groupby(['track_name', 'artists'])['popularity'].idxmax()
    return df.loc[idx].reset_index(drop=True)


def asignar_categoria_y_consolidar_duplicados(
    df: pd.DataFrame,
    key_columns: list = ['artists', 'track_id']
) -> pd.DataFrame:
    """Asigna una categoría a cada género y consolida duplicados."""
    logging.info("Asignando categorías de género y consolidando duplicados...")

    genre_categories = {
        'rock': 'Rock', 'pop': 'Pop', 'j-pop': 'Pop', 'k-pop': 'Pop',
        'electronic': 'Electronic', 'edm': 'Electronic', 'techno': 'Electronic',
        'classical': 'Classical', 'opera': 'Classical',
        'folk': 'Folk', 'acoustic': 'Folk', 'country': 'Folk',
        'jazz': 'Jazz/Blues', 'blues': 'Jazz/Blues', 'soul': 'Jazz/Blues',
        'latin': 'Latin', 'reggaeton': 'Latin',
        'hip-hop': 'Hip-Hop', 'afrobeat': 'Hip-Hop',
        'metal': 'Metal', 'death-metal': 'Metal',
        'punk': 'Punk', 'ska': 'Punk',
        'reggae': 'Reggae',
        'happy': 'Moods', 'chill': 'Moods', 'sad': 'Moods',
        'french': 'Regional', 'german': 'Regional', 'spanish': 'Regional',
        'anime': 'Other', 'comedy': 'Other', 'disney': 'Other'
    }

    def get_category(genre: str) -> str:
        if not genre or pd.isna(genre):
            return 'Unknown'
        genre = genre.lower()
        for key, category in genre_categories.items():
            if key in genre:
                return category
        return 'Other'

    def pick_genre(group: pd.DataFrame) -> pd.Series:
        if len(group) == 1:
            return group.iloc[0]
        most_common = group['track_genre'].mode()
        if not most_common.empty:
            return group[group['track_genre'] == most_common[0]].iloc[0]
        return group.iloc[0]

    df['track_genre'] = df['track_genre'].apply(get_category)
    return df.groupby(key_columns, as_index=False).apply(pick_genre).reset_index(drop=True)


def categorizar_popularity(df: pd.DataFrame) -> pd.DataFrame:
    """Crea una categoría de popularidad."""
    logging.info("Categorizando la popularidad...")
    def categorize(p):
        return 'low' if p < 30 else 'medium' if p < 70 else 'high'
    df['popularity_cat'] = df['popularity'].apply(categorize)
    return df


def categorizar_duration(df: pd.DataFrame) -> pd.DataFrame:
    """Categorización por duración en minutos."""
    logging.info("Categorizando duración de canciones...")
    df['duration_min'] = df['duration_ms'] / 60000
    def categorize(d):
        return 'short' if d < 2.5 else 'medium' if d <= 4 else 'long'
    df['duration_cat'] = df['duration_min'].apply(categorize)
    return df


def categorizar_dance_energy(df: pd.DataFrame) -> pd.DataFrame:
    """Categorización de 'danceability' y 'energy'."""
    logging.info("Categorizando energía y capacidad para bailar...")
    def categorize(x):
        return 'low' if x < 0.33 else 'medium' if x < 0.66 else 'high'
    df['danceability_cat'] = df['danceability'].apply(categorize)
    df['energy_cat'] = df['energy'].apply(categorize)
    return df


def categorizar_valence(df: pd.DataFrame) -> pd.DataFrame:
    """Categorización de valencia emocional."""
    logging.info("Categorizando valencia emocional...")
    def categorize(v):
        if v < 0.2: return 'very sad'
        elif v < 0.4: return 'sad'
        elif v < 0.6: return 'neutral'
        elif v < 0.8: return 'happy'
        return 'very happy'
    df['valence_cat'] = df['valence'].apply(categorize)
    df = df[~df["track_genre"].str.lower().isin(["other", "moods"])].reset_index(drop=True)
    return df


def crear_columnas_booleanas(df: pd.DataFrame) -> pd.DataFrame:
    """Crea columnas binarias basadas en loudness y liveness."""
    logging.info("Creando columnas booleanas (is_loud, is_live)...")
    df['is_loud'] = df['loudness'] > -5
    df['is_live'] = df['liveness'] > 0.8
    return df


def eliminar_columnas_numericas(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina columnas numéricas utilizadas para categorización."""
    logging.info("Eliminando columnas numéricas originales...")
    columnas = [
        'valence','loudness', 'liveness', 'key', 'mode', 'time_signature', 'tempo',
        'speechiness', 'acousticness', 'instrumentalness', 'album_name'
    ]
    return df.drop(columns=columnas, errors='ignore')


def transform_spotify_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica la transformación completa al dataset de Spotify.

    Args:
        df (pd.DataFrame): DataFrame crudo de Spotify.

    Returns:
        pd.DataFrame: DataFrame transformado y listo para análisis.
    """
    logging.info("🚀 Iniciando transformación de datos de Spotify...")
    df = eliminar_columnas_innecesarias(df)
    df = eliminar_nulos(df)
    df = eliminar_duplicados_exactos(df)
    df = asignar_categoria_y_consolidar_duplicados(df)
    df = eliminar_duplicados_por_contenido(df)
    df = conservar_mas_popular_por_nombre_artista(df)
    df = categorizar_popularity(df)
    df = categorizar_duration(df)
    df = categorizar_dance_energy(df)
    df = categorizar_valence(df)
    df = crear_columnas_booleanas(df)
    df = eliminar_columnas_numericas(df)
    logging.info("✅ Transformación completada.")
    return df
