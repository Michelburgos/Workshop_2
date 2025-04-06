import pandas as pd


def eliminar_columnas_innecesarias(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(columns=["Unnamed: 0"], errors='ignore')

def eliminar_nulos(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna().reset_index(drop=True)

def eliminar_duplicados_exactos(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates()

def asignar_categoria_y_consolidar_duplicados(df: pd.DataFrame, key_columns: list = ['artists', 'track_id']) -> pd.DataFrame:
    """
    Asigna una categoría general a cada género musical y consolida duplicados manteniendo
    la fila con el género más frecuente por combinación de artistas y track_id.

    Args:
        df (pd.DataFrame): DataFrame original que contiene al menos la columna 'track_genre'.
        key_columns (list): Columnas clave para identificar duplicados (default: ['artists', 'track_id']).

    Returns:
        pd.DataFrame: DataFrame procesado con categoría asignada y duplicados consolidados.
    """
    # Diccionario de mapeo de géneros
    genre_categories = {
        'rock': 'Rock', 'rockabilly': 'Rock', 'alt-rock': 'Rock', 'alternative': 'Rock', 'emo': 'Rock', 
        'goth': 'Rock', 'grunge': 'Rock', 'hard-rock': 'Rock', 'psych-rock': 'Rock', 'punk-rock': 'Rock', 
        'rock-n-roll': 'Rock',
        'pop': 'Pop', 'power-pop': 'Pop', 'j-pop': 'Pop', 'k-pop': 'Pop', 'synth-pop': 'Pop', 
        'indie-pop': 'Pop', 'cantopop': 'Pop', 'mandopop': 'Pop',
        'electronic': 'Electronic', 'edm': 'Electronic', 'techno': 'Electronic', 'house': 'Electronic', 
        'trance': 'Electronic', 'idm': 'Electronic', 'hardstyle': 'Electronic', 'progressive-house': 'Electronic', 
        'minimal-techno': 'Electronic', 'electro': 'Electronic', 'breakbeat': 'Electronic', 'drum-and-bass': 'Electronic', 
        'dubstep': 'Electronic', 'chicago-house': 'Electronic', 'deep-house': 'Electronic', 'detroit-techno': 'Electronic', 
        'industrial': 'Electronic', 'trip-hop': 'Electronic',
        'classical': 'Classical', 'opera': 'Classical', 'new-age': 'Classical', 'piano': 'Classical', 
        'ambient': 'Classical',
        'folk': 'Folk', 'acoustic': 'Folk', 'bluegrass': 'Folk', 'country': 'Folk', 'honky-tonk': 'Folk', 
        'guitar': 'Folk', 'singer-songwriter': 'Folk', 'songwriter': 'Folk',
        'jazz': 'Jazz/Blues', 'blues': 'Jazz/Blues', 'soul': 'Jazz/Blues', 'funk': 'Jazz/Blues', 'groove': 'Jazz/Blues', 
        'r-n-b': 'Jazz/Blues',
        'latin': 'Latin', 'latino': 'Latin', 'salsa': 'Latin', 'samba': 'Latin', 'sertanejo': 'Latin', 
        'pagode': 'Latin', 'tango': 'Latin', 'forro': 'Latin', 'mpb': 'Latin', 'reggaeton': 'Latin',
        'hip-hop': 'Hip-Hop', 'afrobeat': 'Hip-Hop', 'dancehall': 'Hip-Hop',
        'metal': 'Metal', 'heavy-metal': 'Metal', 'death-metal': 'Metal', 'black-metal': 'Metal', 
        'metalcore': 'Metal', 'grindcore': 'Metal',
        'punk': 'Punk', 'ska': 'Punk', 'hardcore': 'Punk',
        'reggae': 'Reggae', 'dub': 'Reggae',
        'happy': 'Moods', 'sleep': 'Moods', 'chill': 'Moods', 'sad': 'Moods', 'study': 'Moods', 
        'romance': 'Moods', 'party': 'Moods',
        'french': 'Regional', 'german': 'Regional', 'british': 'Regional', 'turkish': 'Regional', 
        'iranian': 'Regional', 'spanish': 'Regional', 'swedish': 'Regional', 'indian': 'Regional', 
        'malay': 'Regional', 'brazil': 'Regional',
        'kids': 'Other', 'anime': 'Other', 'comedy': 'Other', 'disney': 'Other', 'show-tunes': 'Other', 
        'club': 'Other', 'gospel': 'Other', 'world-music': 'Other', 'children': 'Other', 'pop-film': 'Other', 
        'j-idol': 'Pop', 'j-dance': 'Electronic'
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

    
        # Asignar categoría general al género
    df['track_genre'] = df['track_genre'].apply(get_category)

        # Consolidar duplicados por key_columns
    df = df.groupby(key_columns, as_index=False).apply(pick_genre).reset_index(drop=True)

    return df



def eliminar_duplicados_por_contenido(df: pd.DataFrame) -> pd.DataFrame:
    subset_cols = [col for col in df.columns if col not in ["track_id", "album_name"]]
    return df.drop_duplicates(subset=subset_cols, keep="first")

def conservar_mas_popular_por_nombre_artista(df: pd.DataFrame) -> pd.DataFrame:
    idx = df.groupby(['track_name', 'artists'])['popularity'].idxmax()
    return df.loc[idx].reset_index(drop=True)

def categorizar_popularity(df: pd.DataFrame) -> pd.DataFrame:
    def categorize(p):
        if p < 30: return 'low'
        elif p < 70: return 'medium'
        else: return 'high'
    df['popularity_cat'] = df['popularity'].apply(categorize)
    return df

def categorizar_duration(df: pd.DataFrame) -> pd.DataFrame:
    df['duration_min'] = df['duration_ms'] / 60000
    def categorize(d):
        if d < 2.5: return 'short'
        elif d <= 4: return 'medium'
        else: return 'long'
    df['duration_cat'] = df['duration_min'].apply(categorize)
    return df

def categorizar_dance_energy(df: pd.DataFrame) -> pd.DataFrame:
    def categorize(x):
        if x < 0.33: return 'low'
        elif x < 0.66: return 'medium'
        else: return 'high'
    df['danceability_cat'] = df['danceability'].apply(categorize)
    df['energy_cat'] = df['energy'].apply(categorize)
    return df

def categorizar_valence(df: pd.DataFrame) -> pd.DataFrame:
    def categorize(v):
        if v < 0.2: return 'very sad'
        elif v < 0.4: return 'sad'
        elif v < 0.6: return 'neutral'
        elif v < 0.8: return 'happy'
        else: return 'very happy'
    df['valence_cat'] = df['valence'].apply(categorize)
    return df

def crear_columnas_booleanas(df: pd.DataFrame) -> pd.DataFrame:
    df['is_loud'] = df['loudness'] > -5
    df['is_live'] = df['liveness'] > 0.8
    return df

def eliminar_columnas_numericas(df: pd.DataFrame) -> pd.DataFrame:
    columnas = [
        'popularity', 'duration_ms', 'danceability', 'energy', 'valence',
        'loudness', 'liveness', 'key', 'mode', 'time_signature', 'tempo',
        'speechiness', 'acousticness', 'instrumentalness'
    ]
    return df.drop(columns=columnas, errors='ignore')

def transform_spotify_data() -> pd.DataFrame:

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

    return df

