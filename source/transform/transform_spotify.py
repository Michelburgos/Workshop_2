import pandas as pd


def eliminar_columnas_innecesarias(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(columns=["Unnamed: 0"], errors='ignore')

def eliminar_nulos(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna().reset_index(drop=True)

def eliminar_duplicados_exactos(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates()

def consolidar_por_track_id(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby('track_id').agg({
        'track_name': 'first',
        'artists': 'first',
        'album_name': 'first',
        'popularity': 'mean',
        'duration_ms': 'first',
        'explicit': 'first',
        'danceability': 'first',
        'energy': 'first',
        'key': 'first',
        'loudness': 'first',
        'mode': 'first',
        'speechiness': 'first',
        'acousticness': 'first',
        'instrumentalness': 'first',
        'liveness': 'first',
        'valence': 'first',
        'tempo': 'first',
        'time_signature': 'first',
        'track_genre': lambda x: ', '.join(sorted(set(x)))
    }).reset_index()

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
    df = consolidar_por_track_id(df)
    df = eliminar_duplicados_por_contenido(df)
    df = conservar_mas_popular_por_nombre_artista(df)
    df = categorizar_popularity(df)
    df = categorizar_duration(df)
    df = categorizar_dance_energy(df)
    df = categorizar_valence(df)
    df = crear_columnas_booleanas(df)
    df = eliminar_columnas_numericas(df)

    return df

