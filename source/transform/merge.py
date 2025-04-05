import pandas as pd

SPOTIFY_PATH = '/opt/airflow/intermediate/spotify_clean.csv'
GRAMMY_PATH = '/opt/airflow/intermediate/grammy_clean.csv'
API_PATH = '/opt/airflow/intermediate/musicbrainz_clean.csv'
FINAL_PATH = '/opt/airflow/output/final_dataset.csv'

def transform_merge():
    print("🔀 Haciendo merge entre las 3 fuentes...")

    spotify = pd.read_csv(SPOTIFY_PATH)
    grammy = pd.read_csv(GRAMMY_PATH)
    api = pd.read_csv(API_PATH)

    # Merge 1: Spotify + Grammy (por track y artista)
    merged1 = pd.merge(
        spotify,
        grammy,
        left_on=['track_name', 'artists'],
        right_on=['title', 'artist'],
        how='left'
    )

    # Merge 2: Agregar info externa de MusicBrainz (puede tener fecha lanzamiento, tags, etc.)
    merged_final = pd.merge(
        merged1,
        api,
        on=['track_name', 'artists'],
        how='left'
    )

    merged_final.to_csv(FINAL_PATH, index=False)
    print("✅ Dataset final generado y guardado.")
