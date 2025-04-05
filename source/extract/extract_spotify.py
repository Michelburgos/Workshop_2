import pandas as pd
import os

# Guardamos temporalmente el DataFrame como CSV para pasarlo entre tareas
TEMP_FILE = '/opt/airflow/output/spotify_cleaned.csv'
INPUT_FILE = '/opt/airflow/data/spotify.csv'

def extract_spotify_data():
    print("🔍 Leyendo archivo de Spotify...")
    df = pd.read_csv(INPUT_FILE)

    # Campos relevantes
    columns_to_keep = [
        'track_id', 'artists', 'album_name', 'track_name', 'popularity',
        'duration_ms', 'explicit', 'danceability', 'energy', 'key',
        'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness',
        'liveness', 'valence', 'tempo', 'time_signature', 'track_genre'
    ]
    df = df[columns_to_keep]

    # Limpieza básica
    df.dropna(subset=['track_name', 'artists'], inplace=True)
    df['track_name'] = df['track_name'].str.lower().str.strip()
    df['artists'] = df['artists'].str.lower().str.strip()

    # Guardar como CSV para pasos siguientes
    os.makedirs('/opt/airflow/output', exist_ok=True)
    df.to_csv(TEMP_FILE, index=False)

    print(f"✅ Datos de Spotify procesados y guardados en {TEMP_FILE}")
