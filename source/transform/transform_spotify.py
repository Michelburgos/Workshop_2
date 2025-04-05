import pandas as pd

INPUT_PATH = '/opt/airflow/data/spotify.csv'
OUTPUT_PATH = '/opt/airflow/intermediate/spotify_clean.csv'

def transform_spotify_data():
    print("🔄 Transformando datos de Spotify...")

    df = pd.read_csv(INPUT_PATH)

    # Limpiar campos básicos
    df = df.dropna(subset=['track_name', 'artists'])
    df['track_name'] = df['track_name'].str.lower().str.strip()
    df['artists'] = df['artists'].str.lower().str.strip()

    # Opcional: convertir duración de ms a minutos
    df['duration_min'] = df['duration_ms'] / 60000

    # Guardar limpio
    df.to_csv(OUTPUT_PATH, index=False)
    print("✅ Datos de Spotify transformados y guardados.")
