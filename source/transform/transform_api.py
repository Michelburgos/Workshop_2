import pandas as pd

INPUT_PATH = '/opt/airflow/intermediate/musicbrainz_raw.csv'
OUTPUT_PATH = '/opt/airflow/intermediate/musicbrainz_clean.csv'

def transform_api_data():
    print("🔄 Transformando datos de MusicBrainz...")

    df = pd.read_csv(INPUT_PATH)

    # Normalización de nombre
    df['track_name'] = df['track_name'].str.lower().str.strip()
    df['artist'] = df['artist'].str.lower().str.strip()

    # Eliminar duplicados
    df = df.drop_duplicates(subset=['track_name', 'artist'])

    df.to_csv(OUTPUT_PATH, index=False)
    print("✅ Datos de MusicBrainz transformados y guardados.")
