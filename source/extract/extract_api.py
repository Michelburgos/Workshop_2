import pandas as pd
import requests
import time
import os

def query_musicbrainz(artist_name):
    """
    Consulta la API de MusicBrainz y devuelve información relevante del artista.
    """
    base_url = "https://musicbrainz.org/ws/2/artist/"
    params = {
        'query': f'artist:{artist_name}',
        'fmt': 'json',
    }
    headers = {
        "User-Agent": "ETL-Workshop/1.0 (your_email@example.com)"
    }

    try:
        response = requests.get(base_url, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if data['artists']:
                a = data['artists'][0]
                return {
                    'artist_name': artist_name,
                    'artist_id': a.get('id'),
                    'country': a.get('country'),
                    'begin_date': a.get('life-span', {}).get('begin'),
                    'end_date': a.get('life-span', {}).get('end'),
                    'type': a.get('type'),
                    'gender': a.get('gender'),
                    'disambiguation': a.get('disambiguation')
                }
    except Exception as e:
        print(f"⚠️ Error al consultar '{artist_name}': {e}")

    return {
        'artist_name': artist_name,
        'artist_id': None,
        'country': None,
        'begin_date': None,
        'end_date': None,
        'type': None,
        'gender': None,
        'disambiguation': None
    }

def extract_musicbrainz():
    """
    Carga artistas desde Workshop_2/data/artists.csv y consulta la API de MusicBrainz.
    """
    base_path = os.path.dirname(os.path.abspath(__file__))
    artist_path = os.path.join(base_path, '../../data/artists.csv')
    artist_df = pd.read_csv(artist_path)

    unique_artists = artist_df['artists'].dropna().unique()
    records = []

    for i, artist in enumerate(unique_artists):
        print(f"🔍 ({i+1}/{len(unique_artists)}) Consultando: {artist}")
        records.append(query_musicbrainz(artist))
        time.sleep(1.2)  # Evita bloqueo por límite de velocidad de la API

    df_api = pd.DataFrame(records)

    output_path = os.path.join(base_path, '../../data/api_data.csv')
    df_api.to_csv(output_path, index=False)
    print(f"✅ Archivo api_data.csv creado con {len(df_api)} registros.")
    return df_api





