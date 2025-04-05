import pandas as pd
import requests
import time
import os

GRAMMY_FILE = '/opt/airflow/output/grammy_cleaned.csv'
OUTPUT_FILE = '/opt/airflow/output/musicbrainz_artists.csv'

BASE_URL = "https://musicbrainz.org/ws/2/artist/"

HEADERS = {
    'User-Agent': 'ETLProject/1.0 (your_email@example.com)'
}

def query_musicbrainz(artist_name):
    params = {
        'query': f'artist:{artist_name}',
        'fmt': 'json'
    }
    response = requests.get(BASE_URL, params=params, headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        if data['artists']:
            artist = data['artists'][0]
            return {
                'artist': artist_name,
                'country': artist.get('country', 'Unknown'),
                'type': artist.get('type', 'Unknown'),
                'begin_date': artist.get('life-span', {}).get('begin', 'Unknown')
            }
    return {
        'artist': artist_name,
        'country': 'Unknown',
        'type': 'Unknown',
        'begin_date': 'Unknown'
    }

def extract_musicbrainz_data():
    print("🔍 Consultando artistas en MusicBrainz...")
    df = pd.read_csv(GRAMMY_FILE)

    unique_artists = df['artist'].dropna().unique()
    enriched_data = []

    for i, artist in enumerate(unique_artists):
        print(f"{i+1}/{len(unique_artists)} → {artist}")
        info = query_musicbrainz(artist)
        enriched_data.append(info)
        time.sleep(1.1)  # Para evitar bloqueo por rate limit

    enriched_df = pd.DataFrame(enriched_data)
    enriched_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Datos de MusicBrainz guardados en {OUTPUT_FILE}")
