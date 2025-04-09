import os
import re
import time
import logging
import pandas as pd
import requests
from tqdm import tqdm

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
ARTISTS_CSV = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'artists.csv'))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def extract_api() -> pd.DataFrame:
    df_artists = pd.read_csv(ARTISTS_CSV, header=None, names=["raw"])
    artists = _limpiar_artistas(df_artists)

    resultados = _consultar_wikidata(artists)

    return pd.DataFrame(resultados, columns=[
        "artist", "country", "type", "genre", "award", "birth", "death", "gender"
    ])

def _limpiar_artistas(df: pd.DataFrame) -> list:
    separadores = [";", ",", "&", "Featuring", " feat.", "Feat.", " ft.", "/", " x "]
    artistas = []

    for raw in df["raw"]:
        if pd.isna(raw):
            continue
        for sep in separadores:
            raw = raw.replace(sep, "|")
        partes = [re.sub(r'[^\w\s\-]', '', p.strip()) for p in raw.split("|") if p.strip()]
        artistas.extend(partes)

    return sorted(set(artistas))

def _construir_query(artistas: list) -> str:
    values = "\n".join([f'"{nombre}"@en' for nombre in artistas])
    return f"""
    SELECT ?artistLabel ?birth ?death ?countryLabel ?typeLabel ?genreLabel ?awardLabel ?genderLabel WHERE {{
      VALUES ?name {{ {values} }}
      ?artist rdfs:label ?name.
      ?artist wdt:P166 ?award.
      ?award rdfs:label ?awardLabel.
      OPTIONAL {{ ?artist wdt:P27 ?country. }}
      OPTIONAL {{ ?artist wdt:P31 ?type. }}
      OPTIONAL {{ ?artist wdt:P136 ?genre. }}
      OPTIONAL {{ ?artist wdt:P569 ?birth. }}
      OPTIONAL {{ ?artist wdt:P570 ?death. }}
      OPTIONAL {{ ?artist wdt:P21 ?gender. }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    }}
    """

def _consultar_wikidata(artistas: list) -> list:
    resultados = []
    i = 0
    with tqdm(total=len(artistas), desc="🔍 SPARQL") as pbar:
        while i < len(artistas):
            batch = artistas[i:i+60]
            query = _construir_query(batch)
            try:
                res = requests.post(WIKIDATA_ENDPOINT, data={"query": query}, headers={"Accept": "application/sparql-results+json"})
                res.raise_for_status()
                data = res.json()
                for row in data["results"]["bindings"]:
                    resultados.append({
                        "artist": row.get("artistLabel", {}).get("value", ""),
                        "birth": row.get("birth", {}).get("value", ""),
                        "death": row.get("death", {}).get("value", ""),
                        "country": row.get("countryLabel", {}).get("value", ""),
                        "type": row.get("typeLabel", {}).get("value", ""),
                        "genre": row.get("genreLabel", {}).get("value", ""),
                        "award": row.get("awardLabel", {}).get("value", ""),
                        "gender": row.get("genderLabel", {}).get("value", "")
                    })
                i += len(batch)
                pbar.update(len(batch))
                time.sleep(1.1)
            except Exception as e:
                logging.error(f"❌ Error SPARQL: {e}")
                i += 1
                pbar.update(1)
    return resultados
