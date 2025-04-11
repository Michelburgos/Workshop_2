import os
import re
import time
import logging
import pandas as pd
import requests
from tqdm import tqdm


WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
HEADERS = {
    "Accept": "application/sparql-results+json",
    "User-Agent": "ETL-MusicalProject/1.0 (correo_de_ejemplo@gmail.com)"  
}
ARTISTS_CSV = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'artists.csv'))
MAX_QUERY_SIZE = 60000

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def extract_api() -> pd.DataFrame:
    """Extrae datos de artistas desde un archivo CSV y Wikidata, retornando un DataFrame.

    Returns:
        pd.DataFrame: DataFrame con columnas ["artist", "country", "award", "death", "gender"]
        conteniendo información de artistas obtenida de Wikidata.
    """
    artistas_unicos = _cargar_y_limpiar_artistas(ARTISTS_CSV)
    resultados = _consultar_wikidata(artistas_unicos)
    columnas_ordenadas = ["artist", "country", "award", "death", "gender"]
    return pd.DataFrame(resultados, columns=columnas_ordenadas)


def limpiar_nombre(nombre: str) -> str:
    """Limpia un nombre de artista eliminando caracteres no deseados y normalizando el formato.

    Args:
        nombre (str): Nombre del artista a limpiar.

    Returns:
        str: Nombre limpio o None si el nombre es inválido o está vacío.
    """
    if pd.isna(nombre) or not nombre.strip():
        return None
    nombre = nombre.replace("\\", "")
    nombre = nombre.replace('"', '')
    nombre = nombre.replace("'", "")
    nombre = nombre.replace("/", " ")
    nombre = nombre.replace("&", "and")
    return nombre.strip()

def _cargar_y_limpiar_artistas(ruta_csv: str) -> list:
    """Carga un archivo CSV con nombres de artistas y devuelve una lista de nombres únicos limpios.

    Args:
        ruta_csv (str): Ruta al archivo CSV que contiene los nombres de artistas.

    Returns:
        list: Lista ordenada de nombres de artistas únicos y limpios.
    """
    df = pd.read_csv(ruta_csv, header=None, names=["raw"])
    nombres_limpios = [limpiar_nombre(nombre) for nombre in df["raw"]]
    artistas_unicos = sorted(set([nombre for nombre in nombres_limpios if nombre]))
    logging.info(f"Total artistas únicos: {len(artistas_unicos)}")
    return artistas_unicos


def construir_query_sparql(artistas: list) -> str:
    """Construye una consulta SPARQL para obtener datos de artistas desde Wikidata.

    Args:
        artistas (list): Lista de nombres de artistas para incluir en la consulta.

    Returns:
        str: Consulta SPARQL como cadena de texto.
    """
    values = "\n".join([f'"{nombre}"@en' for nombre in artistas])
    return f"""
    SELECT ?artistLabel ?death ?countryLabel ?awardLabel ?genderLabel WHERE {{
      VALUES ?name {{ {values} }}
      ?artist rdfs:label ?name.
      ?artist wdt:P166 ?award.
      ?award rdfs:label ?awardLabel.
      OPTIONAL {{ ?artist wdt:P27 ?country. }}
      OPTIONAL {{ ?artist wdt:P570 ?death. }}
      OPTIONAL {{ ?artist wdt:P21 ?gender. }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    }}
    """

def _obtener_datos_wikidata(artistas_batch):
    """Realiza una consulta SPARQL a Wikidata para un lote de artistas.

    Args:
        artistas_batch (list): Lista de nombres de artistas para consultar.

    Returns:
        dict or None: Respuesta JSON de Wikidata si la consulta es exitosa, None si falla.
    """
    query = construir_query_sparql(artistas_batch)
    try:
        response = requests.post(WIKIDATA_ENDPOINT, data={"query": query}, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error en SPARQL: {e}")
        return None


def _consultar_wikidata(artistas_unicos: list) -> list:
    """Consulta Wikidata para obtener datos de artistas en lotes, manejando errores y límites.

    Args:
        artistas_unicos (list): Lista de nombres de artistas únicos para consultar.

    Returns:
        list: Lista de diccionarios con datos de artistas (artista, país, premios, etc.).
    """
    resultados = []
    i = 0

    logging.info("Consultando Wikidata...")
    with tqdm(total=len(artistas_unicos), desc="Batches SPARQL") as pbar:
        while i < len(artistas_unicos):
            batch_size = 80
            batch_success = False

            while batch_size > 0 and not batch_success:
                batch = artistas_unicos[i:i+batch_size]
                query = construir_query_sparql(batch)

                if len(query.encode("utf-8")) > MAX_QUERY_SIZE:
                    batch_size -= 5
                    continue

                data = _obtener_datos_wikidata(batch)
                if data:
                    for row in data["results"]["bindings"]:
                        resultados.append({
                            "artist": row.get("artistLabel", {}).get("value", ""),
                            "country": row.get("countryLabel", {}).get("value", ""),
                            "award": row.get("awardLabel", {}).get("value", "No awards"),
                            "death": row.get("death", {}).get("value", ""),
                            "gender": row.get("genderLabel", {}).get("value", "Unknown")
                        })
                    batch_success = True
                    i += batch_size
                    pbar.update(batch_size)
                    time.sleep(0.8)
                else:
                    batch_size = batch_size // 2

            if not batch_success:
                logging.warning(f"Saltando artista en índice {i}: {artistas_unicos[i]}")
                i += 1
                pbar.update(1)

    return resultados