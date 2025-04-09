import re
import time
import logging
import requests
import pandas as pd
from tqdm import tqdm

# Configuración de logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Constantes de configuración
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
MAX_QUERY_SIZE = 60000
BATCH_INITIAL_SIZE = 60


def cargar_y_limpiar_csv(path_csv: str) -> list:
    """
    Carga el archivo CSV con nombres de artistas y los limpia para consulta SPARQL.

    Args:
        path_csv (str): Ruta del archivo CSV sin cabecera.

    Returns:
        list: Lista de nombres únicos de artistas limpios.
    """
    logging.info("🔄 Cargando y limpiando nombres de artistas desde el CSV...")
    df = pd.read_csv(path_csv, header=None, names=["raw"])

    def separar_y_limpiar(nombre: str) -> list:
        if pd.isna(nombre):
            return []
        separadores = [";", ",", "&", "Featuring", " feat.", "Feat.", " ft.", "/", " x "]
        for sep in separadores:
            nombre = nombre.replace(sep, "|")
        partes = nombre.split("|")
        return [
            re.sub(r'[^\w\s\-]', '', p.strip().strip('"').strip("'"))
            for p in partes if p.strip()
        ]

    todos = []
    for nombre in df["raw"]:
        todos.extend(separar_y_limpiar(nombre))

    artistas_unicos = sorted(set(filter(None, todos)))
    logging.info(f"✅ Total artistas únicos: {len(artistas_unicos)}")
    return artistas_unicos


def construir_query_sparql(artistas: list) -> str:
    """
    Construye una consulta SPARQL con los artistas proporcionados.

    Args:
        artistas (list): Lista de nombres de artistas.

    Returns:
        str: Consulta SPARQL.
    """
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


def obtener_datos_wikidata(artistas_batch: list) -> dict:
    """
    Ejecuta una consulta SPARQL en Wikidata para un batch de artistas.

    Args:
        artistas_batch (list): Lista de nombres de artistas.

    Returns:
        dict: JSON con resultados SPARQL o None si ocurre error.
    """
    query = construir_query_sparql(artistas_batch)
    headers = {"Accept": "application/sparql-results+json"}
    try:
        response = requests.post(WIKIDATA_ENDPOINT, data={"query": query}, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error en la consulta SPARQL: {e}")
        return None


def extraer_datos(artistas_unicos: list) -> list:
    """
    Realiza extracción por lotes desde Wikidata.

    Args:
        artistas_unicos (list): Lista de artistas limpios.

    Returns:
        list: Lista de diccionarios con la información de cada artista.
    """
    logging.info("🚀 Iniciando consultas a Wikidata...")
    resultados = []
    i = 0

    with tqdm(total=len(artistas_unicos), desc="🔎 Batches SPARQL") as pbar:
        while i < len(artistas_unicos):
            batch_size = BATCH_INITIAL_SIZE
            batch_success = False

            while batch_size > 0 and not batch_success:
                batch = artistas_unicos[i:i + batch_size]
                query = construir_query_sparql(batch)

                if len(query.encode("utf-8")) > MAX_QUERY_SIZE:
                    batch_size -= 5
                    continue

                data = obtener_datos_wikidata(batch)
                if data:
                    for row in data["results"]["bindings"]:
                        resultados.append({
                            "artist": row.get("artistLabel", {}).get("value", ""),
                            "birth": row.get("birth", {}).get("value", ""),
                            "death": row.get("death", {}).get("value", ""),
                            "country": row.get("countryLabel", {}).get("value", ""),
                            "type": row.get("typeLabel", {}).get("value", ""),
                            "genre": row.get("genreLabel", {}).get("value", ""),
                            "award": row.get("awardLabel", {}).get("value", "No awards"),
                            "gender": row.get("genderLabel", {}).get("value", "Unknown")
                        })
                    batch_success = True
                    i += batch_size
                    pbar.update(batch_size)
                    time.sleep(1.1)
                else:
                    batch_size = batch_size // 2

            if not batch_success:
                logging.warning(f"⚠️ Saltando artista en índice {i}: {artistas_unicos[i]}")
                i += 1
                pbar.update(1)

    return resultados


def guardar_resultados(resultados: list, output_path: str) -> None:
    """
    Guarda los resultados extraídos en un archivo CSV.

    Args:
        resultados (list): Lista de diccionarios con datos de artistas.
        output_path (str): Ruta donde guardar el archivo CSV.
    """
    columnas = ["artist", "country", "type", "genre", "award", "birth", "death", "gender"]
    df = pd.DataFrame(resultados, columns=columnas)
    df.to_csv(output_path, index=False)
    logging.info(f"✅ Resultados guardados en {output_path}")





