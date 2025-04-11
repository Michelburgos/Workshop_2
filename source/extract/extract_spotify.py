import os
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SPOTIFY_CSV = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'spotify_dataset.csv'))

def extract_spotify() -> pd.DataFrame:
    """Extrae datos de un archivo CSV de Spotify.

    Returns:
        pd.DataFrame: DataFrame con los datos extraídos del archivo CSV de Spotify.
            Retorna un DataFrame vacío si ocurre un error durante la carga.
    """
    logging.info(f"📥 Cargando datos de Spotify desde: {SPOTIFY_CSV}")
    try:
        df = pd.read_csv(SPOTIFY_CSV)
        logging.info(f"{len(df)} registros extraídos de Spotify.")
        return df
    except Exception as e:
        logging.error(f"Error al cargar datos de Spotify: {e}")
        return pd.DataFrame()


