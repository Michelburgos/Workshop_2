import os
import logging
import pandas as pd

# Configurar logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

def extract_spotify(path_to_csv="../data/spotify_data.csv"):
    """
    Extrae datos desde un archivo CSV de Spotify.

    Args:
        path_to_csv (str): Ruta del archivo CSV.

    Returns:
        pd.DataFrame: DataFrame con los datos de Spotify.
    
    Raises:
        Exception: Si ocurre un error durante la carga.
    """
    try:
        logging.info(f"Cargando datos de Spotify desde: {path_to_csv}")
        df = pd.read_csv(path_to_csv)
        logging.info(f"✅ {len(df)} registros extraídos de Spotify.")
        return df
    except Exception as e:
        logging.error(f"❌ Error al cargar datos de Spotify: {e}")
        return pd.DataFrame()
