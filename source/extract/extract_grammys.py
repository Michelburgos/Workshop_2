import os
import sys
import logging
import pandas as pd

# Agrega el path al directorio BD_connection
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'BD_connection')))
from BD_connection import get_mysql_connection

# Configurar logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

def extract_grammy(query="SELECT * FROM raw_grammy"):
    """
    Ejecuta una consulta SQL sobre la base de datos de los Grammy.

    Args:
        query (str): Consulta SQL a ejecutar. Por defecto: "SELECT * FROM raw_grammy".

    Returns:
        pd.DataFrame: Resultados de la consulta.
    
    Raises:
        Exception: Si ocurre un error durante la conexión o extracción.
    """
    connection = None
    try:
        logging.info("Estableciendo conexión con la base de datos de Grammy...")
        connection = get_mysql_connection()

        df = pd.read_sql(query, con=connection)
        logging.info(f"✅ {len(df)} registros extraídos de la base de datos de Grammy.")
        return df

    except Exception as e:
        logging.error(f"❌ Error durante la extracción de datos de Grammy: {e}")
        raise

    finally:
        if connection and connection.is_connected():
            connection.close()
            logging.info("🔒 Conexión a la base de datos cerrada correctamente.")
