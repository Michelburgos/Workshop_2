import os
import sys
import logging
import pandas as pd

# Añadir el path al directorio BD_connection
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'BD_connection')))
from BD_connection import get_sqlalchemy_engine

# Configurar logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

def extract_grammy(query="SELECT * FROM raw_grammy"):
    """
    Ejecuta una consulta SQL sobre la base de datos PostgreSQL de Grammy.

    Args:
        query (str): Consulta SQL a ejecutar. Por defecto: "SELECT * FROM raw_grammy".

    Returns:
        pd.DataFrame: Resultados de la consulta.
    
    Raises:
        Exception: Si ocurre un error durante la conexión o extracción.
    """
    engine = None
    try:
        logging.info("Estableciendo conexión con la base de datos PostgreSQL...")
        engine = get_sqlalchemy_engine()

        df = pd.read_sql(query, con=engine)
        logging.info(f"✅ {len(df)} registros extraídos de la tabla 'raw_grammy'.")
        return df

    except Exception as e:
        logging.error(f"❌ Error durante la extracción de datos de Grammy: {e}")
        raise

    finally:
        if engine:
            engine.dispose()
            logging.info("🔒 Conexión a PostgreSQL cerrada correctamente.")

