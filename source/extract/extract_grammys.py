import logging
import pandas as pd
from source.BD_connection import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def extract_grammy(query="SELECT * FROM raw_grammy") -> pd.DataFrame:
    try:
        logging.info("📥 Conectando a la base de datos PostgreSQL para extraer Grammy...")
        engine = get_connection()
        df = pd.read_sql(query, con=engine)
        logging.info(f"✅ {len(df)} registros extraídos de Grammy.")
        return df
    except Exception as e:
        logging.error(f"❌ Error extrayendo datos de Grammy: {e}")
        return pd.DataFrame()
    finally:
        if engine:
            engine.dispose()
            logging.info("🔒 Conexión a PostgreSQL cerrada.")


