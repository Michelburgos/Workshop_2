import logging
import pandas as pd
from source.BD_connection import get_connection

# =============================
# 🔧 Configurar logging
# =============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =============================
# ⬆️ Subir el DataFrame
# =============================
def upload_dataframe(df: pd.DataFrame, table_name: str, if_exists: str = "replace"):
    """
    Sube un DataFrame a una tabla de PostgreSQL usando la conexión de BD_connection.py

    Args:
        df (pd.DataFrame): El DataFrame a subir.
        table_name (str): Nombre de la tabla destino.
        if_exists (str): 'replace', 'append' o 'fail' (default: 'replace')
    """
    engine = get_connection("merge")

    try:
        logging.info(f"⬆️ Subiendo datos a la tabla '{table_name}'...")
        df.to_sql(table_name, con=engine, index=False, if_exists=if_exists)
        logging.info(f"✅ Datos subidos exitosamente a '{table_name}'. Total filas: {len(df)}")
    except Exception as e:
        logging.error(f"❌ Error al subir el DataFrame: {e}")
        raise

