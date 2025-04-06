import sys
import os
import pandas as pd

# Añadir el path del directorio BD_connection al sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'BD_connection')))

from BD_connection import get_mysql_connection

def extract_grammy(query="SELECT * FROM grammy_awards"):
    """
    Extrae datos de la base de datos de los Grammy usando BD_connections.py.
    """
    try:
        connection = get_mysql_connection()
        df = pd.read_sql(query, con=connection)
        print(f"✅ Datos de Grammy extraídos correctamente. Filas: {len(df)}")
        return df
    except Exception as e:
        print(f"❌ Error al extraer datos de Grammy: {e}")
        return pd.DataFrame()
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()
