import pandas as pd
import psycopg2
from sqlalchemy import create_engine

FINAL_DATASET = '/opt/airflow/output/final_dataset.csv'

# Configuración de la conexión (ajústala según tu entorno)
DB_HOST = 'postgres'
DB_PORT = '5432'
DB_NAME = 'etl_project'
DB_USER = 'airflow'
DB_PASSWORD = 'airflow'

TABLE_NAME = 'spotify_grammy_musicbrainz'

def load_to_postgres():
    print("📥 Cargando datos a la base de datos...")

    # Cargar el dataset final
    df = pd.read_csv(FINAL_DATASET)

    # Crear engine de conexión
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Cargar en la tabla (reemplaza si ya existe)
    df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)

    print(f"✅ Datos cargados correctamente en la tabla '{TABLE_NAME}'")
