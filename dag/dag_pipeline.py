import os
import sys
import logging
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Hacer visible la carpeta source
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# === Importar funciones ===
from source.extract.extract_api import extract_api
from source.extract.extract_grammys import extract_grammy
from source.extract.extract_spotify import extract_spotify
from source.transform.transform_api import transform_wikidata
from source.transform.transform_grammys import transform_grammy_data
from source.transform.transform_spotify import transform_spotify_data
from source.transform.merge import merge_datasets
from source.load.load import upload_dataframe
from source.load.store import upload_file_to_drive

# === Configuración del DAG ===
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'execution_timeout': timedelta(minutes=100)  # Aumentar a 10 minutos
}

dag = DAG(
    dag_id='etl_artists_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='ETL completo para datos de artistas (Spotify, Grammy, Wikidata)'
)


# === Rutas temporales ===
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_TEMP_DIR = os.path.join(BASE_DIR, 'data_temp')
os.makedirs(DATA_TEMP_DIR, exist_ok=True)

SPOTIFY_PATH = os.path.join(DATA_TEMP_DIR, 'spotify.csv')
GRAMMY_PATH = os.path.join(DATA_TEMP_DIR, 'grammy.csv')
API_PATH = os.path.join(DATA_TEMP_DIR, 'wikidata.csv')
MERGED_PATH = os.path.join(DATA_TEMP_DIR, 'merged.csv')

# ========== TAREAS ==========

# 🔽 Extracción
def task_extract_spotify():
    df = extract_spotify()
    if df.empty:
        raise ValueError("❌ El DataFrame de Spotify está vacío, no se puede continuar.")
    df.to_csv(SPOTIFY_PATH, index=False)
    logging.info(f"✅ Spotify extraído en: {SPOTIFY_PATH}")

def task_extract_grammy():
    df = extract_grammy()
    if df.empty:
        raise ValueError("❌ El DataFrame de Grammy está vacío, no se puede continuar.")
    df.to_csv(GRAMMY_PATH, index=False)
    logging.info(f"✅ Grammy extraído en: {GRAMMY_PATH}")

def task_extract_api():
    df = extract_api()
    if df.empty:
        logging.warning("⚠️ El DataFrame de Wikidata está vacío.")
    df.to_csv(API_PATH, index=False)
    logging.info(f"✅ Wikidata extraído en: {API_PATH}")

# 🔄 Transformaciones separadas
def task_transform_spotify():
    df = pd.read_csv(SPOTIFY_PATH)
    df_transformed = transform_spotify_data(df)
    if df_transformed.empty:
        raise ValueError("❌ El DataFrame transformado de Spotify está vacío.")
    df_transformed.to_csv(SPOTIFY_PATH, index=False)
    logging.info(f"✅ Spotify transformado y sobrescrito: {SPOTIFY_PATH}")

def task_transform_grammy():
    df = pd.read_csv(GRAMMY_PATH)
    df_transformed = transform_grammy_data(df)
    if df_transformed.empty:
        raise ValueError("❌ El DataFrame transformado de Grammy está vacío.")
    df_transformed.to_csv(GRAMMY_PATH, index=False)
    logging.info(f"✅ Grammy transformado y sobrescrito: {GRAMMY_PATH}")

def task_transform_api():
    df = pd.read_csv(API_PATH)
    df_transformed = transform_wikidata(df)
    if df_transformed.empty:
        logging.warning("⚠️ El DataFrame transformado de Wikidata está vacío.")
    df_transformed.to_csv(API_PATH, index=False)
    logging.info(f"✅ Wikidata transformado y sobrescrito: {API_PATH}")

# 🔗 Merge
def task_merge():
    df_spotify = pd.read_csv(SPOTIFY_PATH)
    df_grammy = pd.read_csv(GRAMMY_PATH)
    df_api = pd.read_csv(API_PATH)
    df_merged = merge_datasets(df_spotify, df_grammy, df_api)
    if df_merged.empty:
        raise ValueError("❌ El DataFrame combinado está vacío.")
    df_merged.to_csv(MERGED_PATH, index=False)
    logging.info(f"✅ Merge guardado en: {MERGED_PATH}")

# 📤 Carga a PostgreSQL
def task_load():
    df = pd.read_csv(MERGED_PATH)
    upload_dataframe(df, table_name="artists_data", if_exists="replace")
    logging.info("✅ Datos cargados en PostgreSQL.")

# ☁️ Subir a Google Drive
def task_store():
    upload_file_to_drive(filepath=MERGED_PATH)
    logging.info("✅ Archivo subido a Google Drive.")

# ========== DEFINICIÓN DE TAREAS ==========

t_extract_spotify = PythonOperator(task_id="extract_spotify", python_callable=task_extract_spotify, dag=dag)
t_extract_grammy = PythonOperator(task_id="extract_grammy", python_callable=task_extract_grammy, dag=dag)
t_extract_api = PythonOperator(task_id="extract_api", python_callable=task_extract_api, dag=dag)

t_transform_spotify = PythonOperator(task_id="transform_spotify", python_callable=task_transform_spotify, dag=dag)
t_transform_grammy = PythonOperator(task_id="transform_grammy", python_callable=task_transform_grammy, dag=dag)
t_transform_api = PythonOperator(task_id="transform_api", python_callable=task_transform_api, dag=dag)

t_merge = PythonOperator(task_id="merge_datasets", python_callable=task_merge, dag=dag)
t_load = PythonOperator(task_id="load_to_postgres", python_callable=task_load, dag=dag)
t_store = PythonOperator(task_id="upload_to_drive", python_callable=task_store, dag=dag)

# ========== FLUJO DE TAREAS ==========

# Extracción → Transformación
t_extract_api >> t_transform_api
t_extract_spotify >> t_transform_spotify
t_extract_grammy >> t_transform_grammy

# Transformaciones → Merge
[t_transform_api, t_transform_spotify, t_transform_grammy] >> t_merge

# Merge → Load → Store
t_merge >> t_load >> t_store
