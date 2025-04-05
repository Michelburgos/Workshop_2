import pandas as pd
import os

INPUT_FILE = '/opt/airflow/data/grammy.csv'
TEMP_FILE = '/opt/airflow/output/grammy_cleaned.csv'

def extract_grammy_data():
    print("🔍 Leyendo archivo de Grammys...")
    df = pd.read_csv(INPUT_FILE)

    # Renombrar columna para mayor claridad
    df.rename(columns={'winner': 'is_nominated'}, inplace=True)

    # Normalizar texto
    df['title'] = df['title'].str.lower().str.strip()
    df['artist'] = df['artist'].str.lower().str.strip()
    df['category'] = df['category'].str.strip()

    # Filtrar columnas relevantes
    columns_to_keep = [
        'year', 'title', 'category', 'nominee', 'artist', 'is_nominated'
    ]
    df = df[columns_to_keep]

    # Limpieza básica
    df.dropna(subset=['title', 'artist'], inplace=True)

    # Guardar para siguientes tareas
    os.makedirs('/opt/airflow/output', exist_ok=True)
    df.to_csv(TEMP_FILE, index=False)

    print(f"✅ Datos de Grammy procesados y guardados en {TEMP_FILE}")
