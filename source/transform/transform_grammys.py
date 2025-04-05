import pandas as pd

INPUT_PATH = '/opt/airflow/data/grammys.csv'
OUTPUT_PATH = '/opt/airflow/intermediate/grammy_clean.csv'

def transform_grammy_data():
    print("🔄 Transformando datos de Grammy...")

    df = pd.read_csv(INPUT_PATH)

    # Limpiar nombres de canciones y artistas
    df['title'] = df['title'].str.lower().str.strip()
    df['artist'] = df['artist'].str.lower().str.strip()

    # Convertir columna 'winner' a booleano 'is_nominated'
    df['is_nominated'] = df['winner'].apply(lambda x: True if pd.notna(x) else False)

    # Seleccionar columnas clave
    df = df[['year', 'title', 'artist', 'category', 'is_nominated']]

    df.to_csv(OUTPUT_PATH, index=False)
    print("✅ Datos de Grammy transformados y guardados.")
