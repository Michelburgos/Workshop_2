import pandas as pd

def extract_spotify(path_to_csv='spotify_data.csv'):
    """
    Extrae datos desde un archivo CSV de Spotify.
    """
    try:
        df = pd.read_csv(path_to_csv)
        print(f"✅ Datos de Spotify cargados correctamente. Filas: {len(df)}")
        return df
    except Exception as e:
        print(f"❌ Error al cargar datos de Spotify: {e}")
        return pd.DataFrame()