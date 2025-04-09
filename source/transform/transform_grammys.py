import pandas as pd
import re
import logging
from BD_connection import get_sqlalchemy_engine

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def impute_artist(nominee: str, category: str) -> str | None:
    """
    Intenta imputar el nombre del artista a partir del nominado, dependiendo de la categoría.
    """
    if pd.isna(nominee):
        return None

    artist_categories = [
        'Best New Artist', 'Best New Artist Of', 'Best Producer Of The Year',
        'Producer Of The Year', 'Classical Producer Of The Year', 'Remixer Of The Year'
    ]

    if any(cat in category for cat in artist_categories):
        return nominee

    match = re.match(r'^([^-\\(]+?)\s*[-–]\s*.*$', nominee)
    if match:
        return match.group(1).strip()

    if len(nominee.split()) > 3 or any(kw in nominee.lower() for kw in ['album', 'song', 'works of']):
        return None

    return nominee


def extract_artist_from_parentheses(workers: str) -> str | None:
    """
    Extrae texto entre paréntesis del campo 'workers', si existe.
    """
    if pd.isna(workers):
        return None
    match = re.search(r'\(([^)]+)\)$', workers)
    return match.group(1).strip() if match else None


def extraer_artista(worker: str) -> str | None:
    """
    Extrae nombre de artista usando patrones conocidos desde el campo 'workers'.
    """
    if pd.isnull(worker):
        return None

    m = re.match(r"([^,;]+), (soloist|composer|conductor|artist)", worker)
    if m:
        return m.group(1).strip()

    m = re.match(r"(.+?(Featuring|&| and ).*?)(;|,|$)", worker, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    return worker.strip()


def transform_grammy_data() -> pd.DataFrame:
    """
    Ejecuta todo el proceso de transformación del dataset de premios Grammy.
    """
    logging.info("📥 Extrayendo datos desde base de datos...")
    engine = get_sqlalchemy_engine()
    df = pd.read_sql_query("SELECT * FROM raw_grammy", engine)
    logging.info(f"✅ Datos cargados. Filas: {len(df)}")

    # Eliminar filas con 'nominee' nulo
    df = df.dropna(subset=["nominee"])

    # Filtrar categorías problemáticas
    problematic_categories = [
        'Best Small Ensemble Performance (With Or Without Conductor)',
        'Most Promising New Classical Recording Artist',
        'Best Classical Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)',
        'Best Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)',
        'Best Classical Vocal Soloist',
        'Best New Classical Artist',
        'Best Classical Vocal Performance',
        'Best Classical Vocal Soloist Performance'
    ]
    mask_null = df['artist'].isna() & df['workers'].isna()
    df = df[~(mask_null & df['category'].isin(problematic_categories))]

    # Imputación desde 'nominee'
    logging.info("🔍 Imputando artistas desde 'nominee'...")
    subset = df[df['artist'].isna() & df['workers'].isna()].copy()
    subset['artist'] = subset.apply(lambda row: impute_artist(row['nominee'], row['category']), axis=1)
    df.loc[subset.index, 'artist'] = subset['artist']

    # Imputación desde paréntesis
    mask = df['artist'].isna() & df['workers'].notna()
    df.loc[mask, 'artist'] = df.loc[mask, 'workers'].apply(extract_artist_from_parentheses)

    # Imputación final desde patrones en 'workers'
    df['artist'] = df['artist'].fillna(df['workers'].apply(extraer_artista))

    # Reemplazos y limpieza
    df["artist"] = df["artist"].replace({"(Various Artists)": "Various Artists"})
    df = df.drop(columns=['published_at', 'updated_at', 'img'], errors="ignore")
    df.rename(columns={'winner': 'is_nominated'}, inplace=True)

    logging.info(f"🎼 Transformación completada. Total filas finales: {len(df)}")
    return df
