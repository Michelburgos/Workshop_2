import pandas as pd
import re
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def impute_artist(nominee: str, category: str) -> str | None:
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
    if pd.isna(workers):
        return None
    match = re.search(r'\(([^)]+)\)$', workers)
    return match.group(1).strip() if match else None


def extraer_artista(worker: str) -> str | None:
    if pd.isnull(worker):
        return None

    m = re.match(r"([^,;]+), (soloist|composer|conductor|artist)", worker)
    if m:
        return m.group(1).strip()

    m = re.match(r"(.+?(Featuring|&| and ).*?)(;|,|$)", worker, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    return worker.strip()


def transform_grammy_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el DataFrame del dataset Grammy.
    """
    logging.info(f"📊 Iniciando transformación de datos. Filas recibidas: {len(df)}")

    df = df.dropna(subset=["nominee"])

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

    logging.info("🔍 Imputando artistas desde 'nominee'...")
    subset = df[df['artist'].isna() & df['workers'].isna()].copy()
    subset['artist'] = subset.apply(lambda row: impute_artist(row['nominee'], row['category']), axis=1)
    df.loc[subset.index, 'artist'] = subset['artist']

    mask = df['artist'].isna() & df['workers'].notna()
    df.loc[mask, 'artist'] = df.loc[mask, 'workers'].apply(extract_artist_from_parentheses)

    df['artist'] = df['artist'].fillna(df['workers'].apply(extraer_artista))

    df["artist"] = df["artist"].replace({"(Various Artists)": "Various Artists"})
    df = df.drop(columns=['published_at', 'updated_at', 'img'], errors="ignore")
    df.rename(columns={'winner': 'is_nominated'}, inplace=True)

    logging.info(f"🎼 Transformación completada. Total filas finales: {len(df)}")
    return df
