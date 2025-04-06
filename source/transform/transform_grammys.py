import pandas as pd
import re
from BD_connection import get_sqlalchemy_engine

def impute_artist(nominee, category):
    artist_categories = [
        'Best New Artist', 'Best New Artist Of', 'Best Producer Of The Year',
        'Producer Of The Year', 'Classical Producer Of The Year', 'Remixer Of The Year'
    ]
    if pd.isna(nominee):
        return None
    if any(cat in category for cat in artist_categories):
        return nominee
    match = re.match(r'^([^-\\(]+?)\\s*[-–]\\s*.*$', nominee)
    if match:
        return match.group(1).strip()
    if len(nominee.split()) > 3 or any(kw in nominee.lower() for kw in ['album', 'song', 'works of']):
        return None
    return nominee

def extract_artist_from_parentheses(workers):
    if pd.isna(workers):
        return None
    match = re.search(r'\(([^)]+)\)$', workers)
    return match.group(1).strip() if match else None

def extraer_artista(worker):
    if pd.isnull(worker):
        return None
    m = re.match(r"([^,;]+), (soloist|composer|conductor|artist)", worker)
    if m:
        return m.group(1).strip()
    m = re.match(r"(.+?(Featuring|&| and ).*?)(;|,|$)", worker, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return worker.strip()

def transform_grammy_data():
    engine = get_sqlalchemy_engine()
    df = pd.read_sql_query("SELECT * FROM raw_grammy", engine)

    # Eliminar nulos en 'nominee'
    df = df.dropna(subset=["nominee"])

    # Eliminar categorías problemáticas con nulos en artist/workers
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

    # Imputar 'artist' desde 'nominee'
    subset = df[df['artist'].isna() & df['workers'].isna()].copy()
    subset['artist'] = subset.apply(lambda row: impute_artist(row['nominee'], row['category']), axis=1)
    df.loc[subset.index, 'artist'] = subset['artist']

    # Imputar desde paréntesis en 'workers'
    mask = df['artist'].isna() & df['workers'].notna()
    df.loc[mask, 'artist'] = df.loc[mask, 'workers'].apply(extract_artist_from_parentheses)

    # Aplicar reglas adicionales para imputar desde 'workers'
    df['artist'] = df['artist'].fillna(df['workers'].apply(extraer_artista))

    # Reemplazos y limpieza final
    df["artist"] = df["artist"].replace({"(Various Artists)": "Various Artists"})
    df = df.drop(columns=['published_at', 'updated_at', 'img'])
    df.rename(columns={'winner': 'is_nominated'}, inplace=True)

    return df


