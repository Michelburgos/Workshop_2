# transform_wikidata.py
import pandas as pd
import logging
from langdetect import detect
from tqdm import tqdm

# ========== CONFIGURACIÓN ==========
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
tqdm.pandas(desc="🔍 Filtrando premios válidos")

# Palabras comunes de otros idiomas
palabras_no_ingles = [
    "stär um", "para", "prêmio", "premio", "prix", "voor", "de", "sus", "la", "das", "del", "der", "des",
    "el", "le", "pe", "stella", "sulla", "nagroda", "carriera", "réalta", "premi", "xelata", "tähti",
    "æresdoktor", "famen", "doktor", "oriel", "anfarwolion", "auf dem", "or merit", "kpakpando", "stäär üüb"
]

award_lang_cache = {}

def is_english_filtered(text):
    text_l = str(text).lower().strip()
    if text not in award_lang_cache:
        try:
            award_lang_cache[text] = detect(text)
        except:
            award_lang_cache[text] = "unknown"
    if award_lang_cache[text] != "en":
        return False
    return not any(palabra in text_l for palabra in palabras_no_ingles)


# ========== TRANSFORMACIÓN PRINCIPAL ==========
def transform_wikidata(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("🎯 Iniciando transformación del DataFrame Wikidata...")

    # 1. Limpieza básica
    df['country'] = df['country'].fillna('Unknown')
    df['birth'] = df['birth'].fillna('Unknown')
    df['death'] = df['death'].notna().map({False: 'alive', True: 'deceased'})


    # 3. Eliminar nulos y duplicados exactos
    df = df.dropna()
    df = df.drop_duplicates()

    # 4. Filtrar premios válidos en inglés
    df = df[df['award'].notna() & df['award'].progress_apply(is_english_filtered)]

    # 5. Agrupar por artista consolidando información
    def valor_mas_comun(serie):
        return serie.mode().iloc[0] if not serie.mode().empty else serie.dropna().iloc[0]

    agrupado = df.groupby("artist").agg({
        "country": valor_mas_comun,
        "type": valor_mas_comun,
        "birth": valor_mas_comun,
        "death": valor_mas_comun,
        "gender": valor_mas_comun,
        "award": lambda x: sorted(set(x))
    }).reset_index()

    # 6. Columnas extra
    agrupado["award_count"] = agrupado["award"].apply(len)
    agrupado["won_grammy"] = agrupado["award"].apply(
        lambda premios: any("grammy" in premio.lower() for premio in premios)
    )
    agrupado["award"] = agrupado["award"].apply(lambda x: "; ".join(x))

    logging.info("✅ Transformación completada. Total artistas únicos: %s", len(agrupado))
    return agrupado

