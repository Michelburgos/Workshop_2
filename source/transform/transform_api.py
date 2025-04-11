# transform_wikidata.py
import pandas as pd
import logging
from langdetect import detect
from tqdm import tqdm


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
tqdm.pandas(desc="Filtrando premios válidos")


palabras_no_ingles = [
    "stär um", "para", "prêmio", "premio", "prix", "voor", "de", "sus", "la", "das", "del", "der", "des",
    "el", "le", "pe", "stella", "sulla", "nagroda", "carriera", "réalta", "premi", "xelata", "tähti",
    "æresdoktor", "famen", "doktor", "oriel", "anfarwolion", "auf dem", "or merit", "kpakpando", "stäär üüb"
]

award_lang_cache = {}

def is_english_filtered(text):
    """Filtra texto para verificar si está en inglés y no contiene palabras comunes de otros idiomas.

    Args:
        text: Texto a analizar (generalmente un nombre de premio).

    Returns:
        bool: True si el texto está en inglés y no contiene palabras no deseadas, False en caso contrario.
    """
    text_l = str(text).lower().strip()
    if text not in award_lang_cache:
        try:
            award_lang_cache[text] = detect(text)
        except:
            award_lang_cache[text] = "unknown"
    if award_lang_cache[text] != "en":
        return False
    return not any(palabra in text_l for palabra in palabras_no_ingles)



def transform_wikidata(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma el DataFrame de Wikidata con datos de artistas y premios.

    Args:
        df (pd.DataFrame): DataFrame crudo con columnas como artist, country, death, gender y award.

    Returns:
        pd.DataFrame: DataFrame transformado con datos consolidados, premios filtrados y columnas adicionales.
    """
    logging.info("Iniciando transformación del DataFrame Wikidata...")


    df['country'] = df['country'].fillna('Unknown')
    df['death'] = df['death'].notna().map({False: 'alive', True: 'deceased'})



    df = df.dropna()
    df = df.drop_duplicates()


    df = df[df['award'].notna() & df['award'].progress_apply(is_english_filtered)]


    def valor_mas_comun(serie):
        """Selecciona el valor más común en una serie.

        Args:
            serie: Serie de pandas con datos a consolidar.

        Returns:
            object: Valor más frecuente o el primer valor no nulo si no hay moda.
        """
        return serie.mode().iloc[0] if not serie.mode().empty else serie.dropna().iloc[0]

    agrupado = df.groupby("artist").agg({
        "country": valor_mas_comun,
        "death": valor_mas_comun,
        "gender": valor_mas_comun,
        "award": lambda x: sorted(set(x))
    }).reset_index()


    agrupado["award_count"] = agrupado["award"].apply(len)
    agrupado["won_grammy"] = agrupado["award"].apply(
        lambda premios: "Yes" if any("grammy" in premio.lower() for premio in premios) else "No"
    )
    agrupado["award"] = agrupado["award"].apply(lambda x: "; ".join(x))

    logging.info("Transformación completada. Total artistas únicos: %s", len(agrupado))
    return agrupado
