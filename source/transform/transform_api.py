import pandas as pd

def imputar_nulos_api(df: pd.DataFrame) -> pd.DataFrame:
    df['country'] = df['country'].fillna('Unknown')
    df['begin_date'] = df['begin_date'].fillna('Unknown')
    df['end_date'] = df['end_date'].fillna('Active')
    df['type'] = df['type'].fillna('Unknown')
    df['gender'] = df.apply(lambda row: 'N/A' if row['type'] == 'Group' else row['gender'], axis=1)
    df['gender'] = df['gender'].fillna('Unknown')
    return df

def eliminar_columnas_innecesarias_api(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(columns=['artist_id', 'disambiguation'], errors='ignore')

def transform_api_data() -> pd.DataFrame:

    df = imputar_nulos_api(df)
    df = eliminar_columnas_innecesarias_api(df)

    return df

