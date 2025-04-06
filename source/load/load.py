import logging
from transform.merge import merge_datasets
from BD_connection import get_sqlalchemy_engine

def save_merged_to_db():
    """
    Carga el DataFrame final del merge y lo guarda en la base de datos.
    """
    try:
        df_final = merge_datasets()
        engine = get_sqlalchemy_engine()
        df_final.to_sql("cleaned_merged_data", engine, if_exists="replace", index=False)
        logging.info("✅ Datos guardados exitosamente en la base de datos.")
        print("✅ Datos guardados exitosamente en la base de datos.")

    except Exception as e:
        logging.error(f"❌ Error al guardar en la base de datos: {e}")
        raise

if __name__ == "__main__":
    save_merged_to_db()

