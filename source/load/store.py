import logging
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from transform.merge import merge_datasets

def upload_csv_to_drive(file_path: str, folder_id: str = None):
    """
    Sube un archivo CSV a Google Drive.

    Args:
        file_path (str): Ruta del archivo CSV a subir.
        folder_id (str, opcional): ID de la carpeta de destino en Google Drive.
    """
    try:
        gauth = GoogleAuth()
        gauth.LocalWebserverAuth()
        drive = GoogleDrive(gauth)

        file_drive = drive.CreateFile({
            'title': file_path.split('/')[-1],
            'parents': [{'id': folder_id}] if folder_id else []
        })
        file_drive.SetContentFile(file_path)
        file_drive.Upload()

        print(f"✅ Archivo subido exitosamente a Google Drive: {file_drive['title']}")
        print(f"🔗 Link del archivo: https://drive.google.com/file/d/{file_drive['id']}")

    except Exception as e:
        logging.error(f"❌ Error al subir archivo a Drive: {e}")
        raise

def save_and_upload_to_drive():
    """
    Ejecuta el proceso completo: merge + guardar CSV + subir a Drive.
    """
    df_final = merge_datasets()
    output_path = "merged_data_final.csv"
    df_final.to_csv(output_path, index=False)

    # Si tienes una carpeta específica en Drive, reemplaza aquí
    folder_id = None  # Ej: "1AbcD2EfGh3Ij4KlMnoPQrStUvWxYz"
    upload_csv_to_drive(output_path, folder_id)

if __name__ == "__main__":
    save_and_upload_to_drive()

