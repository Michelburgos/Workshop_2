import os
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

FINAL_DATASET = '/opt/airflow/output/final_dataset.csv'
GDRIVE_FOLDER_ID = 'TU_FOLDER_ID_AQUI'  # Reemplaza con tu folder de Google Drive

def store_to_drive():
    print("☁️ Subiendo dataset final a Google Drive...")

    # Autenticación con Google Drive
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # Abre navegador para autenticar (solo la primera vez)
    drive = GoogleDrive(gauth)

    # Crear archivo en Drive
    file_drive = drive.CreateFile({
        'title': 'final_dataset.csv',
        'parents': [{'id': GDRIVE_FOLDER_ID}]
    })
    file_drive.SetContentFile(FINAL_DATASET)
    file_drive.Upload()

    print("✅ Dataset subido exitosamente a Google Drive.")
