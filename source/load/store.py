import os
import logging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Scopes
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def authenticate_drive():
    """
    Autentica el usuario para acceder a Google Drive.
    """
    creds = None
    token_path = 'token.pickle'

    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)

def upload_file_to_drive(filepath: str, filename: str = None, folder_id: str = None):
    """
    Sube un archivo a Google Drive.

    Args:
        filepath (str): Ruta local del archivo a subir.
        filename (str): Nombre con el que se subirá (si es diferente al nombre local).
        folder_id (str): ID de la carpeta de destino en Drive (opcional).
    """
    service = authenticate_drive()

    file_metadata = {'name': filename or os.path.basename(filepath)}
    if folder_id:
        file_metadata['parents'] = [folder_id]

    media = MediaFileUpload(filepath, resumable=True)

    try:
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        logging.info(f"✅ Archivo subido exitosamente a Drive con ID: {file.get('id')}")
    except Exception as e:
        logging.error(f"❌ Error al subir archivo a Google Drive: {e}")


