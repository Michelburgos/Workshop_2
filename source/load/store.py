import os
import logging
import pickle
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from dotenv import load_dotenv

# Cargar variables del archivo .env
load_dotenv()

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Scopes
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def authenticate_drive():
    """
    Autentica con Google Drive utilizando OAuth.
    En WSL o servidores sin entorno gráfico, se usa run_console() en lugar de run_local_server().
    """
    creds = None
    token_path = os.getenv("GOOGLE_TOKEN_PATH", "token.pickle")
    credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH")

    if not credentials_path:
        raise FileNotFoundError("❌ Variable GOOGLE_CREDENTIALS_PATH no encontrada en .env")

    # Intentar cargar el token ya guardado
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    # Si no hay credenciales válidas, iniciar flujo de autenticación
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # 📌 AUTENTICACIÓN DESDE TERMINAL (sin entorno gráfico, útil en WSL/Airflow)
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            creds = flow.run_console()

        # Guardar token para futuros usos
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)


def upload_file_to_drive(filepath: str, filename: str = None, folder_id: str = None):
    """
    Sube un archivo a Google Drive.
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
