import os
import logging
import pickle
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from dotenv import load_dotenv


load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SCOPES = ['https://www.googleapis.com/auth/drive.file']


def authenticate_drive():
    """Autentica con Google Drive usando OAuth 2.0 y retorna un servicio autenticado.

    Returns:
        googleapiclient.discovery.Resource: Servicio de Google Drive autenticado.

    Raises:
        FileNotFoundError: Si no se encuentra la variable GOOGLE_CREDENTIALS_PATH en .env.
        Exception: Si falla la autenticación en navegador y consola.
    """
    creds = None
    token_path = os.getenv("GOOGLE_TOKEN_PATH", "token.pickle")
    credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH")

    if not credentials_path:
        raise FileNotFoundError("Variable GOOGLE_CREDENTIALS_PATH no encontrada en .env")

    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            try:
                logging.info("Intentando autenticación con navegador...")
                creds = flow.run_local_server(port=8080)
            except Exception as e:
                logging.warning(f"Falló autenticación en navegador: {e}")
                logging.info("Usando autenticación por consola como fallback...")
                creds = flow.run_console()

        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds)


def upload_file_to_drive(filepath: str, filename: str = None, folder_id: str = None):
    """Sube un archivo a Google Drive en una carpeta específica.

    Args:
        filepath (str): Ruta local del archivo a subir.
        filename (str, optional): Nombre con el que se guardará el archivo en Drive.
            Si no se proporciona, se usa el nombre del archivo en filepath.
        folder_id (str, optional): ID de la carpeta en Google Drive donde se subirá
            el archivo. Si no se proporciona, se usa GOOGLE_DRIVE_FOLDER_ID de .env.

    Raises:
        ValueError: Si no se proporciona folder_id ni existe GOOGLE_DRIVE_FOLDER_ID
            en .env.
        Exception: Si ocurre un error al subir el archivo a Google Drive.
    """
    service = authenticate_drive()

    # Obtener folder_id desde .env si no fue pasado como argumento
    if folder_id is None:
        folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
        if not folder_id:
            raise ValueError("No se proporcionó folder_id ni existe GOOGLE_DRIVE_FOLDER_ID en .env")

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
        logging.info(f"Archivo subido exitosamente a Drive con ID: {file.get('id')}")
    except Exception as e:
        logging.error(f"Error al subir archivo a Google Drive: {e}")
