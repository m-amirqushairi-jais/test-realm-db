import os
import io
import csv
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload


# Set up the Google Drive API client
def setup_api_client():
    SCOPES = ['https://www.googleapis.com/auth/drive']
    creds = None

    if os.path.exists('client_secret.json'):
        flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
        creds = flow.run_local_server(port=0)
    else:
        raise FileNotFoundError("client_secret.json not found. Please follow the instructions to enable the Google Drive API and obtain your API credentials.")

    return build('drive', 'v3', credentials=creds)


# List all CSV and Google Sheet files in Google Drive
def list_csv_and_spreadsheets(service):
    try:
        query = "mimeType='text/csv' or mimeType='application/vnd.google-apps.spreadsheet'"
        results = service.files().list(q=query, fields="nextPageToken, files(id, name, mimeType)").execute()
        return results.get('files', [])

    except HttpError as error:
        print(f"An error occurred: {error}")
        return []


# Download CSV or export Google Sheet as CSV
def download_csv(service, file, output_directory):
    file_id = file['id']
    file_name = file['name']
    mime_type = file['mimeType']

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    if mime_type == 'application/vnd.google-apps.spreadsheet':
        request = service.files().export_media(fileId=file_id, mimeType='text/csv')
        file_name += '.csv'
    else:
        request = service.files().get_media(fileId=file_id)

    file_path = os.path.join(output_directory, file_name)

    with open(file_path, 'wb') as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Downloaded {file_name}: {int(status.progress() * 100)}.")
    return file_path


def main():
    output_directory = 'csv_files'

    service = setup_api_client()
    files = list_csv_and_spreadsheets(service)

    if not files:
        print('No CSV or spreadsheet files found.')
    else:
        for file in files:
            download_csv(service, file, output_directory)


if __name__ == '__main__':
    main()
