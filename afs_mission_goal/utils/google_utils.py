"""
This module contains functions for accessing Google Sheets.

Before using this module, you need to:
Set up a service account and share the Google Sheet with the service account email.
Store the credentials file in the `.credentials/` directory or S3 bucket.

Usage:
import afs_mumsnet_analysis.utils.google_utils as gu

# access data from Google Sheets
data = google_utils.access_google_sheet(<sheet_id>, <sheet_name>)

Make sure the credentials file is stored in the `.credentials/` directory or S3 bucket.
Place the path to the credentials file in the `.env` file as an environment variable.
"""

# from google.oauth2.service_account import Credentials
from df2gspread import gspread2df as g2d
from oauth2client.service_account import ServiceAccountCredentials
from pathlib import PosixPath
import pandas as pd
from afs_mission_goal import PROJECT_DIR, S3_BUCKET, logging
from nesta_ds_utils.loading_saving.S3 import download_file

from os import environ, path
from dotenv import load_dotenv

load_dotenv()


def find_credentials(credentials_env_var: str) -> PosixPath:
    """Find credentials file

    For accessing some Google resources, we need credentials stored in a JSON file in `.credentials/`.
    This function takes the name of an environment variable as input and checks whether the corresponding
    credentials file exists. If not, it downloads the file from S3.

    Args:
        credentials_env_var (str): Name of the env var eg "GOOGLE_SHEETS_CREDENTIALS".
        Your .env file should have paths to Google credentials files stored like
        "GOOGLE_APPLICATION_CREDENTIALS=<path-to-credentials-file>".

    Raises:
        EnvironmentError: If this env var is not recorded in `.env`
        Exception: If the function can neither find the credentials file nor download it from S3

    Returns:
        PosixPath: Path to the credentials file
    """
    # Check if the environment variable is set
    if credentials_env_var not in environ:
        raise EnvironmentError("The environment variable is not set.")

    credentials_json = PROJECT_DIR / environ.get(credentials_env_var)

    if not path.isfile(credentials_json):
        logging.info("Credentials not found. Downloading from S3...")
        try:
            download_file(
                path_from=f"credentials/{credentials_json.name}",
                bucket=S3_BUCKET,
                path_to=str(credentials_json),
            )
        except Exception as e:
            raise Exception(f"Error downloading credentials from S3: {e}")

    return credentials_json


def access_google_sheet(
    sheet_id: str, sheet_name: str, col_names: bool = True, row_names: bool = True
) -> pd.DataFrame:
    """
    Accesses a specified Google Sheet and returns its contents as a pandas DataFrame.

    This function authenticates using service account credentials, defines the scope
    for the Google Sheets API, and downloads the sheet contents. The sheet is accessed
    by its unique identifier and a specific sheet name within the spreadsheet.

    Args:
        sheet_id (str): The unique identifier for the Google Sheets file.
        sheet_name (str): The name of the individual sheet within the Google Sheets file.
        col_names (bool): Whether to use the first row as column names (default is True).
        row_names (bool): Whether to use the first column as row names (default is True).

    Returns:
        pandas.DataFrame: A DataFrame containing the data from the specified Google Sheet.

    Raises:
        GoogleAuthError: If authentication with Google Sheets API fails.
        DownloadError: If there is an issue downloading the sheet contents.

    Notes:
    - The GOOGLE_SHEETS_CREDENTIALS environment variable must be set with the path to
      the credentials JSON file ie `.credentials/xxxxx.json`.
    - The service account must have the necessary permissions to access the Google Sheet.
    - The function assumes the first row and column of the sheet contain the header and
      index names, respectively.
    """
    # Load the credentials for use with Google Sheets
    google_credentials_json = find_credentials("GOOGLE_SHEETS_CREDENTIALS")

    # Define the scope for the Google Sheets API (we only want Google sheets)
    scope = ["https://spreadsheets.google.com/feeds"]

    # Authenticate using the credentials JSON file
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        google_credentials_json, scope
    )

    # Load the data into a pandas DataFrame
    data = g2d.download(
        sheet_id,
        sheet_name,
        credentials=credentials,
        col_names=col_names,
        row_names=row_names,
    )

    return data
