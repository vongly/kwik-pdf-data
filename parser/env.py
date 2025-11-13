import os
from pathlib import Path

env_path = Path(__file__).resolve().parent / '.env'

from dotenv import load_dotenv
load_dotenv(dotenv_path=env_path)

S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_REGION = os.getenv('S3_REGION')
S3_ENDPOINT = os.getenv('S3_ENDPOINT')
S3_BUCKET = os.getenv('S3_BUCKET')

PDF_TO_BE_PROCESSED_FOLDER_S3 = os.getenv('PDF_TO_BE_PROCESSED_FOLDER_S3')
PDF_PROCESSED_FOLDER_S3 = os.getenv('PDF_PROCESSED_FOLDER_S3')
CSV_BASE_FOLDER_S3 = os.getenv('CSV_BASE_FOLDER_S3')
