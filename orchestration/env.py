import os
from pathlib import Path

env_path = Path(__file__).resolve().parent / '.env'

from dotenv import load_dotenv
load_dotenv(dotenv_path=env_path)


PARSE_PDFS_S3_PATH = os.getenv('PARSE_PDFS_S3_PATH')
PARSE_PDFS_S3_PATH_TEST = os.getenv('PARSE_PDFS_S3_PATH_TEST')
PARSE_PDFS_S3_PYTHON_EXEC_PATH = os.getenv('PARSE_PDFS_S3_PYTHON_EXEC_PATH')
