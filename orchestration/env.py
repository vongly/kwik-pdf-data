import os
from pathlib import Path

env_path = Path(__file__).resolve().parent / '.env'

from dotenv import load_dotenv
load_dotenv(dotenv_path=env_path)

ROOT_DIR = Path(__file__).resolve().parent.parent


PARSE_PDFS_S3_PATH = ROOT_DIR / os.getenv('PARSE_PDFS_S3_PATH')
PARSE_PDFS_S3_PATH_TEST = ROOT_DIR / os.getenv('PARSE_PDFS_S3_PATH_TEST')
PARSE_PDFS_S3_PYTHON_EXEC_PATH = ROOT_DIR / os.getenv('PARSE_PDFS_S3_PYTHON_EXEC_PATH')
