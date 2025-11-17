import os
from pathlib import Path

env_path = Path(__file__).resolve().parent / '.env'

from dotenv import load_dotenv
load_dotenv(dotenv_path=env_path)


PARSE_PDFS_S3_PATH = os.getenv('PARSE_PDFS_S3_PATH')
PARSE_PDFS_S3_PATH_TEST = os.getenv('PARSE_PDFS_S3_PATH_TEST')
PARSE_PDFS_S3_PYTHON_EXEC_PATH = os.getenv('PARSE_PDFS_S3_PYTHON_EXEC_PATH')

S3_TO_POSTGRES_PATH = os.getenv('S3_TO_POSTGRES_PATH')
S3_TO_POSTGRES_PATH_TEST = os.getenv('S3_TO_POSTGRES_PATH_TEST')
EXTRACT_LOAD_PYTHON_EXEC_PATH = os.getenv('EXTRACT_LOAD_PYTHON_EXEC_PATH')

DBT_DIR = os.environ['DBT_DIR']
DBT_DOTENV_PATH = os.environ['DBT_DOTENV_PATH']
DBT_VENV_PATH = os.environ['DBT_VENV_PATH']
DBT_EXEC_PATH = os.environ['DBT_EXEC_PATH']
DBT_EXEC_PYTHON_PATH = os.environ['DBT_EXEC_PYTHON_PATH']
