import os
from pathlib import Path

env_path = Path(__file__).resolve().parent / '.env'

from dotenv import load_dotenv
load_dotenv(dotenv_path=env_path)

ROOT_DIR = Path(os.getenv('ROOT_DIR'))

PARSE_PDFS_S3_PYTHON_EXEC_PATH = ROOT_DIR / os.getenv('PARSE_PDFS_S3_PYTHON_EXEC_PATH')
PARSE_PDFS_S3_PATH = ROOT_DIR / os.getenv('PARSE_PDFS_S3_PATH')
PARSE_PDFS_S3_TEST_PATH = ROOT_DIR / os.getenv('PARSE_PDFS_S3_TEST_PATH')

EXTRACT_LOAD_PYTHON_EXEC_PATH = ROOT_DIR / os.getenv('EXTRACT_LOAD_PYTHON_EXEC_PATH')
S3_TO_POSTGRES_PATH = ROOT_DIR / os.getenv('S3_TO_POSTGRES_PATH')
S3_TO_POSTGRES_TEST_PATH = ROOT_DIR / os.getenv('S3_TO_POSTGRES_TEST_PATH')
S3_TO_POSTGRES_DEV_PATH = ROOT_DIR / os.getenv('S3_TO_POSTGRES_DEV_PATH')
S3_TO_POSTGRES_DEV_TEST_PATH = ROOT_DIR / os.getenv('S3_TO_POSTGRES_DEV_TEST_PATH')

DBT_DIR = ROOT_DIR / os.environ['DBT_DIR']
DBT_DOTENV_PATH = ROOT_DIR / os.environ['DBT_DOTENV_PATH']
DBT_VENV_PATH = ROOT_DIR / os.environ['DBT_VENV_PATH']
DBT_EXEC_PATH = ROOT_DIR / os.environ['DBT_EXEC_PATH']
DBT_EXEC_PYTHON_PATH = ROOT_DIR / os.environ['DBT_EXEC_PYTHON_PATH']

if __name__ == '__main__':
    for name, value in list(globals().items()):
        if name.isupper():
            print(f'{name} = {value}')