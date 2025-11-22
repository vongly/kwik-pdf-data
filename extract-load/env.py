import os
from pathlib import Path

env_path = Path(__file__).resolve().parent / '.env'

from dotenv import load_dotenv
load_dotenv(dotenv_path=env_path)


ROOT_DIR = Path(os.environ['ROOT_DIR'])
PROJECT_DIR = Path(os.environ['ROOT_DIR']) / os.environ['PROJECT_DIR_RELATIVE']

EXTRACT_DIR = PROJECT_DIR / os.environ['EXTRACT_DIR_RELATIVE']

PIPELINES_DIR_RELATIVE = os.environ['PIPELINES_DIR_RELATIVE']
PIPELINES_DIR = os.path.join(PROJECT_DIR, PIPELINES_DIR_RELATIVE)

S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_REGION = os.getenv('S3_REGION')
S3_ENDPOINT = os.getenv('S3_ENDPOINT')
S3_BUCKET = os.getenv('S3_BUCKET')
S3_REPORT_FOLDERS_PATH_BASE = os.getenv('S3_REPORT_FOLDERS_PATH_BASE')

POSTGRES_USER_DEV = os.environ['POSTGRES_USER_DEV']
POSTGRES_PASSWORD_DEV = os.environ['POSTGRES_PASSWORD_DEV']
POSTGRES_HOST_DEV = os.environ['POSTGRES_HOST_DEV']
POSTGRES_PORT_DEV = os.environ['POSTGRES_PORT_DEV']
POSTGRES_DB_DEV = os.environ['POSTGRES_DB_DEV']
POSTGRES_CERTIFICATION_DEV = None if os.environ['POSTGRES_CERTIFICATION_DEV'] == 'None' else os.environ['POSTGRES_CERTIFICATION_DEV']
POSTGRES_CERTIFICATION_PATH_DEV = None if POSTGRES_CERTIFICATION_DEV is None else os.path.join(ROOT_DIR, POSTGRES_CERTIFICATION_DEV)
POSTGRES_TABLES_PREFIX_DEV = '' if os.environ['POSTGRES_TABLES_PREFIX_DEV'] == 'None' else os.environ['POSTGRES_TABLES_PREFIX_DEV']

POSTGRES_USER_PROD = os.environ['POSTGRES_USER_PROD']
POSTGRES_PASSWORD_PROD = os.environ['POSTGRES_PASSWORD_PROD']
POSTGRES_HOST_PROD = os.environ['POSTGRES_HOST_PROD']
POSTGRES_PORT_PROD = os.environ['POSTGRES_PORT_PROD']
POSTGRES_DB_PROD = os.environ['POSTGRES_DB_PROD']
POSTGRES_CERTIFICATION_PROD = None if os.environ['POSTGRES_CERTIFICATION_PROD'] == 'None' else os.environ['POSTGRES_CERTIFICATION_PROD']
POSTGRES_CERTIFICATION_PATH_PROD = None if POSTGRES_CERTIFICATION_PROD is None else os.path.join(ROOT_DIR, POSTGRES_CERTIFICATION_PROD)
POSTGRES_TABLES_PREFIX_PROD = '' if os.environ['POSTGRES_TABLES_PREFIX_PROD'] == 'None' else os.environ['POSTGRES_TABLES_PREFIX_PROD']

if __name__ == '__main__':
    for name, value in list(globals().items()):
        if name.isupper():
            print(f'{name} = {value}')