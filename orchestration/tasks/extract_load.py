import subprocess
from prefect import task

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from _utils.helpers import run_py_file

from env import (
    S3_TO_POSTGRES_PATH,
    S3_TO_POSTGRES_PATH_TEST,
    EXTRACT_LOAD_PYTHON_EXEC_PATH,
)


@task(name='Load S3 Files to Postgres')
def s3_to_postgres():
    run_py_file(
        filepath=S3_TO_POSTGRES_PATH,
        python_exec_path=EXTRACT_LOAD_PYTHON_EXEC_PATH,
    )

@task(name='Test Load S3 Files to Postgres')
def s3_to_postgres_test():
    run_py_file(
        filepath=S3_TO_POSTGRES_PATH_TEST,
        python_exec_path=EXTRACT_LOAD_PYTHON_EXEC_PATH,
    )


if __name__ == '__main__':
    parse_pdfs_s3()