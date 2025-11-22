import subprocess
from prefect import task

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from _utils.helpers import run_py_file

from env import (
    EXTRACT_LOAD_PYTHON_EXEC_PATH,
    S3_TO_POSTGRES_PATH,
    S3_TO_POSTGRES_TEST_PATH,
    S3_TO_POSTGRES_DEV_PATH,
    S3_TO_POSTGRES_DEV_TEST_PATH,
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
        filepath=S3_TO_POSTGRES_TEST_PATH,
        python_exec_path=EXTRACT_LOAD_PYTHON_EXEC_PATH,
    )

@task(name='Load S3 Files to Postgres - Dev')
def s3_to_postgres_dev():
    run_py_file(
        filepath=S3_TO_POSTGRES_DEV_PATH,
        python_exec_path=EXTRACT_LOAD_PYTHON_EXEC_PATH,
    )

@task(name='Test Load S3 Files to Postgres - Dev')
def s3_to_postgres_dev_test():
    run_py_file(
        filepath=S3_TO_POSTGRES_DEV_TEST_PATH,
        python_exec_path=EXTRACT_LOAD_PYTHON_EXEC_PATH,
    )

if __name__ == '__main__':
    s3_to_postgres_dev_test()