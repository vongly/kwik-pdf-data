import subprocess
from prefect import task

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from _utils.helpers import run_py_file

from env import (
    PARSE_PDFS_S3_PATH,
    PARSE_PDFS_S3_PATH_TEST,
    PARSE_PDFS_S3_PYTHON_EXEC_PATH,
)


@task(name='Parse PDF Files from S3 Bucket')
def parse_pdfs_s3():
    run_py_file(
        filepath=PARSE_PDFS_S3_PATH,
        python_exec_path=PARSE_PDFS_S3_PYTHON_EXEC_PATH,
    )

@task(name='Test Parse PDF Files from S3 Bucket')
def parse_pdfs_s3_test():
    run_py_file(
        filepath=PARSE_PDFS_S3_PATH_TEST,
        python_exec_path=PARSE_PDFS_S3_PYTHON_EXEC_PATH,
    )


if __name__ == '__main__':
    parse_pdfs_s3()