from prefect import get_run_logger
import sys
import os
from pathlib import Path
import subprocess

from glob import glob

def run_py_file(filepath, python_exec_path):

    logger = get_run_logger()

    result = subprocess.run(
        [
            python_exec_path,
            filepath,
        ],
        capture_output=True,
        text=True,
    )

    logger.info(f'Exit code: {result.returncode}')
    if result.stdout:
        logger.info(f'STDOUT:\n{result.stdout.strip()}')
    if result.stderr:
        logger.error(f'STDERR:\n{result.stderr.strip()}')

    if result.returncode != 0:
        raise RuntimeError(
            f'Code failed with exit code {result.returncode}'
        )
