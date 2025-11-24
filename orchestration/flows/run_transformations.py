from prefect import flow, get_run_logger

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from tasks.dbt import dbt_build_task


@flow(name='dbt Build')
def run_transformations():
    dbt_build_task('--target prod')

if __name__ == '__main__':
    run_transformations()