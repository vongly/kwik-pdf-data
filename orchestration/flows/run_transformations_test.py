from prefect import flow, get_run_logger

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from tasks.dbt import dbt_build_task


@flow(name='dbt Build Test')
def run_transformations_test():

    dbt_build_task('--target dev')

if __name__ == '__main__':
    run_transformations_test()