import subprocess
from prefect import flow, task, get_run_logger

import sys, os, io
from pathlib import Path

from dotenv import load_dotenv

parent_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(parent_dir))

from env import (
    DBT_DIR,
    DBT_DOTENV_PATH,
    DBT_EXEC_PATH,
)

load_dotenv(dotenv_path=DBT_DOTENV_PATH)


def run_dbt_commands(command):

    env = os.environ.copy()
    env['DBT_DIR'] = DBT_DIR

    logger = get_run_logger()
    result = subprocess.run(
        [DBT_EXEC_PATH] + command,
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
    )

    logger.info(f'Exit code: {result.returncode}')
    if result.stdout:
        logger.info(f'STDOUT:\n{result.stdout.strip()}')
    if result.stderr:
        logger.error(f'STDERR:\n{result.stderr.strip()}')

    if result.returncode != 0:
        raise RuntimeError(f'dbt run failed with exit code {result.returncode}')

    logger.info(f'Raw output:\n{result.stdout.strip()}')


@task(name='dbt Run')
def dbt_run_task(cmd_suffix=None):
    command = ['run']

    if cmd_suffix:
        cmd_suffix = cmd_suffix.split(' ')
        command = command + cmd_suffix

    run_dbt_commands(command)

@task(name='dbt Seed')
def dbt_seed_task(cmd_suffix=None):
    command = 'seed'

    if cmd_suffix:
        cmd_suffix = cmd_suffix.split(' ')
        command = command + cmd_suffix

    run_dbt_commands(command)

@task(name='dbt Build')
def dbt_build_task(cmd_suffix=None):
    command = ['build']

    if cmd_suffix:
        cmd_suffix = cmd_suffix.split(' ')
        command = command + cmd_suffix

    run_dbt_commands(command)

if __name__ == '__main__':
    dbt_run_task('--target prod')