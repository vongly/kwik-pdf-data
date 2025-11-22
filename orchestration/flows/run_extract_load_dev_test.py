from prefect import flow

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from tasks.extract_load import s3_to_postgres_dev_test


@flow(name='Test Load S3 Files to Postgres - Dev')
def run_extract_load_dev_test():
    s3_to_postgres_dev_test()

if __name__ == '__main__':
    run_extract_load_dev_test()