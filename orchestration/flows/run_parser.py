from prefect import flow

import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(parent_dir))

from tasks.parser import parse_pdfs_s3


@flow(name='Parse PDF Files from S3 Bucket')
def run_parser():
    parse_pdfs_s3()

if __name__ == '__main__':
    run_parser()