from prefect import flow

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from tasks.parser import parse_pdfs_s3


@flow(name='Parse PDF Files from S3 Bucket')
def run_parser():
    parse_pdfs_s3()

if __name__ == '__main__':
    run_parser()